// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Asynchronous mutual-exclusion primitive used to serialize critical sections inside the async
/// pipeline. The uncontended fast path is a pure
/// <see cref="Interlocked.CompareExchange(ref int, int, int)"/> ownership transfer — no
/// <see cref="SemaphoreSlim"/> touch at all, which is the dominant case in chained operator
/// pipelines. The contended slow path uses a signal-only <see cref="SemaphoreSlim"/> as a
/// wake-up channel and retries the CAS after each signal.
/// </summary>
/// <remarks>
/// <para>Same-thread reentry is granted via an owner-thread-id check, with a non-shared recursion
/// counter; nested exits just decrement it. Cross-thread reentry inside a single gate entry
/// would deadlock — no in-tree caller exercises that pattern.</para>
/// </remarks>
internal sealed class AsyncSerialGate : IDisposable
{
    /// <summary>
    /// Signal-only semaphore used to wake one waiter when the gate is released. Initial count is
    /// zero; <see cref="SemaphoreSlim.Release()"/> is called only when at least one waiter is
    /// recorded in <see cref="_waiters"/>, so the count tracks pending wake-ups.
    /// </summary>
    private readonly SemaphoreSlim _semaphore = new(0, int.MaxValue);

    /// <summary>
    /// Managed thread id of the thread that currently owns the gate. Zero when the gate is free.
    /// Used both as the ownership flag (CAS target on acquire / release) and as the reentry key.
    /// </summary>
    private int _ownerThreadId;

    /// <summary>
    /// Number of nested <see cref="EnterAsync"/> calls beyond the initial acquisition. Read / written
    /// only by the owning thread, so unguarded mutation is safe.
    /// </summary>
    private int _recursionDepth;

    /// <summary>
    /// Count of awaiters parked on the slow path. Read by <see cref="Exit"/> to decide whether
    /// to signal the semaphore; incremented / decremented around each <see cref="SemaphoreSlim.WaitAsync(CancellationToken)"/>.
    /// </summary>
    private int _waiters;

    /// <summary>Indicates whether this instance has been disposed.</summary>
    private bool _disposedValue;

    /// <summary>Gets the number of awaiters currently parked on the slow path. Exposed for
    /// deterministic contention tests so they can spin-wait until a contender has entered
    /// <see cref="WaitForEntryAsync"/> before tripping the release.</summary>
    internal int WaitersCount => Volatile.Read(ref _waiters);

    /// <summary>Asynchronously acquires the gate, returning a <see cref="Lease"/> that releases it on disposal.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="ValueTask{Lease}"/> that completes when the gate has been acquired.</returns>
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync(CancellationToken cancellationToken = default)
    {
        var currentThreadId = Environment.CurrentManagedThreadId;

        // Same-thread reentry: bump depth, no synchronization needed (we already own it).
        if (Volatile.Read(ref _ownerThreadId) == currentThreadId)
        {
            _recursionDepth++;
            return new(new Lease(this));
        }

        // Fast uncontended acquire: pure CAS, no semaphore touch.
        if (Interlocked.CompareExchange(ref _ownerThreadId, currentThreadId, 0) == 0)
        {
            return new(new Lease(this));
        }

        return WaitForEntryAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposedValue)
        {
            return;
        }

        _semaphore.Dispose();
        _disposedValue = true;
    }

    /// <summary>
    /// Exits the gate. Decrements the recursion depth on a nested exit, or clears the owner
    /// and signals one waiter (if any) on the outermost release.
    /// </summary>
    internal void Exit()
    {
        if (_recursionDepth > 0)
        {
            _recursionDepth--;
            return;
        }

        Volatile.Write(ref _ownerThreadId, 0);
        WakeNextWaiter();
    }

    /// <summary>
    /// Signals one parked waiter if any are present. An extra signal observed across the
    /// <see cref="_waiters"/> read / <see cref="SemaphoreSlim.Release()"/> race lands harmlessly in
    /// the semaphore count and is consumed by the next waiter that arrives.
    /// </summary>
    private void WakeNextWaiter()
    {
        if (Volatile.Read(ref _waiters) == 0)
        {
            return;
        }

        _semaphore.Release();
    }

    /// <summary>Slow path: park as a waiter and retry the acquire CAS after each semaphore signal.</summary>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    /// <returns>A <see cref="Lease"/> for the acquired gate.</returns>
    private async ValueTask<Lease> WaitForEntryAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _waiters);
        try
        {
            while (true)
            {
                // Retry the CAS before waiting; closes the race where the owner releases between
                // the caller's fast-path failure and our increment of _waiters.
                if (Interlocked.CompareExchange(ref _ownerThreadId, Environment.CurrentManagedThreadId, 0) == 0)
                {
                    return new(this);
                }

                await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _waiters);
        }
    }

    /// <summary>Releases a previously acquired <see cref="AsyncSerialGate"/> when disposed.</summary>
    public readonly record struct Lease : IDisposable
    {
        /// <summary>The parent <see cref="AsyncSerialGate"/> whose lock is released when this lease is disposed.</summary>
        private readonly AsyncSerialGate _parent;

        /// <summary>Initializes a new instance of the <see cref="Lease"/> struct.</summary>
        /// <param name="parent">The <see cref="AsyncSerialGate"/> that owns this lease.</param>
        public Lease(AsyncSerialGate parent) => _parent = parent;

        /// <inheritdoc/>
        public void Dispose() => _parent.Exit();
    }
}
