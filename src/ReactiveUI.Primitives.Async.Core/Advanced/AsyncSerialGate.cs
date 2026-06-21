// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Asynchronous mutual-exclusion primitive that serializes critical sections in the async pipeline.
/// Uncontended acquire is a pure <see cref="Interlocked.CompareExchange(ref int, int, int)"/> (no
/// <see cref="SemaphoreSlim"/> touch); the contended path waits on a signal-only semaphore and retries
/// the CAS after each signal. Same-thread reentry is granted via the owner-thread-id and a recursion counter.
/// </summary>
public sealed class AsyncSerialGate : IDisposable
{
    /// <summary>Signal-only semaphore; released once per recorded waiter to wake one.</summary>
    private readonly SemaphoreSlim _semaphore = new(0, int.MaxValue);

    /// <summary>Owning thread id, 0 when free; doubles as the CAS ownership flag and reentry key.</summary>
    private int _ownerThreadId;

    /// <summary>Nested <c>EnterAsync</c> count beyond the first acquire; owner-thread-only, so unguarded.</summary>
    private int _recursionDepth;

    /// <summary>Awaiters parked on the slow path; read by <see cref="Exit"/> to decide whether to signal.</summary>
    private int _waiters;

    /// <summary>Whether this instance has been disposed.</summary>
    private bool _disposedValue;

    /// <summary>Gets the number of awaiters currently parked on the slow path. Exposed for
    /// deterministic contention tests so they can spin-wait until a contender has entered
    /// <see cref="WaitForEntryAsync"/> before tripping the release.</summary>
    internal int WaitersCount => Volatile.Read(ref _waiters);

    /// <summary>Asynchronously acquires the gate, returning a <see cref="Lease"/> that releases it on disposal.</summary>
    /// <returns>A <see cref="ValueTask{Lease}"/> that completes when the gate has been acquired.</returns>
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync() =>
        EnterAsync(CancellationToken.None);

    /// <summary>Asynchronously acquires the gate, returning a <see cref="Lease"/> that releases it on disposal.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="ValueTask{Lease}"/> that completes when the gate has been acquired.</returns>
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync(CancellationToken cancellationToken)
    {
        var currentThreadId = Environment.CurrentManagedThreadId;

        // Same-thread reentry: bump depth, no synchronization needed (we already own it).
        if (Volatile.Read(ref _ownerThreadId) == currentThreadId)
        {
            _recursionDepth++;
            return new(new Lease(this));
        }

        // Fast uncontended acquire: pure CAS, no semaphore touch.
        return Interlocked.CompareExchange(ref _ownerThreadId, currentThreadId, 0) == 0
            ? new(new Lease(this))
            : WaitForEntryAsync(cancellationToken);
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

        _ = _semaphore.Release();
    }

    /// <summary>Slow path: park as a waiter and retry the acquire CAS after each semaphore signal.</summary>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    /// <returns>A <see cref="Lease"/> for the acquired gate.</returns>
    private async ValueTask<Lease> WaitForEntryAsync(CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _waiters);
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
            _ = Interlocked.Decrement(ref _waiters);
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
