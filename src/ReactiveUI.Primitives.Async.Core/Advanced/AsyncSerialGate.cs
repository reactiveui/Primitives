// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Asynchronous mutual-exclusion primitive that serializes critical sections in the async pipeline.
/// Each acquire hands back a <see cref="Lease"/> that releases the gate when disposed. Ownership is keyed
/// on the managed thread id, so a nested acquire on the holding thread is granted immediately and reentry
/// is recognised only while the caller stays on the thread that took the gate.
/// </summary>
[System.Diagnostics.DebuggerDisplay("AsyncSerialGate: OwnerThreadId = {_ownerThreadId}, Waiters = {_waiters}, RecursionDepth = {_recursionDepth}")]
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

    /// <summary>Disposal latch; non-zero once this instance has been disposed.</summary>
    private int _disposedValue;

    /// <summary>Gets the number of awaiters parked on the slow path.</summary>
    internal int WaitersCount => Volatile.Read(ref _waiters);

    /// <summary>Asynchronously acquires the gate, returning a <see cref="Lease"/> that releases it on disposal.</summary>
    /// <returns>A <see cref="ValueTask{Lease}"/> that completes when the gate has been acquired.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync() =>
        EnterAsync(CancellationToken.None);

    /// <summary>Asynchronously acquires the gate, returning a <see cref="Lease"/> that releases it on disposal.</summary>
    /// <param name="cancellationToken">Observed only while waiting for a contended gate; an uncontended acquire
    /// never checks it.</param>
    /// <returns>A <see cref="ValueTask{Lease}"/> that completes when the gate has been acquired.</returns>
    /// <exception cref="OperationCanceledException">Thrown when the token is cancelled before the gate is
    /// acquired.</exception>
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync(CancellationToken cancellationToken)
    {
        var currentThreadId = Environment.CurrentManagedThreadId;

        // Same-thread reentry: the calling thread owns the gate, so bumping depth needs no synchronization.
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
        if (Interlocked.Exchange(ref _disposedValue, 1) != 0)
        {
            return;
        }

        _semaphore.Dispose();
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
    /// Signals one parked waiter if any are present. A signal released after the last waiter has left
    /// lands in the semaphore count and is consumed by the next waiter to arrive.
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

    /// <summary>Holds one acquisition of an <see cref="AsyncSerialGate"/> and releases it on disposal.</summary>
    [System.Diagnostics.DebuggerDisplay("Lease: Parent = {_parent}")]
    public readonly record struct Lease : IDisposable
    {
        /// <summary>The parent <see cref="AsyncSerialGate"/> whose lock is released when this lease is disposed.</summary>
        private readonly AsyncSerialGate _parent;

        /// <summary>Initializes a new instance of the <see cref="Lease"/> struct.</summary>
        /// <param name="parent">The <see cref="AsyncSerialGate"/> that owns this lease.</param>
        public Lease(AsyncSerialGate parent) => _parent = parent;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _parent.Exit();
    }
}
