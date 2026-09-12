// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Serializes asynchronous critical sections, permitting reentry only from the owning managed thread.</summary>
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    public ValueTask<Lease> EnterAsync(CancellationToken cancellationToken) =>
        EnterForThreadAsync(Environment.CurrentManagedThreadId, cancellationToken);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposedValue, 1) != 0)
        {
            return;
        }

        _semaphore.Dispose();
    }

    /// <summary>Acquires ownership for the supplied caller thread, allowing same-thread reentry.</summary>
    /// <param name="currentThreadId">The calling thread identifier.</param>
    /// <param name="cancellationToken">Cancellation observed while waiting.</param>
    /// <returns>The acquired gate lease.</returns>
    internal ValueTask<Lease> EnterForThreadAsync(int currentThreadId, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _ownerThreadId) == currentThreadId)
        {
            _recursionDepth++;
            return new(new Lease(this));
        }

        return Interlocked.CompareExchange(ref _ownerThreadId, currentThreadId, 0) == 0
            ? new(new Lease(this))
            : WaitForEntryAsync(cancellationToken);
    }

    /// <summary>Releases one acquisition, waking a waiter after the outermost release.</summary>
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

    /// <summary>Slow path: park as a waiter and retry the acquire CAS after each semaphore signal.</summary>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    /// <returns>A <see cref="Lease"/> for the acquired gate.</returns>
    internal async ValueTask<Lease> WaitForEntryAsync(CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _waiters);
        try
        {
            while (true)
            {
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

    /// <summary>Signals one parked waiter if any are present; a late signal is consumed by the next waiter.</summary>
    private void WakeNextWaiter()
    {
        if (Volatile.Read(ref _waiters) == 0)
        {
            return;
        }

        _ = _semaphore.Release();
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
