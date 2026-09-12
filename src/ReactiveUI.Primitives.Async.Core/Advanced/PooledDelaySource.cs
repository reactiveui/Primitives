// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Poolable <see cref="IValueTaskSource"/> backing <c>DelayAsync</c> for non-System <see cref="TimeProvider"/> instances, so a delay costs no per-call allocation.</summary>
/// <remarks>
/// Whichever of the timer callback and the cancellation registration fires first claims completion
/// through an <see cref="Interlocked.CompareExchange(ref int, int, int)"/> on a state flag; the
/// loser is a no-op.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("PooledDelaySource: Completed = {_completed}, Timer = {_timer}")]
public sealed class PooledDelaySource : IValueTaskSource
{
    /// <summary>State value for <see cref="_completed"/> meaning "no terminal event yet".</summary>
    private const int StateOpen = 0;

    /// <summary>State value for <see cref="_completed"/> meaning "either timer or cancellation claimed completion".</summary>
    private const int StateClaimed = 1;

    /// <summary>One reusable delay source per thread; concurrent rentals allocate when this slot is empty.</summary>
    [ThreadStatic]
    private static PooledDelaySource? _threadCached;

    /// <summary>Backing source. Continuations run asynchronously so awaiters never re-enter the timer / cancel path.</summary>
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };

    /// <summary>Tracks completion: <see cref="StateOpen"/> until the timer or cancellation claims completion; then <see cref="StateClaimed"/>.</summary>
    private int _completed;

    /// <summary>The active timer, or <see langword="null"/> when the source isn't currently in use.</summary>
    private ITimer? _timer;

    /// <summary>The cancellation registration, or <c>default</c> when not registered.</summary>
    private CancellationTokenRegistration _cancellationRegistration;

    /// <summary>Initializes a new instance of the <see cref="PooledDelaySource"/> class.</summary>
    private PooledDelaySource()
    {
    }

    /// <summary>Rents a source from the per-thread cache, allocating a new one only on cache miss.</summary>
    /// <returns>A reset, ready-to-use <see cref="PooledDelaySource"/>.</returns>
    public static PooledDelaySource Rent()
    {
        var cached = _threadCached;
        if (cached is null)
        {
            return new();
        }

        _threadCached = null;
        return cached;
    }

    /// <summary>
    /// Begins the delay. The returned <see cref="ValueTask"/> completes when the timer fires or
    /// the cancellation token is signalled — whichever happens first. The caller MUST await it
    /// exactly once; the instance returns to the pool inside <see cref="GetResult(short)"/>.
    /// </summary>
    /// <param name="delay">The dueTime passed to <see cref="TimeProvider.CreateTimer"/>.</param>
    /// <param name="timeProvider">The non-System time provider supplying the timer.</param>
    /// <param name="cancellationToken">Cancellation token observed while waiting.</param>
    /// <returns>A <see cref="ValueTask"/> backed by this source.</returns>
    public ValueTask BeginAsync(TimeSpan delay, TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            _completed = StateClaimed;
            _core.SetException(new OperationCanceledException(cancellationToken));
            return new(this, _core.Version);
        }

        // CreateTimer may invoke the callback synchronously, flipping _completed to Claimed before
        // this call returns.
        _timer = timeProvider.CreateTimer(
            static state => ((PooledDelaySource)state!).OnTimerFired(),
            this,
            delay,
            Timeout.InfiniteTimeSpan);

        if (Volatile.Read(ref _completed) == StateClaimed)
        {
            // Sync-fire fast path: the source is complete, so no cancellation registration is needed.
            return new(this, _core.Version);
        }

        if (cancellationToken.CanBeCanceled)
        {
            _cancellationRegistration = cancellationToken.UnsafeRegister(
                static (state, ct) => ((PooledDelaySource)state!).OnCancelled(ct),
                this);
        }

        return new(this, _core.Version);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);

    /// <inheritdoc/>
    public void GetResult(short token)
    {
        try
        {
            _ = _core.GetResult(token);
        }
        finally
        {
            ReturnToPool();
        }
    }

    /// <summary>Completes the delay successfully when the timer's dueTime elapses, unless cancellation claimed it first.</summary>
    private void OnTimerFired()
    {
        if (!ConcurrencyRaceHelpers.TryClaim(ref _completed, StateOpen, StateClaimed))
        {
            return;
        }

        _core.SetResult(true);
    }

    /// <summary>Faults the delay with <see cref="OperationCanceledException"/> when the caller's token fires, unless the timer claimed it first.</summary>
    /// <param name="cancellationToken">The cancellation token that fired.</param>
    private void OnCancelled(CancellationToken cancellationToken)
    {
        if (!ConcurrencyRaceHelpers.TryClaim(ref _completed, StateOpen, StateClaimed))
        {
            return;
        }

        _core.SetException(new OperationCanceledException(cancellationToken));
    }

    /// <summary>Resets per-call state and caches the instance in the per-thread slot.</summary>
    private void ReturnToPool()
    {
        _cancellationRegistration.Dispose();
        _cancellationRegistration = default;
        _timer?.Dispose();
        _timer = null;
        _completed = StateOpen;
        _core.Reset();

        // One instance cached per thread; any extra instances are dropped for the GC.
        _threadCached ??= this;
    }
}
