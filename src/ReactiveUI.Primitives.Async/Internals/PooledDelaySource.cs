// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Sources;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Poolable <see cref="IValueTaskSource"/> used by <c>DelayAsync</c> for the non-System
/// <see cref="TimeProvider"/> code path. Replaces the per-call
/// <see cref="TaskCompletionSource{TResult}"/> + <see cref="Task{TResult}"/> +
/// <see cref="CancellationTokenRegistration"/> allocation chain with a rented-then-returned
/// instance. The wrapped <see cref="ManualResetValueTaskSourceCore{TResult}"/> is the standard
/// poolable async-primitive shape from <c>System.Threading.Tasks.Sources</c>.
/// </summary>
/// <remarks>
/// <para>Completion is claimed by whichever of the timer callback or the cancellation registration
/// fires first, using an <see cref="Interlocked.CompareExchange(ref int, int, int)"/> on a state
/// flag. The loser is a no-op. After the caller awaits the returned <see cref="ValueTask"/>, the
/// instance is reset and pushed back to the pool inside <see cref="GetResult(short)"/>.</para>
/// </remarks>
internal sealed class PooledDelaySource : IValueTaskSource
{
    /// <summary>State value for <see cref="_completed"/> meaning "no terminal event yet".</summary>
    private const int StateOpen = 0;

    /// <summary>State value for <see cref="_completed"/> meaning "either timer or cancellation claimed completion".</summary>
    private const int StateClaimed = 1;

    /// <summary>
    /// Per-thread cached instance. A thread-static slot has zero per-rent / per-return allocation
    /// (a <see cref="ConcurrentStack{T}.Push(T)"/> allocates a <c>Node</c> wrapper, which
    /// dominated allocation in the first cut of this pool). Single-slot caching is sufficient
    /// because the operators that consume <c>DelayAsync</c> serialise their per-instance work
    /// behind a gate, so a single thread holds at most one in-flight delay per operator at a time.
    /// Concurrent delays from different threads each get their own cached slot.
    /// </summary>
    [ThreadStatic]
    [SuppressMessage(
        "Critical Code Smell",
        "S2696:Instance members should not write to \"static\" fields",
        Justification = "Thread-static slot is intentional — each thread caches its own instance.")]
    private static PooledDelaySource? _threadCached;

    /// <summary>Backing source. Continuations run asynchronously so awaiters never re-enter the timer / cancel path.</summary>
    private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };

    /// <summary><see cref="StateOpen"/> until the timer or cancellation claims completion; then <see cref="StateClaimed"/>.</summary>
    private int _completed;

    /// <summary>The active timer, or <see langword="null"/> when the source isn't currently in use.</summary>
    private ITimer? _timer;

    /// <summary>The cancellation registration, or <c>default</c> when not registered.</summary>
    private CancellationTokenRegistration _ctReg;

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
            return new PooledDelaySource();
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
            return new ValueTask(this, _core.Version);
        }

        // CreateTimer may invoke the callback synchronously (the immediate-fire pattern used by
        // some test / benchmark providers); in that case _completed flips to Claimed before this
        // call returns.
        _timer = timeProvider.CreateTimer(
            static state => ((PooledDelaySource)state!).OnTimerFired(),
            this,
            delay,
            Timeout.InfiniteTimeSpan);

        if (Volatile.Read(ref _completed) == StateClaimed)
        {
            // Sync-fire fast path: no cancellation registration needed; the source is already done.
            return new ValueTask(this, _core.Version);
        }

        if (cancellationToken.CanBeCanceled)
        {
            _ctReg = cancellationToken.UnsafeRegister(
                static (state, ct) => ((PooledDelaySource)state!).OnCancelled(ct),
                this);
        }

        return new ValueTask(this, _core.Version);
    }

    /// <inheritdoc/>
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    /// <inheritdoc/>
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);

    /// <inheritdoc/>
    public void GetResult(short token)
    {
        try
        {
            _core.GetResult(token);
        }
        finally
        {
            ReturnToPool();
        }
    }

    /// <summary>
    /// Callback invoked when the timer's dueTime elapses. The race-loser branch (where
    /// <c>OnCancelled</c> claimed the state first) cannot be deterministically triggered in
    /// unit tests because the timer and cancellation must fire concurrently — the underlying
    /// claim logic is covered by direct tests against <see cref="ConcurrencyRaceHelpers.TryClaim"/>.
    /// </summary>
    [ExcludeFromCodeCoverage]
    private void OnTimerFired()
    {
        if (!ConcurrencyRaceHelpers.TryClaim(ref _completed, StateOpen, StateClaimed))
        {
            return;
        }

        _core.SetResult(true);
    }

    /// <summary>
    /// Callback invoked when the caller's cancellation token transitions to cancelled. Same
    /// race-only loser branch as <see cref="OnTimerFired"/>.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token that fired.</param>
    [ExcludeFromCodeCoverage]
    private void OnCancelled(CancellationToken cancellationToken)
    {
        if (!ConcurrencyRaceHelpers.TryClaim(ref _completed, StateOpen, StateClaimed))
        {
            return;
        }

        _core.SetException(new OperationCanceledException(cancellationToken));
    }

    /// <summary>Resets per-call state and caches the instance in the per-thread slot.</summary>
    [SuppressMessage(
        "Critical Code Smell",
        "S2696:Instance members should not write to \"static\" fields",
        Justification = "Thread-static slot is intentional — each thread caches its own instance.")]
    private void ReturnToPool()
    {
        _ctReg.Dispose();
        _ctReg = default;
        _timer?.Dispose();
        _timer = null;
        _completed = StateOpen;
        _core.Reset();

        // Only one instance cached per thread; drop the rest for the GC. Capacity-of-one is the
        // sweet spot for these operators — they hold at most one in-flight delay per gate.
        _threadCached ??= this;
    }
}
