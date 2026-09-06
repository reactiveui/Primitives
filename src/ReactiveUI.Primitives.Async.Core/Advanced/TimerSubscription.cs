// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits one or more timer ticks.</summary>
[System.Diagnostics.DebuggerDisplay("TimerSubscription: DueTime = {DueTime}, Period = {Period}")]
public sealed class TimerSubscription : TaskSignalSubscription<long>
{
    /// <summary>Initializes a new instance of the <see cref="TimerSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving ticks.</param>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The optional delay between subsequent ticks.</param>
    /// <param name="timeProvider">The time provider used for scheduling.</param>
    public TimerSubscription(
        IObserverAsync<long> observer,
        TimeSpan dueTime,
        TimeSpan? period,
        TimeProvider timeProvider)
        : base(observer)
    {
        DueTime = dueTime;
        Period = period;
        TimeProvider = timeProvider;
    }

    /// <summary>Gets the delay before the first tick.</summary>
    private TimeSpan DueTime { get; }

    /// <summary>Gets the optional delay between subsequent ticks.</summary>
    private TimeSpan? Period { get; }

    /// <summary>Gets the time provider used for scheduling.</summary>
    private TimeProvider TimeProvider { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<long> observer,
        CancellationToken cancellationToken)
    {
        await SignalAsyncExtensions.DelayAsync(DueTime, TimeProvider, cancellationToken).ConfigureAwait(false);

        if (Period is not { } period)
        {
            await observer.OnNextAsync(0L, cancellationToken).ConfigureAwait(false);
            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return;
        }

        long tick = 0;
        while (!cancellationToken.IsCancellationRequested)
        {
            var current = tick;
            tick++;
            await observer.OnNextAsync(current, cancellationToken).ConfigureAwait(false);
            await SignalAsyncExtensions.DelayAsync(period, TimeProvider, cancellationToken).ConfigureAwait(false);
        }
    }
}
