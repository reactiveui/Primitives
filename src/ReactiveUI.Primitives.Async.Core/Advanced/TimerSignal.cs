// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits one or more timer ticks.</summary>
[System.Diagnostics.DebuggerDisplay("DueTime = {DueTime}, Period = {Period}")]
public sealed class TimerSignal : IObservableAsync<long>
{
    /// <summary>Initializes a new instance of the <see cref="TimerSignal"/> class.</summary>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The optional delay between subsequent ticks.</param>
    /// <param name="timeProvider">The time provider used for scheduling.</param>
    public TimerSignal(TimeSpan dueTime, TimeSpan? period, TimeProvider timeProvider)
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
    ValueTask<IAsyncDisposable> IObservableAsync<long>.SubscribeAsync(
        IObserverAsync<long> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        TimerSubscription subscription = new(observer, DueTime, Period, TimeProvider);
        subscription.Start();
        return new(subscription);
    }
}
