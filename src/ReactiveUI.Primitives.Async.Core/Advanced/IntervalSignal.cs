// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits incrementing ticks at a fixed interval.</summary>
public sealed class IntervalSignal : IObservableAsync<long>
{
    /// <summary>Initializes a new instance of the <see cref="IntervalSignal"/> class.</summary>
    /// <param name="period">The delay between ticks.</param>
    /// <param name="timeProvider">The time provider used for custom scheduling.</param>
    public IntervalSignal(TimeSpan period, TimeProvider? timeProvider)
    {
        Period = period;
        TimeProvider = timeProvider;
    }

    /// <summary>Gets the delay between ticks.</summary>
    private TimeSpan Period { get; }

    /// <summary>Gets the time provider used for custom scheduling.</summary>
    private TimeProvider? TimeProvider { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<long>.SubscribeAsync(
        IObserverAsync<long> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        IntervalSubscription subscription = new(observer, Period, TimeProvider);
        subscription.Start();
        return new(subscription);
    }
}
