// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides factory methods for creating asynchronous observable sequences.
/// </summary>
/// <remarks>The ObservableAsync class offers static methods to construct observables that emit values
/// asynchronously. These methods are useful for scenarios where data needs to be produced or streamed over time, such
/// as timers or event-driven sequences. All members of this class are thread-safe and can be used from multiple threads
/// concurrently.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Creates an asynchronous observable sequence that emits a long integer value at each specified time interval.
    /// </summary>
    /// <remarks>The sequence continues emitting values until the observer unsubscribes or the cancellation
    /// token is triggered. This method is useful for generating periodic events or timers in asynchronous
    /// workflows.</remarks>
    /// <param name="period">The time interval between emissions of values. Must be a positive duration.</param>
    /// <returns>An ObservableAsync{long} that emits an increasing long value at each interval, starting from 1, until the
    /// sequence is cancelled.</returns>
    public static IObservableAsync<long> Interval(TimeSpan period) =>
        Interval(period, (TimeProvider?)null);

    /// <summary>
    /// Creates an asynchronous observable sequence that emits a long integer value at each specified time interval.
    /// </summary>
    /// <remarks>The sequence continues emitting values until the observer unsubscribes or the cancellation
    /// token is triggered. This method is useful for generating periodic events or timers in asynchronous
    /// workflows.</remarks>
    /// <param name="period">The time interval between emissions of values. Must be a positive duration.</param>
    /// <param name="timeProvider">An optional time provider used to control the timing of emissions. If null or set to TimeProvider.System, the
    /// system clock is used.</param>
    /// <returns>An ObservableAsync{long} that emits an increasing long value at each interval, starting from 1, until the
    /// sequence is cancelled.</returns>
    public static IObservableAsync<long> Interval(TimeSpan period, TimeProvider? timeProvider) =>
        CreateAsBackgroundJob<long>(
            async (observer, cancellationToken) =>
            {
                long tick = 1;
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (timeProvider is null || timeProvider == TimeProvider.System)
                    {
                        await Task.Delay(period, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        await using var tp = timeProvider.CreateTimer(
                            x => ((TaskCompletionSource<bool>)x!).TrySetResult(true),
                            tcs,
                            period,
                            System.Threading.Timeout.InfiniteTimeSpan);

#if NET8_0_OR_GREATER
                        await using var ct =
                            cancellationToken.Register(
                                x => ((TaskCompletionSource<bool>)x!).TrySetCanceled(cancellationToken),
                                tcs);
#else
                        using var ct =
                            cancellationToken.Register(
                                x => ((TaskCompletionSource<bool>)x!).TrySetCanceled(cancellationToken),
                                tcs);
#endif

                        await tcs.Task.ConfigureAwait(false);
                    }

                    await observer.OnNextAsync(tick++, cancellationToken).ConfigureAwait(false);
                }
            },
            true);
}
