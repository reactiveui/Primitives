// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the Timer factory method for creating asynchronous observable sequences that produce
/// a single value after a specified delay.
/// </summary>
/// <remarks>Timer is useful for triggering one-shot deferred actions in observable pipelines.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that produces a single value (0) after the specified delay, then completes.</summary>
    /// <param name="dueTime">The time span after which to produce the value. Must be non-negative.</param>
    /// <returns>An observable sequence that produces a single value after the specified delay and then completes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
    public static IObservableAsync<long> After(TimeSpan dueTime)
        => Timer(dueTime, (TimeProvider?)null);

    /// <summary>
    /// Creates an observable sequence that produces a single value (0) after the specified delay,
    /// then continues to produce sequential values at each specified period.
    /// </summary>
    /// <param name="dueTime">The initial delay before the first value is produced. Must be non-negative.</param>
    /// <param name="period">The interval between subsequent values after the initial delay. Must be positive.</param>
    /// <returns>An observable sequence that produces values starting after the initial delay and continuing
    /// at the specified period.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative
    /// or <paramref name="period"/> is non-positive.</exception>
    public static IObservableAsync<long> After(TimeSpan dueTime, TimeSpan period)
        => Timer(dueTime, period, (TimeProvider?)null);

    /// <summary>Emits monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Every(TimeSpan period) => Timer(period, period);

    /// <summary>Alias for <see cref="Every(TimeSpan)"/>.</summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Pulse(TimeSpan period) => Every(period);

    /// <summary>Creates an observable sequence that produces a single value (0) after the specified delay, then completes.</summary>
    /// <param name="dueTime">The time span after which to produce the value. Must be non-negative.</param>
    /// <returns>An observable sequence that produces a single value after the specified delay and then completes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
    public static IObservableAsync<long> Timer(TimeSpan dueTime)
        => After(dueTime);

    /// <summary>Creates an observable sequence that produces a single value (0) after the specified delay, then completes.</summary>
    /// <param name="dueTime">The time span after which to produce the value. Must be non-negative.</param>
    /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
    /// is used.</param>
    /// <returns>An observable sequence that produces a single value after the specified delay and then completes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
    public static IObservableAsync<long> Timer(TimeSpan dueTime, TimeProvider? timeProvider)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(dueTime, TimeSpan.Zero);
#else
        if (dueTime < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(dueTime));
        }
#endif

        var tp = timeProvider ?? TimeProvider.System;

        return CreateAsBackgroundJob<long>(
            async (observer, cancellationToken) =>
            {
                await SignalAsyncExtensions.DelayAsync(dueTime, tp, cancellationToken).ConfigureAwait(false);
                await observer.OnNextAsync(0L, cancellationToken).ConfigureAwait(false);
                await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            },
            true);
    }

    /// <summary>
    /// Creates an observable sequence that produces a single value (0) after the specified delay,
    /// then continues to produce sequential values at each specified period.
    /// </summary>
    /// <param name="dueTime">The initial delay before the first value is produced. Must be non-negative.</param>
    /// <param name="period">The interval between subsequent values after the initial delay. Must be positive.</param>
    /// <returns>An observable sequence that produces values starting after the initial delay and continuing
    /// at the specified period.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative
    /// or <paramref name="period"/> is non-positive.</exception>
    public static IObservableAsync<long> Timer(TimeSpan dueTime, TimeSpan period)
        => After(dueTime, period);

    /// <summary>
    /// Creates an observable sequence that produces a single value (0) after the specified delay,
    /// then continues to produce sequential values at each specified period.
    /// </summary>
    /// <param name="dueTime">The initial delay before the first value is produced. Must be non-negative.</param>
    /// <param name="period">The interval between subsequent values after the initial delay. Must be positive.</param>
    /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
    /// is used.</param>
    /// <returns>An observable sequence that produces values starting after the initial delay and continuing
    /// at the specified period.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative
    /// or <paramref name="period"/> is non-positive.</exception>
    public static IObservableAsync<long> Timer(TimeSpan dueTime, TimeSpan period, TimeProvider? timeProvider)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfLessThan(dueTime, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);
#else
        if (dueTime < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(dueTime));
        }

        if (period <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(period));
        }
#endif

        var tp = timeProvider ?? TimeProvider.System;

        return CreateAsBackgroundJob<long>(
            async (observer, cancellationToken) =>
            {
                await SignalAsyncExtensions.DelayAsync(dueTime, tp, cancellationToken).ConfigureAwait(false);

                long tick = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    await observer.OnNextAsync(tick++, cancellationToken).ConfigureAwait(false);
                    await SignalAsyncExtensions.DelayAsync(period, tp, cancellationToken).ConfigureAwait(false);
                }
            },
            true);
    }
}
