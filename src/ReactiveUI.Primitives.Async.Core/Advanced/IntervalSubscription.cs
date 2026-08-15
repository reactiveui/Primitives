// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits incrementing ticks at a fixed interval.</summary>
[System.Diagnostics.DebuggerDisplay("Period = {Period}, TimeProvider = {TimeProvider}")]
public sealed class IntervalSubscription : TaskSignalSubscription<long>
{
    /// <summary>Initializes a new instance of the <see cref="IntervalSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving ticks.</param>
    /// <param name="period">The delay between ticks.</param>
    /// <param name="timeProvider">The time provider used for custom scheduling.</param>
    public IntervalSubscription(IObserverAsync<long> observer, TimeSpan period, TimeProvider? timeProvider)
        : base(observer)
    {
        Period = period;
        TimeProvider = timeProvider;
    }

    /// <summary>Gets the delay between ticks.</summary>
    private TimeSpan Period { get; }

    /// <summary>Gets the time provider used for custom scheduling.</summary>
    private TimeProvider? TimeProvider { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<long> observer,
        CancellationToken cancellationToken)
    {
        long tick = 1;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (TimeProvider is null || TimeProvider == TimeProvider.System)
            {
                await Task.Delay(Period, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await DelayWithProviderAsync(cancellationToken).ConfigureAwait(false);
            }

            var current = tick;
            tick++;
            await observer.OnNextAsync(current, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Delays one interval through a custom time provider.</summary>
    /// <param name="cancellationToken">The cancellation token for the delay.</param>
    /// <returns>A task that completes when the delay fires.</returns>
    private async ValueTask DelayWithProviderAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var tp = TimeProvider!.CreateTimer(
            static x => ((TaskCompletionSource<bool>)x!).TrySetResult(true),
            tcs,
            Period,
            Timeout.InfiniteTimeSpan);

#if NET8_0_OR_GREATER
        await using var ct =
            cancellationToken.UnsafeRegister(
                static x =>
                {
                    var (tcs, ct) = ((TaskCompletionSource<bool>, CancellationToken))x!;
                    _ = tcs.TrySetCanceled(ct);
                },
                (tcs, cancellationToken));
#else
        using var ct =
            cancellationToken.Register(
                static x =>
                {
                    var (tcs, ct) = ((TaskCompletionSource<bool>, CancellationToken))x!;
                    _ = tcs.TrySetCanceled(ct);
                },
                (tcs, cancellationToken));
#endif

        await tcs.Task.ConfigureAwait(false);
    }
}
