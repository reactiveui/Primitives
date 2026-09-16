// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits incrementing ticks at a fixed interval.</summary>
[System.Diagnostics.DebuggerDisplay("IntervalSubscription: Period = {Period}, TimeProvider = {TimeProvider}")]
public sealed class IntervalSubscription : IAsyncDisposable, ITaskSignalJob<long>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<long> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="IntervalSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving ticks.</param>
    /// <param name="period">The delay between ticks.</param>
    /// <param name="timeProvider">The time provider that schedules the ticks, or <see langword="null"/> for the
    /// system clock.</param>
    public IntervalSubscription(IObserverAsync<long> observer, TimeSpan period, TimeProvider? timeProvider)
    {
        _observer = observer;
        Period = period;
        TimeProvider = timeProvider;
    }

    /// <summary>Gets the delay between ticks.</summary>
    private TimeSpan Period { get; }

    /// <summary>Gets the time provider used for custom scheduling.</summary>
    private TimeProvider? TimeProvider { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<long>.ExecuteAsync(
        IObserverAsync<long> observer,
        CancellationToken cancellationToken)
    {
        long tick = 1;
        while (!cancellationToken.IsCancellationRequested)
        {
            if (TimeProvider is null || TimeProvider == TimeProvider.System)
            {
                await SignalAsyncExtensions.DelayAsync(Period, TimeProvider.System, cancellationToken).ConfigureAwait(false);
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
