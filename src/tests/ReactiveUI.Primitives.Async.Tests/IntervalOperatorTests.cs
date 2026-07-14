// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the <c>Interval</c> operator's shutdown path: disposing the subscription from inside a tick
/// cancels the tick loop between one notification and the next, so the loop exits on its own cancellation
/// check rather than by tearing a pending delay down with an exception.
/// </summary>
public class IntervalOperatorTests
{
    /// <summary>The tick the handler disposes on.</summary>
    private const long DisposeOnTick = 2;

    /// <summary>Seconds a test waits for the reentrant dispose to finish.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>The interval between ticks.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(20);

    /// <summary>Maximum time a test waits for the reentrant dispose to finish.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(WaitTimeoutSeconds);

    /// <summary>How long a test waits afterwards to prove no further tick is emitted.</summary>
    private static readonly TimeSpan QuietWindow = TimeSpan.FromMilliseconds(200);

    /// <summary>Verifies that disposing the interval subscription from inside a tick handler ends the tick loop:
    /// the ticks seen so far start at one and are consecutive, and nothing arrives after the dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedFromWithinATick_ThenTheTickLoopStops()
    {
        TaskCompletionSource<IAsyncDisposable> handleReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<long> ticks = [];

        var subscription = await SignalAsync.Interval(TickPeriod).SubscribeAsync(async (tick, _) =>
        {
            ticks.Add(tick);
            if (tick < DisposeOnTick)
            {
                return;
            }

            var handle = await handleReady.Task.ConfigureAwait(false);
            await handle.DisposeAsync().ConfigureAwait(false);
            IgnoredResult.Of(disposed.TrySetResult());
        });

        handleReady.SetResult(subscription);
        await disposed.Task.WaitAsync(WaitTimeout);

        var ticksAtDispose = ticks.Count;
        var keptTicking = await AsyncTestHelpers.WaitForConditionAsync(
            () => ticks.Count > ticksAtDispose,
            QuietWindow);

        await Assert.That(keptTicking).IsFalse();
        await Assert.That(ticks).Count().IsEqualTo(ticksAtDispose);
        await Assert.That(ticks[0]).IsEqualTo(1L);
        await Assert.That(ticks[^1]).IsEqualTo(DisposeOnTick);
    }
}
