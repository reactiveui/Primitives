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

    /// <summary>The interval between ticks.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies that disposing the interval subscription from inside a tick handler ends the tick loop:
    /// the ticks seen so far start at one and are consecutive, and nothing arrives after the dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedFromWithinATick_ThenTheTickLoopStops()
    {
        const long SecondTick = 2L;
        ManualTimeProvider time = new();
        List<long> ticks = [];
        IAsyncDisposable? handle = null;
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        handle = await SignalAsync.Interval(TickPeriod, time).SubscribeAsync(async (tick, _) =>
        {
            ticks.Add(tick);
            if (tick == DisposeOnTick)
            {
                await handle!.DisposeAsync();
                disposed.SetResult();
            }
        });
        await time.RunAsync(disposed.Task);
        await Assert.That(ticks).IsCollectionEqualTo([1L, SecondTick]);
    }
}
