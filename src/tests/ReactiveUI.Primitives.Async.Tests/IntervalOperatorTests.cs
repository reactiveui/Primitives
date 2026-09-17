// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the <c>Interval</c> operator's tick loop and its shutdown path.</summary>
public class IntervalOperatorTests
{
    /// <summary>The tick the handler disposes on.</summary>
    private const long DisposeOnTick = 1;

    /// <summary>The interval between ticks.</summary>
    private static readonly TimeSpan TickPeriod = TimeSpan.FromMilliseconds(20);

    /// <summary>Verifies that disposing from inside a tick handler delivers no tick after the one that disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedFromWithinATick_ThenTheTickLoopStops()
    {
        const long FirstTick = 0L;
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
        await Assert.That(ticks).IsCollectionEqualTo([FirstTick, DisposeOnTick]);
    }
}
