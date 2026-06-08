// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>ThrottleDistinctObservable</c> — the after-terminal guards on the
/// distinct-throttle sink, exercised via a source that pushes events past its own completion.</summary>
public class ThrottleDistinctObservableTests
{
    /// <summary>Tick window for advancing past the throttle in settle assertions.</summary>
    private const int SettleTicks = 100;

    /// <summary>Tick window for the throttle itself.</summary>
    private const int ThrottleTicks = 10;

    /// <summary>Verifies that an <c>OnNext</c> arriving after completion is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        var completed = false;

        using var sub = source.ThrottleDistinct(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(values.Add, () => completed = true);

        source.Observer.OnCompleted();
        scheduler.AdvanceBy(SettleTicks);
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(SettleTicks);

        await Assert.That(completed).IsTrue();
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies that an <c>OnError</c> arriving after a prior <c>OnCompleted</c> is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completed = false;

        using var sub = source.ThrottleDistinct(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(static _ => { }, ex => caught = ex, () => completed = true);

        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException("late"));

        await Assert.That(completed).IsTrue();
        await Assert.That(caught).IsNull();
    }
}
