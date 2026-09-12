// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests synchronous behavior of operators that use the default scheduler.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>A value satisfying the predicate bypasses the throttle window and completes synchronously.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrottleUntilTrue_DefaultSchedulerMatchingValue_ForwardsImmediately()
    {
        List<int> values = [];
        var completed = false;
        using var subscription = Observable.Return(SampleValue42)
            .ThrottleUntilTrue(TimeSpan.FromDays(1), static value => value == SampleValue42)
            .Subscribe(values.Add, () => completed = true);

        await Assert.That(values).IsCollectionEqualTo([SampleValue42]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>A source error is forwarded without waiting for the default scheduler's debounce window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DebounceImmediate_DefaultSchedulerSourceError_ForwardsImmediately()
    {
        InvalidOperationException failure = new("source");
        Exception? observed = null;
        using var subscription = Observable.Throw<int>(failure)
            .DebounceImmediate(TimeSpan.FromDays(1))
            .Subscribe(static _ => { }, error => observed = error);

        await Assert.That(observed).IsSameReferenceAs(failure);
    }

    /// <summary>An empty source completes without producing a buffer or waiting for an inactivity timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BufferUntilInactive_DefaultSchedulerEmptySource_CompletesImmediately()
    {
        List<IList<int>> buffers = [];
        var completed = false;
        using var subscription = Observable.Empty<int>()
            .BufferUntilInactive(TimeSpan.FromDays(1))
            .Subscribe(buffers.Add, () => completed = true);

        await Assert.That(buffers).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>The default overload shares its timer with the explicit default-scheduler overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncTimer_DefaultScheduler_ReusesExplicitDefaultTimer()
    {
        var period = TimeSpan.FromDays(1);
        var timer = period.SyncTimer();

        await Assert.That(timer).IsSameReferenceAs(period.SyncTimer(Sequencer.Default));
    }
}
