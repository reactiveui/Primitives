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

    /// <summary>Scheduler-taking overloads fall back to the default sequencer when given none.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SchedulerOverloads_NullScheduler_UseDefaultSequencer()
    {
        const int MaxRetries = 1;
        const double BackoffFactor = 2;
        var window = TimeSpan.FromDays(1);
        var source = Observable.Return(1);

        await Assert.That(source.BufferUntilIdle(window, null)).IsNotNull();
        await Assert.That(source.ThrottleFirst(window, null)).IsNotNull();
        await Assert.That(source.DebounceImmediate(window, null)).IsNotNull();
        await Assert.That(source.DebounceUntil(window, static value => value > 0, null)).IsNotNull();
        await Assert.That(source.BufferUntilInactive(window, null)).IsNotNull();
        await Assert.That(source.RetryWithBackoff(MaxRetries, window, BackoffFactor, null, null)).IsNotNull();
    }

    /// <summary>The using operators run against a null resource, completing on success and erroring when the body throws.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Using_NullResource_RunsWithoutDisposing()
    {
        const int Result = 3;
        IDisposable resource = null!;
        List<int> results = [];
        List<Exception> actionErrors = [];
        List<Exception> functionErrors = [];
        var actionCompleted = false;

        _ = resource.Using(static _ => { }, Sequencer.Immediate).Subscribe(static _ => { }, static _ => { }, () => actionCompleted = true);
        _ = resource.Using(static _ => Result, Sequencer.Immediate).Subscribe(results.Add);
        _ = resource.Using(static _ => throw new InvalidOperationException("action"), Sequencer.Immediate).Subscribe(static _ => { }, actionErrors.Add);
        _ = resource.Using<IDisposable, int>(static _ => throw new InvalidOperationException("function"), Sequencer.Immediate).Subscribe(static _ => { }, functionErrors.Add);

        await Assert.That(actionCompleted).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([Result]);
        await Assert.That(actionErrors).HasSingleItem();
        await Assert.That(functionErrors).HasSingleItem();
    }
}
