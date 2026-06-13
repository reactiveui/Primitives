// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Async.Tests;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests DebounceImmediate emits first immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DebounceImmediate_EmitsFirstImmediately()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.DebounceImmediate(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        await Assert.That(results).IsNotEmpty();
        await Assert.That(results[0]).IsEqualTo(1);
    }

    /// <summary>Tests ThrottleFirst emits first immediately, then ignores subsequent values within the throttle window.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ThrottleFirst_EmitsFirstImmediately_IgnoresSubsequentWithinWindow()
    {
        Subject<int> subject = new();
        List<int> results = [];

        // Throttle window of 100 ms
        subject.ThrottleFirst(TimeSpan.FromMilliseconds(100)).Subscribe(results.Add);
        subject.OnNext(1); // Should be emitted immediately
        subject.OnNext(SampleValue2); // Should be ignored (within throttle window)
        subject.OnNext(SampleValue3); // Should be ignored (within throttle window)
        await Task.Delay(ThrottleWaitMilliseconds); // Wait for throttle window to pass
        subject.OnNext(SampleValue4); // Should be emitted

        // Verify results
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue4]);
    }

    /// <summary>Tests DropIfBusy drops values when busy.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous RxVoid test.</returns>
    [Test]
    public async Task DropIfBusy_DropsWhenBusy()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource<object> release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource processed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        subject.DropIfBusy(async x =>
        {
            await release.Task;
            results.Add(x);
            processed.TrySetResult();
        }).Subscribe();
        subject.OnNext(1); // Should process
        subject.OnNext(SampleValue2); // Should drop
        subject.OnNext(SampleValue3); // Should drop
        release.SetResult(new()); // Complete the async action
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests ThrottleDistinct throttles distinct values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ThrottleDistinct_ThrottlesDistinct()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.ThrottleDistinct(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(1); // Duplicate, ignored
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        subject.OnNext(SampleValue2); // Duplicate after throttle
        await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
    }

    /// <summary>Tests DebounceUntil emits immediately when condition true, delays when false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DebounceUntil_EmitsImmediatelyWhenConditionTrue_DelaysWhenFalse()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.DebounceUntil(TimeSpan.FromTicks(100), x => x % SampleValue2 == 0, scheduler).Subscribe(results.Add);
        subject.OnNext(1); // Odd, should be delayed
        scheduler.AdvanceBy(SchedulerHalfWindowTicks); // Advance less than debounce period
        subject.OnNext(SampleValue2); // Even, should emit immediately, cancelling delayed 1
        scheduler.AdvanceBy(SchedulerWindowTicks); // Advance past debounce period
        await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
    }

    /// <summary>Tests ThrottleOnScheduler throttles on the specified scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnScheduler_ThenThrottlesOnScheduler()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.ThrottleOnScheduler(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
    }

    /// <summary>Tests ThrottleDistinct with scheduler throttles and deduplicates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctWithScheduler_ThenThrottlesAndDeduplicates()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.ThrottleDistinct(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(1); // Duplicate, suppressed by DistinctUntilChanged
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
    }

    /// <summary>Tests DebounceImmediate flushes pending value when source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateSourceErrors_ThenFlushesAndForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        Exception? observedError = null;
        subject.DebounceImmediate(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add, ex => observedError = ex);
        subject.OnNext(1); // Emitted immediately (first)
        subject.OnNext(SampleValue2); // Buffered as pending
        subject.OnError(new InvalidOperationException("test"));
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(observedError).IsNotNull();
        }
    }

    /// <summary>Tests DebounceImmediate flushes pending value when source completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateSourceCompletes_ThenFlushesAndCompletes()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        subject.DebounceImmediate(TimeSpan.FromTicks(100), scheduler).Subscribe(results.Add, () => completed = true);
        subject.OnNext(1); // Emitted immediately (first)
        subject.OnNext(SampleValue2); // Buffered as pending
        subject.OnCompleted();
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests DebounceUntil with scheduler delays non-matching values using the scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilWithScheduler_ThenUsesSchedulerForDelay()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.DebounceUntil(TimeSpan.FromTicks(100), x => x % SampleValue2 == 0, scheduler).Subscribe(results.Add);
        subject.OnNext(SampleValue2); // Even, emits immediately
        subject.OnNext(1); // Odd, delayed
        scheduler.AdvanceBy(SchedulerAdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([SampleValue2, 1]);
    }

    /// <summary>Tests ThrottleUntilTrue with predicate false path applies throttle.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTruePredicateFalse_ThenAppliesThrottle()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource<int> throttledArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.ThrottleUntilTrue(TimeSpan.FromMilliseconds(100), x => x > 5).Subscribe(value =>
        {
            results.Add(value);
            _ = value == 1 && throttledArrived.TrySetResult(value);
        });

        // Predicate true: immediate.
        subject.OnNext(SampleValue10);

        // Predicate false: throttled — wait on the event instead of racing a fixed delay.
        subject.OnNext(1);
        await throttledArrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).Contains(SampleValue10);
        await Assert.That(results).Contains(1);
    }

    /// <summary>Tests ThrottleDistinct without scheduler parameter.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctWithoutScheduler_ThenThrottlesAndDeduplicates()
    {
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.ThrottleDistinct(TimeSpan.FromMilliseconds(200)).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        await AsyncTestHelpers.WaitForConditionAsync(() => results.Contains(SampleValue2), TimeSpan.FromSeconds(30));
        await Assert.That(results).Contains(SampleValue2);
    }

    /// <summary>Tests DebounceUntil with scheduler delays non-matching values and passes matching immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilWithScheduler_ThenUsesScheduler()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        subject.DebounceUntil(TimeSpan.FromTicks(100), x => x % SampleValue2 == 0, scheduler).Subscribe(results.Add);
        subject.OnNext(SampleValue2); // condition true -> immediate
        subject.OnNext(SampleValue3); // condition false -> delayed
        scheduler.AdvanceBy(SchedulerWindowTicks + 1);
        await Assert.That(results).Contains(SampleValue2);
        await Assert.That(results).Contains(SampleValue3);
    }

    /// <summary>Tests DebounceImmediate with null scheduler uses Default scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateNullScheduler_ThenUsesDefault()
    {
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.DebounceImmediate(TimeSpan.FromMilliseconds(200)).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 2, TimeSpan.FromSeconds(30));
        await Assert.That(results).Contains(1);
        await Assert.That(results).Contains(SampleValue2);
    }

    /// <summary>Tests DebounceUntil without scheduler emits immediately when condition true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilWithoutScheduler_ThenEmitsImmediatelyWhenConditionTrue()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.DebounceUntil(TimeSpan.FromMilliseconds(500), x => x % 2 == 0).Subscribe(v =>
        {
            results.Add(v);
            received.TrySetResult();
        });

        // Even values should emit immediately (condition true)
        subject.OnNext(SampleValue2);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).Contains(SampleValue2);
    }

    /// <summary>Verifies that <c>DebounceUntil</c> forwards source completion downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilSourceCompletes_ThenForwardsCompletion()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        var completed = false;
        using var sub = subject.DebounceUntil(TimeSpan.FromTicks(SchedulerWindowTicks), static _ => true, scheduler)
            .Subscribe(
                static _ => { },
                () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>DebounceUntil</c> forwards source errors downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilSourceErrors_ThenForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new("source-failed");
        using var sub = subject.DebounceUntil(TimeSpan.FromTicks(SchedulerWindowTicks), static _ => true, scheduler)
            .Subscribe(
                static _ => { },
                ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
