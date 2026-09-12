// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>The throttle window in virtual ticks.</summary>
    private const int ThrottleWindowTicks = 100;

    /// <summary>The interval used to verify distinct-value throttling.</summary>
    private static readonly TimeSpan DistinctThrottleWindow = TimeSpan.FromMilliseconds(200);

    /// <summary>The debounce interval.</summary>
    private static readonly TimeSpan DebounceWindow = TimeSpan.FromMilliseconds(500);

    /// <summary>Tests DebounceImmediate emits first immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DebounceImmediate_EmitsFirstImmediately()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.DebounceImmediate(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler).Subscribe(results.Add);
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
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        _ = subject.ThrottleFirst(TimeSpan.FromTicks(ThrottleWindowTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(1); // Should be emitted immediately
        subject.OnNext(SampleValue2); // Should be ignored (within throttle window)
        subject.OnNext(SampleValue3); // Should be ignored (within throttle window)
        scheduler.AdvanceBy(ThrottleWindowTicks + 1); // Move past the throttle window
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
        _ = subject.DropIfBusy(async x =>
        {
            await release.Task.ConfigureAwait(false);
            results.Add(x);
            processed.SetResult();
        }).Subscribe();
        subject.OnNext(1); // Should process
        subject.OnNext(SampleValue2); // Should drop
        subject.OnNext(SampleValue3); // Should drop
        release.SetResult(new()); // Complete the async action
        await processed.Task;
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
        _ = subject.ThrottleDistinct(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler).Subscribe(results.Add);
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
        _ = subject.DebounceUntil(TimeSpan.FromTicks(SchedulerWindowTicks), static x => x % SampleValue2 == 0, scheduler)
            .Subscribe(results.Add);
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
        _ = subject.ThrottleOnScheduler(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler).Subscribe(results.Add);
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
        _ = subject.ThrottleDistinct(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler).Subscribe(results.Add);
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
        _ = subject.DebounceImmediate(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler)
            .Subscribe(results.Add, ex => observedError = ex);
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
        _ = subject.DebounceImmediate(TimeSpan.FromTicks(SchedulerWindowTicks), scheduler)
            .Subscribe(results.Add, () => completed = true);
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
        _ = subject.DebounceUntil(TimeSpan.FromTicks(SchedulerWindowTicks), static x => x % SampleValue2 == 0, scheduler)
            .Subscribe(results.Add);
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
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = new ThrottleUntilTrueObservable<int>(
            subject,
            TimeSpan.FromTicks(ThrottleWindowTicks),
            static x => x > PredicateThreshold,
            scheduler).Subscribe(results.Add);

        // Predicate true: immediate.
        subject.OnNext(SampleValue10);

        // Predicate false: held until the clock passes the throttle window.
        subject.OnNext(1);
        await Assert.That(results).IsCollectionEqualTo([SampleValue10]);
        scheduler.AdvanceBy(ThrottleWindowTicks + 1);
        await Assert.That(results).Contains(SampleValue10);
        await Assert.That(results).Contains(1);
    }

    /// <summary>Verifies the throttle window emits the latest distinct value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctWindowEnds_ThenEmitsLatestDistinctValue()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.ThrottleDistinct(DistinctThrottleWindow, scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        scheduler.AdvanceBy(DistinctThrottleWindow.Ticks);
        await Assert.That(results).IsCollectionEqualTo([SampleValue2]);
    }

    /// <summary>Tests DebounceUntil with scheduler delays non-matching values and passes matching immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilWithScheduler_ThenUsesScheduler()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        _ = subject.DebounceUntil(TimeSpan.FromTicks(SchedulerWindowTicks), static x => x % SampleValue2 == 0, scheduler)
            .Subscribe(results.Add);
        subject.OnNext(SampleValue2); // condition true -> immediate
        subject.OnNext(SampleValue3); // condition false -> delayed
        scheduler.AdvanceBy(SchedulerWindowTicks + 1);
        await Assert.That(results).Contains(SampleValue2);
        await Assert.That(results).Contains(SampleValue3);
    }

    /// <summary>Verifies the first value is immediate and the trailing value waits for the debounce window.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateWithScheduler_ThenEmitsTrailingValue()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.DebounceImmediate(DebounceWindow, scheduler).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        await Assert.That(results).IsCollectionEqualTo([1]);
        scheduler.AdvanceBy(DebounceWindow.Ticks);
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
    }

    /// <summary>Tests DebounceUntil without scheduler emits immediately when condition true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilWithoutScheduler_ThenEmitsImmediatelyWhenConditionTrue()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.DebounceUntil(DebounceWindow, static x => x % SampleValue2 == 0).Subscribe(v =>
        {
            results.Add(v);
            _ = received.TrySetResult();
        });

        // Even values should emit immediately (condition true)
        subject.OnNext(SampleValue2);
        await received.Task;
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
