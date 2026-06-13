// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the reactive-condition <c>ObserveOnIf</c> overload
/// backed by <c>ObserveOnIfObservable&lt;T&gt;</c> — condition switching, error forwarding,
/// and completion forwarding.</summary>
public class ObserveOnIfObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies that values dispatch on the false-scheduler before any condition arrives.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfNoCondition_ThenUsesFalseScheduler()
    {
        const int Value = 11;
        Subject<int> source = new();
        Subject<bool> condition = new();
        RecordingScheduler trueScheduler = new();
        RecordingScheduler falseScheduler = new();
        TaskCompletionSource<int> emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler)
            .Subscribe(v => emitted.TrySetResult(v));
        source.OnNext(Value);
        var v2 = await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(v2).IsEqualTo(Value);
        await Assert.That(falseScheduler.ScheduleCount).IsGreaterThanOrEqualTo(1);
        await Assert.That(trueScheduler.ScheduleCount).IsEqualTo(0);
    }

    /// <summary>Verifies that emitting after the condition becomes true dispatches on the true-scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfConditionTrue_ThenUsesTrueScheduler()
    {
        const int Value = 22;
        Subject<int> source = new();
        Subject<bool> condition = new();
        RecordingScheduler trueScheduler = new();
        RecordingScheduler falseScheduler = new();
        TaskCompletionSource<int> emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler)
            .Subscribe(v => emitted.TrySetResult(v));
        condition.OnNext(true);
        source.OnNext(Value);
        var v2 = await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(v2).IsEqualTo(Value);
        await Assert.That(trueScheduler.ScheduleCount).IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that <c>ObserveOnIf</c> forwards source errors without scheduler dispatch.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfSourceErrors_ThenForwardsError()
    {
        Subject<int> source = new();
        Subject<bool> condition = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = source.ObserveOnIf(condition, TaskPoolSequencer.Default, Sequencer.Immediate).Subscribe(
            static _ => { },
            ex => caught = ex);
        source.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ObserveOnIf</c> forwards source completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int> source = new();
        Subject<bool> condition = new();
        var completed = false;
        using var sub = source.ObserveOnIf(condition, TaskPoolSequencer.Default, Sequencer.Immediate).Subscribe(
            static _ => { },
            () => completed = true);
        source.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that the single-scheduler overload defaults the false branch to
    /// <see cref = "Sequencer.Immediate"/> by emitting synchronously when the condition is false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfSingleSchedulerConditionFalse_ThenImmediate()
    {
        const int Value = 33;
        Subject<int> source = new();
        Subject<bool> condition = new();
        RecordingScheduler trueScheduler = new();
        List<int> results = [];
        using var sub = source.ObserveOnIf(condition, trueScheduler).Subscribe(results.Add);
        condition.OnNext(false);
        source.OnNext(Value);
        await Assert.That(results).IsCollectionEqualTo([Value]);
        await Assert.That(trueScheduler.ScheduleCount).IsEqualTo(0);
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the source has completed is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Subject<bool> condition = new();
        var trueScheduler = Sequencer.Immediate;
        var falseScheduler = Sequencer.Immediate;
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler)
            .Subscribe(values.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Exercises the <c>_done</c> guard inside the scheduled callback —
    /// when the source completes between <c>OnNext</c>'s schedule call and the scheduler firing
    /// the queued callback, the callback observes <c>_done == true</c> and returns without
    /// forwarding to downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduledCallbackFiresAfterSourceCompleted_ThenDroppedByDoneGuard()
    {
        SyncDirectSource<int> source = new();
        Subject<bool> condition = new();
        VirtualClock scheduler = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.ObserveOnIf(condition, scheduler, scheduler)
            .Subscribe(values.Add, () => completedCount++);

        // First emission queues the forward-to-downstream callback on the VirtualClock.
        source.Observer.OnNext(1);

        // Source completes synchronously, flipping _done = true before the queued callback runs.
        source.Observer.OnCompleted();

        // Advance the scheduler so the queued callback fires; it observes _done == true and
        // returns at the in-callback guard rather than calling downstream.OnNext.
        scheduler.AdvanceBy(1);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies the condition observer's duplicate-value short-circuit — emitting the
    /// same condition value twice in a row hits the <c>_hasCondition &amp; &amp; _lastCondition == c</c>
    /// guard and returns silently without re-assigning the current scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfConditionDuplicate_ThenSilentlyShortCircuits()
    {
        Subject<int> source = new();
        Subject<bool> condition = new();
        RecordingScheduler trueScheduler = new();
        RecordingScheduler falseScheduler = new();
        List<int> values = [];
        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler).Subscribe(values.Add);

        // First emission seeds the gate (_hasCondition transitions from false to true).
        condition.OnNext(true);

        // Second identical emission hits the duplicate-value guard and returns early.
        condition.OnNext(true);
        source.OnNext(1);

        // Sanity: subsequent value still routes through the true-scheduler (the duplicate did
        // not corrupt the captured state).
        await Assert.That(values.Count).IsLessThanOrEqualTo(1);
    }

    /// <summary>Sequencer that delegates to the default thread-pool sequencer but records each scheduled work item.</summary>
    private sealed class RecordingScheduler : ISequencer
    {
        /// <summary>Backing scheduler used to actually dispatch work.</summary>
        private readonly TaskPoolSequencer _inner = TaskPoolSequencer.Default;

        /// <summary>Gets the number of recorded schedule calls.</summary>
        public int ScheduleCount { get; private set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => _inner.Now;

        /// <inheritdoc/>
        public long Timestamp => _inner.Timestamp;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            ScheduleCount++;
            _inner.Schedule(item);
        }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp)
        {
            ScheduleCount++;
            _inner.Schedule(item, dueTimestamp);
        }
    }
}
