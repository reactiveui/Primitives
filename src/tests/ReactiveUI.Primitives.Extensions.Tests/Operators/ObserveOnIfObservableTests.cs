// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests scheduler selection from condition changes and terminal notification forwarding.</summary>
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
        var v2 = await emitted.Task;
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
        var v2 = await emitted.Task;
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
        using var sub = source.ObserveOnIf(condition, new RecordingScheduler(), Sequencer.Immediate).Subscribe(
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
        using var sub = source.ObserveOnIf(condition, new RecordingScheduler(), Sequencer.Immediate).Subscribe(
            static _ => { },
            () => completed = true);
        source.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies the single-scheduler overload emits synchronously while the condition is false.</summary>
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

    /// <summary>Verifies a queued emission is dropped when the source completes before the callback fires.</summary>
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

        // The emission is queued on the clock, so the completion below lands ahead of it.
        source.Observer.OnNext(1);
        source.Observer.OnCompleted();

        scheduler.AdvanceBy(1);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies a repeated condition value is ignored and leaves the selected scheduler in place.</summary>
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

        condition.OnNext(true);
        condition.OnNext(true);
        source.OnNext(1);

        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>Sequencer that delegates to the default thread-pool sequencer but records each scheduled work item.</summary>
    private sealed class RecordingScheduler : ISequencer
    {
        /// <summary>Backing scheduler used to actually dispatch work.</summary>
        private readonly ImmediateSequencer _inner = Sequencer.Immediate;

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
