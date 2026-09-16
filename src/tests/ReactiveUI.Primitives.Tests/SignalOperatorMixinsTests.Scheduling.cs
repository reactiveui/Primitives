// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests scheduled delivery and virtual-time operator behavior.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>Timestamp stamps every value from its immediate or clock-backed sequencer and rejects null observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyTimestampBranches()
    {
        RecordingWitness<Moment<int>> immediateMoments = new();
        Signal.Sequence(One, Three).Timestamp(Sequencer.Immediate).Subscribe(immediateMoments).Dispose();
        IEnumerable<int> expectedImmediateMoments = [One, Two, Three];
        int[] immediateMomentValues =
            [immediateMoments.Values[0].Value, immediateMoments.Values[1].Value, immediateMoments.Values[Two].Value];
        await Assert.That(immediateMomentValues.SequenceEqual(expectedImmediateMoments)).IsTrue();
        await Assert.That(immediateMoments.Completed).IsEqualTo(1);
        List<Moment<int>> clockMoments = [];
        var clockMomentCompleted = 0;
        _ = Signal.Sequence(Four, Two).Timestamp(new VirtualClock(DateTimeOffset.UnixEpoch))
            .Subscribe(clockMoments.Add, static ex => throw ex, () => clockMomentCompleted++);
        IEnumerable<int> expectedClockMoments = [Four, Five];
        int[] clockMomentValues = [clockMoments[0].Value, clockMoments[1].Value];
        await Assert.That(clockMomentValues.SequenceEqual(expectedClockMoments)).IsTrue();
        await Assert.That(clockMomentCompleted).IsEqualTo(1);
        List<Moment<int>> immediateMomentActions = [];
        var immediateMomentCompleted = 0;
        var immediateTimestampSignal =
            (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(Sequencer.Immediate);
        immediateTimestampSignal.Subscribe(immediateMomentActions.Add, static ex => throw ex, () => immediateMomentCompleted++)
            .Dispose();
        IEnumerable<int> expectedImmediateMomentActions = [Two, Three];
        int[] immediateMomentActionValues = [immediateMomentActions[0].Value, immediateMomentActions[1].Value];
        await Assert.That(immediateMomentActionValues.SequenceEqual(expectedImmediateMomentActions)).IsTrue();
        await Assert.That(immediateMomentCompleted).IsEqualTo(1);
        RecordingWitness<Moment<int>> clockMomentObserver = new();
        var clockTimestampSignal =
            (IInlineSignal<Moment<int>>)Signal.Sequence(Two, Two).Timestamp(new VirtualClock(DateTimeOffset.UnixEpoch));
        clockTimestampSignal.Subscribe(clockMomentObserver).Dispose();
        IEnumerable<int> expectedClockMomentObserver = [Two, Three];
        int[] clockMomentObserverValues = [clockMomentObserver.Values[0].Value, clockMomentObserver.Values[1].Value];
        await Assert.That(clockMomentObserverValues.SequenceEqual(expectedClockMomentObserver)).IsTrue();
        await Assert.That(clockMomentObserver.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateTimestampSignal.Subscribe((IObserver<Moment<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateTimestampSignal.Subscribe((Action<Moment<int>>)null!, static _ => { }, static () => { }));
    }

    /// <summary>Time-interval reports a zero gap for values its sequencer delivers without advancing the clock.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyTimeIntervalBranches()
    {
        RecordingWitness<TimeInterval<int>> immediateIntervals = new();
        Signal.Sequence(One, Three).TimeInterval(Sequencer.Immediate).Subscribe(immediateIntervals).Dispose();
        IEnumerable<int> expectedImmediateIntervals = [One, Two, Three];
        int[] immediateIntervalValues =
        [
            immediateIntervals.Values[0].Value, immediateIntervals.Values[1].Value, immediateIntervals.Values[Two].Value
        ];
        await Assert.That(immediateIntervalValues.SequenceEqual(expectedImmediateIntervals)).IsTrue();
        await Assert.That(immediateIntervals.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(immediateIntervals.Completed).IsEqualTo(1);
        List<TimeInterval<int>> clockIntervals = [];
        var clockIntervalCompleted = 0;
        _ = Signal.Sequence(Four, Three).TimeInterval(new VirtualClock(DateTimeOffset.UnixEpoch))
            .Subscribe(clockIntervals.Add, static ex => throw ex, () => clockIntervalCompleted++);
        IEnumerable<int> expectedClockIntervals = [Four, Five, Six];
        int[] clockIntervalValues = [clockIntervals[0].Value, clockIntervals[1].Value, clockIntervals[Two].Value];
        await Assert.That(clockIntervalValues.SequenceEqual(expectedClockIntervals)).IsTrue();
        await Assert.That(clockIntervals[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervals[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalCompleted).IsEqualTo(1);
        List<TimeInterval<int>> immediateIntervalActions = [];
        var immediateIntervalCompleted = 0;
        var immediateIntervalSignal =
            (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Two).TimeInterval(Sequencer.Immediate);
        immediateIntervalSignal
            .Subscribe(immediateIntervalActions.Add, static ex => throw ex, () => immediateIntervalCompleted++)
            .Dispose();
        IEnumerable<int> expectedImmediateIntervalActions = [Two, Three];
        int[] immediateIntervalActionValues = [immediateIntervalActions[0].Value, immediateIntervalActions[1].Value];
        await Assert.That(immediateIntervalActionValues.SequenceEqual(expectedImmediateIntervalActions)).IsTrue();
        await Assert.That(immediateIntervalCompleted).IsEqualTo(1);
        RecordingWitness<TimeInterval<int>> clockIntervalObserver = new();
        var clockIntervalSignal =
            (IInlineSignal<TimeInterval<int>>)Signal.Sequence(Two, Three)
                .TimeInterval(new VirtualClock(DateTimeOffset.UnixEpoch));
        clockIntervalSignal.Subscribe(clockIntervalObserver).Dispose();
        IEnumerable<int> expectedClockIntervalObserver = [Two, Three, Four];
        int[] clockIntervalObserverValues =
        [
            clockIntervalObserver.Values[0].Value, clockIntervalObserver.Values[1].Value,
            clockIntervalObserver.Values[Two].Value
        ];
        await Assert.That(clockIntervalObserverValues.SequenceEqual(expectedClockIntervalObserver)).IsTrue();
        await Assert.That(clockIntervalObserver.Values[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[1].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Values[Two].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(clockIntervalObserver.Completed).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateIntervalSignal.Subscribe((IObserver<TimeInterval<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            immediateIntervalSignal.Subscribe((Action<TimeInterval<int>>)null!, static _ => { }, static () => { }));
    }

    /// <summary>Verifies delay-start delivery, the sequencer work item, and the queue guard clauses.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifyDelayStartAndWorkItemBranches()
    {
        RecordingWitness<int> shiftedObserver = new();
        Signal.Sequence(One, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate).Subscribe(shiftedObserver).Dispose();
        int[] expectedShiftedObserver = [One, Two];
        await Assert.That(shiftedObserver.Values.SequenceEqual(expectedShiftedObserver)).IsTrue();
        await Assert.That(shiftedObserver.Completed).IsEqualTo(1);
        List<int> shiftedActions = [];
        var shiftedActionCompleted = 0;
        _ = Signal.Sequence(Three, Two).DelayStart(TimeSpan.Zero, Sequencer.Immediate)
            .Subscribe(shiftedActions.Add, static ex => throw ex, () => shiftedActionCompleted++);
        int[] expectedShiftedActions = [Three, Four];
        await Assert.That(shiftedActions.SequenceEqual(expectedShiftedActions)).IsTrue();
        await Assert.That(shiftedActionCompleted).IsEqualTo(1);
        var currentThreadShift =
            (IRequireCurrentThread<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.CurrentThread);
        await Assert.That(currentThreadShift.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        var inlineShift = (IInlineSignal<int>)Signal.Sequence(One, One).DelayStart(TimeSpan.Zero, Sequencer.Immediate);
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Sequence(One, One)
            .DelayStart(TimeSpan.Zero, Sequencer.Immediate)
            .Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe((Action<int>)null!, static _ => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => inlineShift.Subscribe(static _ => { }, static _ => { }, null!));
        var unusedScheduled =
            ScheduledItem.Create(Sequencer.Immediate, "unused", static (_, _) => EmptyDisposable.Instance, One);
        await Assert.That(new SequencerQueue<int>().Remove(unusedScheduled)).IsFalse();
        _ = Assert.Throws<ArgumentOutOfRangeException>(CreatePriorityQueueWithInvalidCapacity);
        PriorityQueue<int> shrink = new(ThirtyTwo);
        for (var i = 0; i < ThirtyTwo; i++)
        {
            shrink.Enqueue(i);
        }

        for (var i = 0; i < TwentySix; i++)
        {
            await Assert.That(shrink.Dequeue()).IsEqualTo(i);
        }
    }

    /// <summary>Verifies absolute thread pool scheduling and the disposal of a scheduled work item.</summary>
    /// <returns>A task representing the asynchronous verification.</returns>
    private static async Task VerifyThreadPoolWorkItemBranchesAsync()
{
        using ManualThreadPool pool = new();
        var absoluteValue = 0;
        var absolute = pool.Sequencer.Schedule(Five, FixedTimestamp, (_, state) =>
        {
            absoluteValue = state;
            return EmptyDisposable.Instance;
        });
        pool.RunReady();
        await Assert.That(absoluteValue).IsEqualTo(Five);
        absolute.Dispose();
        absolute.Dispose();
        var skipped = false;
        ThreadPoolSequencer.ScheduledWorkItem<int> delayed = new(pool.Sequencer, One, (_, _) =>
        {
            skipped = true;
            return EmptyDisposable.Instance;
        });
        delayed.Dispose();
        delayed.Queue(TimeSpan.FromTicks(Ten));
        pool.RunDue(long.MaxValue);
        delayed.Execute();
        await Assert.That(skipped).IsFalse();
        var disposedReturned = 0;
        ThreadPoolSequencer.ScheduledWorkItem<int>? selfDisposing = null;
        selfDisposing = new(pool.Sequencer, One, (_, _) =>
        {
            selfDisposing!.Dispose();
            return new ActionDisposable(() => disposedReturned++);
        });
        selfDisposing.Execute();
        await Assert.That(disposedReturned).IsEqualTo(1);
    }

    /// <summary>Creates a priority queue with an invalid capacity.</summary>
    private static void CreatePriorityQueueWithInvalidCapacity()
    {
        PriorityQueue<int> invalid = new(-1);
        GC.KeepAlive(invalid);
    }
}
