// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies core runtime contracts for sparks, witnesses, disposables, and sequencers.
/// </summary>
public class CoreRuntimeContractTests
{
    /// <summary>
    /// Verifies completed sparks compare equal by value for each value type.
    /// </summary>
    [Test]
    public void CompletedSparksAreEqualPerValueType()
    {
        var first = Spark.CreateOnCompleted<int>();
        var second = Spark.CreateOnCompleted<int>();

        Assert.True(first == second);
        Assert.Equal(first, second);
    }

    /// <summary>
    /// Verifies delegate witnesses route next, error, and completion callbacks.
    /// </summary>
    [Test]
    public void WitnessCreateRoutesCallbacks()
    {
        const int ObservedValue = 7;
        var calls = new List<string>();
        var error = new InvalidOperationException("boom");
        var witness = Witness.Create<int>(
            value => calls.Add("N" + value),
            ex => calls.Add("E" + ex.Message),
            () => calls.Add("C"));

        witness.OnNext(ObservedValue);
        witness.OnError(error);
        witness.OnCompleted();

        var expected = new[] { "N" + ObservedValue, "Eboom", "C" };
        Assert.Equal(expected, calls);
    }

    /// <summary>
    /// Verifies safe witnesses ignore notifications after termination and dispose once.
    /// </summary>
    [Test]
    public void SafeWitnessIgnoresSignalsAfterTerminalAndDisposesOnce()
    {
        const int FirstValue = 1;
        const int LateValue = 2;
        var calls = new List<string>();
        var disposed = 0;
        var witness = Witness.Safe(
            Witness.Create<int>(
                value => calls.Add("N" + value),
                ex => calls.Add("E" + ex.Message),
                () => calls.Add("C")),
            Disposable.Create(() => disposed++));

        witness.OnNext(FirstValue);
        witness.OnCompleted();
        witness.OnNext(LateValue);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();

        var expected = new[] { "N" + FirstValue, "C" };
        Assert.Equal(expected, calls);
        Assert.Equal(1, disposed);
    }

    /// <summary>
    /// Verifies a null disposable action uses the shared empty disposable.
    /// </summary>
    [Test]
    public void DisposableCreateNullActionReturnsEmptyDisposable()
    {
        var disposable = Disposable.Create(null!);

        disposable.Dispose();
        Assert.Same(Disposable.Empty, disposable);
    }

    /// <summary>
    /// Verifies removing one disposable leaves the others attached until disposal.
    /// </summary>
    [Test]
    public void MultipleDisposableRemoveDisposesOnlyTheRequestedItem()
    {
        var first = 0;
        var second = 0;
        var firstDisposable = Disposable.Create(() => first++);
        var secondDisposable = Disposable.Create(() => second++);
        var pocket = new MultipleDisposable(firstDisposable, secondDisposable);

        Assert.True(pocket.Remove(firstDisposable));

        Assert.Equal(1, first);
        Assert.Equal(0, second);

        pocket.Dispose();

        Assert.Equal(1, first);
        Assert.Equal(1, second);
    }

    /// <summary>
    /// Verifies assigning a disposed single slot disposes the incoming disposable immediately.
    /// </summary>
    [Test]
    public void SingleDisposableCreateAfterDisposeDisposesIncomingDisposableImmediately()
    {
        var disposed = 0;
        var slot = new SingleDisposable();

        slot.Dispose();
        slot.Create(Disposable.Create(() => disposed++));

        Assert.True(slot.IsDisposed);
        Assert.Equal(1, disposed);
    }

    /// <summary>
    /// Verifies a replaceable disposable invokes its disposal action only once.
    /// </summary>
    [Test]
    public void SingleReplaceableDisposableRunsActionOnlyOnce()
    {
        var actionCount = 0;
        var slot = new SingleReplaceableDisposable(() => actionCount++);

        slot.Dispose();
        slot.Dispose();

        Assert.Equal(1, actionCount);
    }

    /// <summary>
    /// Verifies nested current-thread work is queued until the current action finishes.
    /// </summary>
    [Test]
    public void CurrentThreadSequencerQueuesNestedWorkUntilCurrentActionCompletes()
    {
        const int FirstCall = 1;
        const int SecondCall = 2;
        const int ThirdCall = 3;
        var calls = new List<int>();

        Sequencer.CurrentThread.Schedule(() =>
        {
            calls.Add(FirstCall);
            Sequencer.CurrentThread.Schedule(() => calls.Add(ThirdCall));
            calls.Add(SecondCall);
        });

        var expected = new[] { FirstCall, SecondCall, ThirdCall };
        Assert.Equal(expected, calls);
    }

    /// <summary>
    /// Verifies the immediate sequencer waits until an absolute due time.
    /// </summary>
    [Test]
    public void ImmediateSequencerHonorsAbsoluteDueTime()
    {
        var elapsed = Stopwatch.StartNew();

        Sequencer.Immediate.Schedule(Sequencer.Immediate.Now + TimeSpan.FromMilliseconds(30), () => { });

        elapsed.Stop();
        Assert.True(elapsed.Elapsed >= TimeSpan.FromMilliseconds(20));
    }

    /// <summary>
    /// Verifies virtual-clock work runs only after the clock reaches the due time.
    /// </summary>
    [Test]
    public void VirtualClockRunsScheduledWorkOnlyWhenAdvancedPastDueTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        var clock = new VirtualClock();
        var calls = new List<long>();

        clock.Schedule(TimeSpan.FromTicks(DueTicks), () => calls.Add(clock.Clock.Ticks));

        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        Assert.Equal(0, calls.Count);

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        var expected = new[] { DueTicks };
        Assert.Equal(expected, calls);
    }

    /// <summary>
    /// Verifies virtual-clock timestamp scheduling converts monotonic ticks back to virtual time.
    /// </summary>
    [Test]
    public void VirtualClockConvertsMonotonicTimestampDeltasToVirtualTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        var clock = new VirtualClock();
        var calls = new List<long>();
        var dueTimestamp = Sequencer.AddTimestamp(clock.Timestamp, TimeSpan.FromTicks(DueTicks));

        clock.Schedule(new CallbackWorkItem(() => calls.Add(clock.Clock.Ticks)), dueTimestamp);

        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        Assert.Equal(0, calls.Count);

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        var expected = new[] { DueTicks };
        Assert.Equal(expected, calls);
    }

    /// <summary>
    /// Verifies default sequencer aliases expose migration-friendly names.
    /// </summary>
    [Test]
    public void SchedulerDefaultAliasesExposeMigrationFriendlyNames()
    {
        Assert.Same(TaskPoolSequencer.Instance, TaskPoolSequencer.Default);
        Assert.Same(TaskPoolSequencer.Default, Sequencer.Default);
        Assert.Same(ThreadPoolSequencer.Instance, ThreadPoolSequencer.Instance);
    }

    /// <summary>
    /// Verifies nullable time value structs use deterministic null hash codes.
    /// </summary>
    [Test]
    public void NullableValueTimeStructsUseDeterministicNullHashCodes()
    {
        const int NullHashSeed = 1963;
        var timestamp = new DateTimeOffset(2026, 5, 24, 22, 52, 0, TimeSpan.Zero);
        var moment = new Moment<string?>(null, timestamp);
        var interval = TimeSpan.FromMilliseconds(123);
        var timeInterval = new TimeInterval<string?>(null, interval);

        Assert.Equal(timestamp.GetHashCode() ^ NullHashSeed, moment.GetHashCode());
        Assert.Equal(interval.GetHashCode() ^ NullHashSeed, timeInterval.GetHashCode());
    }

    /// <summary>
    /// Verifies scheduled-item constructor argument validation.
    /// </summary>
    [Test]
    public void ScheduledItemConstructorValidatesSchedulerAndAction()
    {
        const int State = 42;

        Assert.Throws<ArgumentNullException>(() =>
            CreateScheduledItem(null!, State, (_, _) => Disposable.Empty));

        Assert.Throws<ArgumentNullException>(() =>
            CreateScheduledItem(Sequencer.Immediate, State, null!));

        static void CreateScheduledItem(
            ISequencer scheduler,
            int state,
            Func<ISequencer, int, IDisposable> action) =>
            GC.KeepAlive(new ScheduledItem<DateTimeOffset, int>(scheduler, state, action, DateTimeOffset.UnixEpoch));
    }

    /// <summary>
    /// Test work item that invokes a supplied callback.
    /// </summary>
    private sealed class CallbackWorkItem : IWorkItem
    {
        /// <summary>
        /// Callback invoked by the work item.
        /// </summary>
        private readonly Action _callback;

        /// <summary>
        /// Initializes a new instance of the <see cref="CallbackWorkItem"/> class.
        /// </summary>
        /// <param name="callback">Callback invoked by the work item.</param>
        public CallbackWorkItem(Action callback) => _callback = callback;

        /// <inheritdoc/>
        public void Execute() => _callback();
    }
}
