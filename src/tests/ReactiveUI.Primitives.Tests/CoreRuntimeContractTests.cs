// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103 // Coverage tests intentionally group branch-heavy scenarios.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies core runtime contracts for sparks, witnesses, disposables, and sequencers.</summary>
public class CoreRuntimeContractTests
{
    /// <summary>Verifies completed sparks compare equal by value for each value type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletedSparksAreEqualPerValueType()
    {
        var first = Spark.CreateOnCompleted<int>();
        var second = Spark.CreateOnCompleted<int>();
        await Assert.That(first == second).IsTrue();
        await Assert.That(second).IsEqualTo(first);
    }

    /// <summary>Verifies delegate witnesses route next, error, and completion callbacks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WitnessCreateRoutesCallbacks()
    {
        const int ObservedValue = 7;
        var calls = new List<string>();
        var error = new InvalidOperationException("boom");
        var witness = Witness.Create<int>(value => calls.Add("N" + value), ex => calls.Add("E" + ex.Message), () => calls.Add("C"));
        witness.OnNext(ObservedValue);
        witness.OnError(error);
        witness.OnCompleted();
        var expected = new[]
        {
            "N" + ObservedValue,
            "Eboom",
            "C"
        };
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies safe witnesses ignore notifications after termination and dispose once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SafeWitnessIgnoresSignalsAfterTerminalAndDisposesOnce()
    {
        const int FirstValue = 1;
        const int LateValue = 2;
        var calls = new List<string>();
        var disposed = 0;
        var witness = Witness.Safe(Witness.Create<int>(value => calls.Add("N" + value), ex => calls.Add("E" + ex.Message), () => calls.Add("C")), new ActionDisposable(() => disposed++));
        witness.OnNext(FirstValue);
        witness.OnCompleted();
        witness.OnNext(LateValue);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();
        var expected = new[]
        {
            "N" + FirstValue,
            "C"
        };
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies a null disposable action uses the shared empty disposable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ActionDisposableNullActionIsDisposedAfterDispose()
    {
        var disposable = new ActionDisposable(null!);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Verifies removing one disposable leaves the others attached until disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MultipleDisposableRemoveDisposesOnlyTheRequestedItem()
    {
        var first = 0;
        var second = 0;
        var firstDisposable = new ActionDisposable(() => first++);
        var secondDisposable = new ActionDisposable(() => second++);
        var pocket = new MultipleDisposable(firstDisposable, secondDisposable);
        await Assert.That(pocket.Remove(firstDisposable)).IsTrue();
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(0);
        pocket.Dispose();
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(1);
    }

    /// <summary>Verifies assigning a disposed single slot disposes the incoming disposable immediately.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleDisposableCreateAfterDisposeDisposesIncomingDisposableImmediately()
    {
        var disposed = 0;
        var slot = new SingleDisposable();
        slot.Dispose();
        slot.Create(new ActionDisposable(() => disposed++));
        await Assert.That(slot.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies a replaceable disposable invokes its disposal action only once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleReplaceableDisposableRunsActionOnlyOnce()
    {
        var actionCount = 0;
        var slot = new SingleReplaceableDisposable(() => actionCount++);
        slot.Dispose();
        slot.Dispose();
        await Assert.That(actionCount).IsEqualTo(1);
    }

    /// <summary>Verifies nested current-thread work is queued until the current action finishes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CurrentThreadSequencerQueuesNestedWorkUntilCurrentActionCompletes()
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
        var expected = new[]
        {
            FirstCall,
            SecondCall,
            ThirdCall
        };
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies the immediate sequencer waits until an absolute due time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateSequencerHonorsAbsoluteDueTime()
    {
        var elapsed = Stopwatch.StartNew();
        Sequencer.Immediate.Schedule(Sequencer.Immediate.Now + TimeSpan.FromMilliseconds(30), () =>
        {
        });
        elapsed.Stop();
        await Assert.That(elapsed.Elapsed >= TimeSpan.FromMilliseconds(20)).IsTrue();
    }

    /// <summary>Verifies virtual-clock work runs only after the clock reaches the due time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockRunsScheduledWorkOnlyWhenAdvancedPastDueTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        var clock = new VirtualClock();
        var calls = new List<long>();
        clock.Schedule(TimeSpan.FromTicks(DueTicks), () => calls.Add(clock.Clock.Ticks));
        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        await Assert.That(calls.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        var expected = new[]
        {
            DueTicks
        };
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies virtual-clock timestamp scheduling converts monotonic ticks back to virtual time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockConvertsMonotonicTimestampDeltasToVirtualTime()
    {
        const long DueTicks = 10;
        const long BeforeDueTicks = 9;
        var clock = new VirtualClock();
        var calls = new List<long>();
        var dueTimestamp = Sequencer.AddTimestamp(clock.Timestamp, TimeSpan.FromTicks(DueTicks));
        clock.Schedule(new CallbackWorkItem(() => calls.Add(clock.Clock.Ticks)), dueTimestamp);
        clock.AdvanceBy(TimeSpan.FromTicks(BeforeDueTicks));
        await Assert.That(calls.Count).IsEqualTo(0);
        clock.AdvanceBy(TimeSpan.FromTicks(1));
        var expected = new[]
        {
            DueTicks
        };
        await Assert.That(calls.SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Verifies default sequencer aliases expose migration-friendly names.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SchedulerDefaultAliasesExposeMigrationFriendlyNames()
    {
        await Assert.That(TaskPoolSequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Instance);
        await Assert.That(Sequencer.Default).IsSameReferenceAs(TaskPoolSequencer.Default);
        await Assert.That(ThreadPoolSequencer.Instance).IsSameReferenceAs(ThreadPoolSequencer.Instance);
    }

    /// <summary>Verifies nullable time value structs use deterministic null hash codes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NullableValueTimeStructsUseDeterministicNullHashCodes()
    {
        const int NullHashSeed = 1963;
        var timestamp = new DateTimeOffset(2026, 5, 24, 22, 52, 0, TimeSpan.Zero);
        var moment = new Moment<string?>(null, timestamp);
        var interval = TimeSpan.FromMilliseconds(123);
        var timeInterval = new TimeInterval<string?>(null, interval);
        await Assert.That(moment.GetHashCode()).IsEqualTo(timestamp.GetHashCode() ^ NullHashSeed);
        await Assert.That(timeInterval.GetHashCode()).IsEqualTo(interval.GetHashCode() ^ NullHashSeed);
    }

    /// <summary>Verifies scheduled-item constructor argument validation.</summary>
    [Test]
    public void ScheduledItemConstructorValidatesSchedulerAndAction()
    {
        const int State = 42;
        Assert.Throws<ArgumentNullException>(() => CreateScheduledItem(null!, State, (_, _) => EmptyDisposable.Instance));
        Assert.Throws<ArgumentNullException>(() => CreateScheduledItem(Sequencer.Immediate, State, null!));
        static void CreateScheduledItem(ISequencer scheduler, int state, Func<ISequencer, int, IDisposable> action) => GC.KeepAlive(new ScheduledItem<DateTimeOffset, int>(scheduler, state, action, DateTimeOffset.UnixEpoch));
    }

    /// <summary>Test work item that invokes a supplied callback.</summary>
    private sealed class CallbackWorkItem : IWorkItem
    {
        /// <summary>Callback invoked by the work item.</summary>
        private readonly Action _callback;

        /// <summary>Initializes a new instance of the <see cref = "CallbackWorkItem"/> class.</summary>
        /// <param name = "callback">Callback invoked by the work item.</param>
        public CallbackWorkItem(Action callback) => _callback = callback;

        /// <inheritdoc/>
        public void Execute() => _callback();
    }
}
