// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

public class CoreRuntimeContractTests
{
    [Test]
    public void CompletedSparksAreCachedPerValueType()
    {
        var first = Spark.CreateOnCompleted<int>();
        var second = Spark.CreateOnCompleted<int>();

        Assert.Same(first, second);
        Assert.True(first == second);
    }

    [Test]
    public void WitnessCreateRoutesCallbacks()
    {
        var calls = new List<string>();
        var error = new InvalidOperationException("boom");
        var witness = Witness.Create<int>(
            value => calls.Add("N" + value),
            ex => calls.Add("E" + ex.Message),
            () => calls.Add("C"));

        witness.OnNext(7);
        witness.OnError(error);
        witness.OnCompleted();

        Assert.Equal(new[] { "N7", "Eboom", "C" }, calls);
    }

    [Test]
    public void SafeWitnessIgnoresSignalsAfterTerminalAndDisposesOnce()
    {
        var calls = new List<string>();
        var disposed = 0;
        var witness = Witness.Safe(
            Witness.Create<int>(
                value => calls.Add("N" + value),
                ex => calls.Add("E" + ex.Message),
                () => calls.Add("C")),
            Disposable.Create(() => disposed++));

        witness.OnNext(1);
        witness.OnCompleted();
        witness.OnNext(2);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();

        Assert.Equal(new[] { "N1", "C" }, calls);
        Assert.Equal(1, disposed);
    }

    [Test]
    public void DisposableCreateNullActionReturnsEmptyDisposable()
    {
        var disposable = Disposable.Create(null!);

        disposable.Dispose();
        Assert.Same(Disposable.Empty, disposable);
    }

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

    [Test]
    public void SingleReplaceableDisposableRunsActionOnlyOnce()
    {
        var actionCount = 0;
        var slot = new SingleReplaceableDisposable(() => actionCount++);

        slot.Dispose();
        slot.Dispose();

        Assert.Equal(1, actionCount);
    }

    [Test]
    public void CurrentThreadSequencerQueuesNestedWorkUntilCurrentActionCompletes()
    {
        var calls = new List<int>();

        Sequencer.CurrentThread.Schedule(() =>
        {
            calls.Add(1);
            Sequencer.CurrentThread.Schedule(() => calls.Add(3));
            calls.Add(2);
        });

        Assert.Equal(new[] { 1, 2, 3 }, calls);
    }

    [Test]
    public void ImmediateSequencerHonorsAbsoluteDueTime()
    {
        var elapsed = Stopwatch.StartNew();

        Sequencer.Immediate.Schedule(Sequencer.Immediate.Now + TimeSpan.FromMilliseconds(30), () => { });

        elapsed.Stop();
        Assert.True(elapsed.Elapsed >= TimeSpan.FromMilliseconds(20));
    }

    [Test]
    public void VirtualClockRunsScheduledWorkOnlyWhenAdvancedPastDueTime()
    {
        var clock = new VirtualClock();
        var calls = new List<long>();

        clock.Schedule(TimeSpan.FromTicks(10), () => calls.Add(clock.Clock.Ticks));

        clock.AdvanceBy(TimeSpan.FromTicks(9));
        Assert.Equal(0, calls.Count);

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(new[] { 10L }, calls);
    }

    [Test]
    public void SchedulerDefaultAliasesExposeMigrationFriendlyNames()
    {
        Assert.Same(TaskPoolSequencer.Instance, TaskPoolSequencer.Default);
        Assert.Same(TaskPoolSequencer.Default, Sequencer.Default);
        Assert.Same(ThreadPoolSequencer.Instance, ThreadPoolSequencer.Instance);
    }

    [Test]
    public void NullableValueTimeStructsUseDeterministicNullHashCodes()
    {
        var timestamp = new DateTimeOffset(2026, 5, 24, 22, 52, 0, TimeSpan.Zero);
        var moment = new Moment<string?>(null, timestamp);
        var interval = TimeSpan.FromMilliseconds(123);
        var timeInterval = new TimeInterval<string?>(null, interval);

        Assert.Equal(timestamp.GetHashCode() ^ 1963, moment.GetHashCode());
        Assert.Equal(interval.GetHashCode() ^ 1963, timeInterval.GetHashCode());
    }

    [Test]
    public void ScheduledItemConstructorValidatesSchedulerAndAction()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ScheduledItem<DateTimeOffset, int>(null!, 42, (_, _) => Disposable.Empty, DateTimeOffset.UnixEpoch));

        Assert.Throws<ArgumentNullException>(() =>
            new ScheduledItem<DateTimeOffset, int>(Sequencer.Immediate, 42, null!, DateTimeOffset.UnixEpoch));
    }
}
