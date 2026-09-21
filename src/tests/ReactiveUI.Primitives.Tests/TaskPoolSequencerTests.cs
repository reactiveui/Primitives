// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests task-pool sequencer dispatch and cancellation.</summary>
public class TaskPoolSequencerTests
{
    /// <summary>Scheduling state returns a cancellation handle.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TestCreate()
    {
        ManualTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));
        using var disposable = sequencer.Schedule(0, static (_, _) => EmptyDisposable.Instance);
        await Assert.That(disposable).IsNotNull();
        scheduler.RunPending();
    }

    /// <summary>The clock returns a UTC timestamp.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolNowUsesUtc() =>
        await Assert.That(TaskPoolSequencer.Instance.Now.Offset).IsEqualTo(TimeSpan.Zero);

    /// <summary>Immediate work is queued through the supplied task factory.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolScheduleAction()
    {
        ManualTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));
        StrongBox<bool> ran = new();
        using var scheduled = sequencer.Schedule(ran, static state => state.Value = true);
        await Assert.That(ran.Value).IsFalse();
        scheduler.RunPending();
        await Assert.That(ran.Value).IsTrue();
    }

    /// <summary>Work due immediately uses the task factory queue.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDueNow()
    {
        ManualTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));
        StrongBox<bool> ran = new();
        using var scheduled = sequencer.Schedule(ran, TimeSpan.Zero, static state => state.Value = true);
        await Assert.That(ran.Value).IsFalse();
        scheduler.RunPending();
        await Assert.That(ran.Value).IsTrue();
    }

    /// <summary>Delayed work reaches the task factory only after its delay callback runs.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDue()
    {
        ManualTaskScheduler scheduler = new();
        ManualSequencer delays = new();
        TaskPoolSequencer sequencer = new(new(scheduler), delays);
        StrongBox<bool> ran = new();
        using var scheduled = sequencer.Schedule(ran, DateTimeOffset.MaxValue, static (_, state) =>
        {
            state.Value = true;
            return EmptyDisposable.Instance;
        });
        scheduler.RunPending();
        await Assert.That(ran.Value).IsFalse();
        delays.RunPending();
        await Assert.That(ran.Value).IsFalse();
        scheduler.RunPending();
        await Assert.That(ran.Value).IsTrue();
    }

    /// <summary>Cancellation suppresses delayed dispatch.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolScheduleActionCancel()
    {
        ManualTaskScheduler scheduler = new();
        ManualSequencer delays = new();
        TaskPoolSequencer sequencer = new(new(scheduler), delays);
        StrongBox<int> runs = new();
        var scheduled = sequencer.Schedule(runs, DateTimeOffset.MaxValue, static (_, state) =>
        {
            state.Value++;
            return EmptyDisposable.Instance;
        });
        scheduled.Dispose();
        delays.RunPending();
        scheduler.RunPending();
        await Assert.That(runs.Value).IsEqualTo(0);
    }

    /// <summary>Large delays retain their cancellation handle.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolDelayLargerThanIntMaxValue()
    {
        ManualTaskScheduler scheduler = new();
        ManualSequencer delays = new();
        TaskPoolSequencer sequencer = new(new(scheduler), delays);
        var dueTime = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);
        using var scheduled = sequencer.Schedule(dueTime, static () => { });
        await Assert.That(scheduled).IsNotNull();
    }

    /// <summary>Cancellation between the delay callback and task dispatch suppresses the action.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Schedule_CancelAfterDelayBeforeDispatch_DropsWork()
    {
        using ManualThreadPool delays = new();
        ManualTaskScheduler tasks = new();
        TaskPoolSequencer sequencer = new(new(tasks), delays.Sequencer);
        StrongBox<int> runs = new();
        var scheduled = sequencer.Schedule(runs, TimeSpan.FromSeconds(1), static state => state.Value++);

        tasks.RunPending();
        await Assert.That(runs.Value).IsEqualTo(0);
        delays.RunDue(Sequencer.ToTimestampDelta(TimeSpan.FromSeconds(1)));
        scheduled.Dispose();
        tasks.RunPending();
        await Assert.That(runs.Value).IsEqualTo(0);
    }

    /// <summary>Work remains delayed until the supplied monotonic clock reaches its due timestamp.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Schedule_AdvanceClockToDue_DispatchesOnce()
    {
        using ManualThreadPool delays = new();
        ManualTaskScheduler tasks = new();
        TaskPoolSequencer sequencer = new(new(tasks), delays.Sequencer);
        StrongBox<int> runs = new();
        using var scheduled = sequencer.Schedule(runs, TimeSpan.FromSeconds(1), static state => state.Value++);
        var due = Sequencer.ToTimestampDelta(TimeSpan.FromSeconds(1));
        delays.RunDue(due - 1);
        tasks.RunPending();
        await Assert.That(runs.Value).IsEqualTo(0);
        delays.RunDue(due);
        await Assert.That(sequencer.Timestamp).IsEqualTo(due);
        await Assert.That(runs.Value).IsEqualTo(0);
        tasks.RunPending();
        await Assert.That(runs.Value).IsEqualTo(1);
    }

    /// <summary>A null task factory is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullTaskFactory_ThrowsArgumentNull() =>
        await Assert.That(static () => new TaskPoolSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>A null task factory or time provider is rejected by the provider constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProviderConstructorRejectsANullTaskFactoryOrProvider()
    {
        TimeProvider provider = new VirtualClock();

        await Assert.That(() => new TaskPoolSequencer(null!, provider)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new TaskPoolSequencer(new(new ManualTaskScheduler()), (TimeProvider)null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        TimeProvider provider = clock;
        TaskPoolSequencer sequencer = new(new(new ManualTaskScheduler()), provider);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(1));
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Delayed work reaches the task factory only when the provider is advanced to its timestamp.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkIsDispatchedWhenTheProviderReachesItsTimestamp()
    {
        VirtualClock clock = new();
        TimeProvider provider = clock;
        ManualTaskScheduler tasks = new();
        TaskPoolSequencer sequencer = new(new(tasks), provider);
        RecordingWorkItem item = new();
        var oneSecond = TimeSpan.FromSeconds(1);

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(oneSecond));
        clock.AdvanceBy(oneSecond - TimeSpan.FromTicks(1));
        tasks.RunPending();

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
        tasks.RunPending();

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Delayed work cancelled before its timestamp never runs.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancelledDelayedWorkIsNotDispatched()
    {
        VirtualClock clock = new();
        TimeProvider provider = clock;
        ManualTaskScheduler tasks = new();
        TaskPoolSequencer sequencer = new(new(tasks), provider);
        RecordingWorkItem item = new();
        var oneSecond = TimeSpan.FromSeconds(1);

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(oneSecond));
        item.Dispose();
        clock.AdvanceBy(oneSecond);
        tasks.RunPending();

        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }
}
