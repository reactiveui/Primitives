// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests task-pool sequencer behavior.</summary>
public class ConcurencyTests
{
    /// <summary>Defines the maximum time to wait for scheduled work in tests.</summary>
    private static readonly TimeSpan ScheduleTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Defines the maximum tolerated difference between sequencer and system time.</summary>
    private static readonly TimeSpan ClockTolerance = TimeSpan.FromSeconds(1);

    /// <summary>Defines the short due time used by delayed scheduling tests.</summary>
    private static readonly TimeSpan ShortDueTime = TimeSpan.FromMilliseconds(10);

    /// <summary>Defines the due time used by cancellation tests.</summary>
    private static readonly TimeSpan CancelDueTime = TimeSpan.FromMilliseconds(200);

    /// <summary>Defines the observation window used after canceling scheduled work.</summary>
    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(400);

    /// <summary>Verifies that scheduling state returns a disposable.</summary>
    [Test]
    public void TestCreate()
    {
        var scheduler = TaskPoolSequencer.Instance;
        var disposable = scheduler.Schedule(0, (_, _) => EmptyDisposable.Instance);
        Assert.NotNull(disposable);
        disposable.Dispose();
    }

    /// <summary>Verifies that the task-pool sequencer reports current UTC time.</summary>
    [Test]
    public void TaskPoolNow()
    {
        var delta = TaskPoolSequencer.Instance.Now - TimeProvider.System.GetUtcNow();

        Assert.True(delta.Duration() < ClockTolerance);
    }

    /// <summary>Verifies that immediate work is scheduled on a different thread.</summary>
    [Test]
    public void TaskPoolScheduleAction()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        using var completed = new ManualResetEventSlim();
        using var scheduled = nt.Schedule(() =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            completed.Set();
        });

        Assert.True(completed.Wait(ScheduleTimeout));
    }

    /// <summary>Verifies that work due immediately is scheduled on a different thread.</summary>
    [Test]
    public void TaskPoolScheduleActionDueNow()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        using var completed = new ManualResetEventSlim();
        using var scheduled = nt.Schedule(TimeSpan.Zero, () =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            completed.Set();
        });

        Assert.True(completed.Wait(ScheduleTimeout));
    }

    /// <summary>Verifies that delayed work is scheduled on a different thread.</summary>
    [Test]
    public void TaskPoolScheduleActionDue()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        using var completed = new ManualResetEventSlim();
        using var scheduled = nt.Schedule(ShortDueTime, () =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            completed.Set();
        });

        Assert.True(completed.Wait(ScheduleTimeout));
    }

    /// <summary>Verifies that canceled delayed work does not run.</summary>
    [Test]
    public void TaskPoolScheduleActionCancel()
    {
        var nt = TaskPoolSequencer.Instance;
        var runCount = 0;
        using var completed = new ManualResetEventSlim();
        using var scheduled = nt.Schedule(CancelDueTime, () =>
        {
            Volatile.Write(ref runCount, 1);
            completed.Set();
        });

        scheduled.Dispose();

        Assert.False(completed.Wait(CancelObservationWindow));
        Assert.Equal(0, Volatile.Read(ref runCount));
    }

    /// <summary>Verifies that delays larger than <see cref="int.MaxValue"/> milliseconds are accepted.</summary>
    [Test]
    public void TaskPoolDelayLargerThanIntMaxValue()
    {
        var dueTime = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);

        using var scheduled = TaskPoolSequencer.Instance.Schedule(dueTime, () => { });

        Assert.NotNull(scheduled);
    }
}
