// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Threading;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// ConcurencyTests.
/// </summary>
public class ConcurencyTests
{
    /// <summary>
    /// Tests this instance.
    /// </summary>
    [Test]
    public void TestCreate()
    {
        var scheduler = TaskPoolSequencer.Instance;
        var disposable = scheduler.Schedule(0, (__, _) => Disposable.Empty);
        Assert.NotNull(disposable);
        disposable.Dispose();
    }

    /// <summary>
    /// Tasks the pool now.
    /// </summary>
    [Test]
    public void TaskPoolNow()
    {
        var res = TaskPoolSequencer.Instance.Now - DateTime.Now;
        Assert.True(res.Seconds < 1);
    }

    /// <summary>
    /// Tasks the pool schedule action.
    /// </summary>
    [Test]
    public void TaskPoolScheduleAction()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        var evt = new ManualResetEvent(false);
        nt.Schedule(() =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            evt.Set();
        });
        evt.WaitOne();
    }

    /// <summary>
    /// Tasks the pool schedule action due now.
    /// </summary>
    [Test]
    public void TaskPoolScheduleActionDueNow()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        var evt = new ManualResetEvent(false);
        nt.Schedule(TimeSpan.Zero, () =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            evt.Set();
        });
        evt.WaitOne();
    }

    /// <summary>
    /// Tasks the pool schedule action due.
    /// </summary>
    [Test]
    public void TaskPoolScheduleActionDue()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        var evt = new ManualResetEvent(false);
        nt.Schedule(TimeSpan.FromMilliseconds(1), () =>
        {
            Assert.NotEqual(id, Environment.CurrentManagedThreadId);
            evt.Set();
        });
        evt.WaitOne();
    }

    /// <summary>
    /// Tasks the pool schedule action cancel.
    /// </summary>
    [Test]
    public void TaskPoolScheduleActionCancel()
    {
        var id = Environment.CurrentManagedThreadId;
        var nt = TaskPoolSequencer.Instance;
        var set = false;
        var d = nt.Schedule(TimeSpan.FromSeconds(0.2), () => set = true);
        d.Dispose();
        Thread.Sleep(400);
        Assert.False(set);
    }

    /// <summary>
    /// Tasks the pool delay larger than int maximum value.
    /// </summary>
    [Test]
    public void TaskPoolDelayLargerThanIntMaxValue()
    {
        var dueTime = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);

        // Just ensuring the call to Schedule does not throw.
        var d = TaskPoolSequencer.Instance.Schedule(dueTime, () => { });

        d.Dispose();
    }
}
