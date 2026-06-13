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
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TestCreate()
    {
        var scheduler = TaskPoolSequencer.Instance;
        var disposable = scheduler.Schedule(0, (_, _) => EmptyDisposable.Instance);
        await Assert.That(disposable).IsNotNull();
        disposable.Dispose();
    }

    /// <summary>Verifies that the task-pool sequencer reports current UTC time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolNow()
    {
        var delta = TaskPoolSequencer.Instance.Now - TimeProvider.System.GetUtcNow();
        await Assert.That(delta.Duration() < ClockTolerance).IsTrue();
    }

    /// <summary>Verifies that immediate work is scheduled asynchronously.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleAction()
    {
        var nt = TaskPoolSequencer.Instance;
        var schedulingThreadId = Environment.CurrentManagedThreadId;
        var scheduling = 1;
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scheduled = nt.Schedule(() => completed.TrySetResult(
            Environment.CurrentManagedThreadId == schedulingThreadId &&
            Volatile.Read(ref scheduling) != 0));
        Volatile.Write(ref scheduling, 0);
        var ranInline = await completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that work due immediately is scheduled asynchronously.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDueNow()
    {
        var nt = TaskPoolSequencer.Instance;
        var schedulingThreadId = Environment.CurrentManagedThreadId;
        var scheduling = 1;
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scheduled = nt.Schedule(
            TimeSpan.Zero,
            () => completed.TrySetResult(
                Environment.CurrentManagedThreadId == schedulingThreadId &&
                Volatile.Read(ref scheduling) != 0));
        Volatile.Write(ref scheduling, 0);
        var ranInline = await completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that delayed work is scheduled asynchronously.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDue()
    {
        var nt = TaskPoolSequencer.Instance;
        var schedulingThreadId = Environment.CurrentManagedThreadId;
        var scheduling = 1;
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scheduled = nt.Schedule(
            ShortDueTime,
            () => completed.TrySetResult(
                Environment.CurrentManagedThreadId == schedulingThreadId &&
                Volatile.Read(ref scheduling) != 0));
        Volatile.Write(ref scheduling, 0);
        var ranInline = await completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that canceled delayed work does not run.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionCancel()
    {
        var nt = TaskPoolSequencer.Instance;
        var runCount = 0;
        var completed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var scheduled = nt.Schedule(CancelDueTime, () =>
        {
            Volatile.Write(ref runCount, 1);
            completed.TrySetResult(1);
        });
        scheduled.Dispose();
        var delay = Task.Delay(CancelObservationWindow);
        var observed = await Task.WhenAny(completed.Task, delay);
        await Assert.That(observed).IsSameReferenceAs(delay);
        await Assert.That(Volatile.Read(ref runCount)).IsEqualTo(0);
    }

    /// <summary>Verifies that delays larger than <see cref = "int.MaxValue"/> milliseconds are accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolDelayLargerThanIntMaxValue()
    {
        var dueTime = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);
        using var scheduled = TaskPoolSequencer.Instance.Schedule(dueTime, () => { });
        await Assert.That(scheduled).IsNotNull();
    }
}
