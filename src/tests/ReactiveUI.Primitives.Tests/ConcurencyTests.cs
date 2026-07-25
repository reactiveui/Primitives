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
        var disposable = scheduler.Schedule(0, static (_, _) => EmptyDisposable.Instance);
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
        var probe = new InlineExecutionProbe();
        using var scheduled = nt.Schedule(probe, static p => p.RecordExecution());
        probe.MarkSchedulingFinished();
        var ranInline = await probe.Completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that work due immediately is scheduled asynchronously.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDueNow()
    {
        var nt = TaskPoolSequencer.Instance;
        var probe = new InlineExecutionProbe();
        using var scheduled = nt.Schedule(probe, TimeSpan.Zero, static p => p.RecordExecution());
        probe.MarkSchedulingFinished();
        var ranInline = await probe.Completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that delayed work is scheduled asynchronously.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionDue()
    {
        var nt = TaskPoolSequencer.Instance;
        var probe = new InlineExecutionProbe();
        using var scheduled = nt.Schedule(probe, ShortDueTime, static p => p.RecordExecution());
        probe.MarkSchedulingFinished();
        var ranInline = await probe.Completed.Task.WaitAsync(ScheduleTimeout);
        await Assert.That(ranInline).IsFalse();
    }

    /// <summary>Verifies that canceled delayed work does not run.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolScheduleActionCancel()
    {
        var nt = TaskPoolSequencer.Instance;
        var probe = new CancellationProbe();
        var scheduled = nt.Schedule(probe, CancelDueTime, static p => p.RecordExecution());
        scheduled.Dispose();
        var delay = Task.Delay(CancelObservationWindow);
        var observed = await Task.WhenAny(probe.Completed.Task, delay);
        await Assert.That(observed).IsSameReferenceAs(delay);
        await Assert.That(probe.RunCount).IsEqualTo(0);
    }

    /// <summary>Verifies that delays larger than <see cref = "int.MaxValue"/> milliseconds are accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskPoolDelayLargerThanIntMaxValue()
    {
        var dueTime = TimeSpan.FromMilliseconds((double)int.MaxValue + 1);
        using var scheduled = TaskPoolSequencer.Instance.Schedule(dueTime, static () => { });
        await Assert.That(scheduled).IsNotNull();
    }

    /// <summary>Records whether a scheduled callback ran on the scheduling thread before scheduling returned.</summary>
    private sealed class InlineExecutionProbe
    {
        /// <summary>The thread the scheduling call was made on.</summary>
        private readonly int _schedulingThreadId = Environment.CurrentManagedThreadId;

        /// <summary>Non-zero while the scheduling call has not yet returned.</summary>
        private int _scheduling = 1;

        /// <summary>Gets the source completed with whether the callback observed an inline execution.</summary>
        public TaskCompletionSource<bool> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Signals that the scheduling call has returned, so any later callback cannot have run inline.</summary>
        public void MarkSchedulingFinished() => Volatile.Write(ref _scheduling, 0);

        /// <summary>Completes <see cref = "Completed"/> with whether this callback ran inline on the scheduling thread.</summary>
        public void RecordExecution() => _ = Completed.TrySetResult(
            Environment.CurrentManagedThreadId == _schedulingThreadId &&
            Volatile.Read(ref _scheduling) != 0);
    }

    /// <summary>Records whether a scheduled callback ran at all, so a cancellation can be shown to have suppressed it.</summary>
    private sealed class CancellationProbe
    {
        /// <summary>How many times the scheduled callback has run.</summary>
        private int _runCount;

        /// <summary>Gets the source completed when the scheduled callback runs.</summary>
        public TaskCompletionSource<int> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of times the scheduled callback ran.</summary>
        public int RunCount => Volatile.Read(ref _runCount);

        /// <summary>Records that the scheduled callback ran.</summary>
        public void RecordExecution()
        {
            Volatile.Write(ref _runCount, 1);
            _ = Completed.TrySetResult(1);
        }
    }
}
