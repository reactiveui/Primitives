// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="BoundedSerializedStreamWorkLane"/>.</summary>
public sealed class BoundedSerializedStreamWorkLaneTests
{
    /// <summary>The work lane capacity used by tests.</summary>
    private const int Capacity = 2;

    /// <summary>The first work result.</summary>
    private const int FirstResult = 1;

    /// <summary>The second work result.</summary>
    private const int SecondResult = 2;

    /// <summary>The third work result.</summary>
    private const int ThirdResult = 3;

    /// <summary>The three-item capacity used by scheduler-drain tests.</summary>
    private const int ThreeItemCapacity = 3;

    /// <summary>The number of synchronous work items used to detect recursive continuation chaining.</summary>
    private const int SynchronousWorkItems = 128;

    /// <summary>Defines a guard timeout for deterministic scheduler assertions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies non-positive capacities are rejected.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task ConstructorRejectsNonPositiveCapacity() =>
        await Assert.That(static () => new BoundedSerializedStreamWorkLane(0)).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies the lane bounds admitted active plus queued work.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncRejectsWorkWhenLaneIsFull()
    {
        using var lane = new BoundedSerializedStreamWorkLane(1);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);

        await Assert.That(() => lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None)).ThrowsExactly<InvalidOperationException>();

        releaseFirst.SetResult();
        await Assert.That(await first).IsEqualTo(FirstResult);
    }

    /// <summary>Verifies disposal rejects later admission and can be repeated safely.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task DisposeRejectsLaterAdmissionAndIsIdempotent()
    {
        var lane = new BoundedSerializedStreamWorkLane(Capacity);

        lane.Dispose();
        lane.Dispose();

        await Assert.That(() => lane.EnqueueAsync(static _ => ValueTask.FromResult(FirstResult), CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => lane.WhenIdleAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposal cancels queued work while allowing active work to complete.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task DisposeCancelsQueuedWorkAndPreservesActiveWork()
    {
        var lane = new BoundedSerializedStreamWorkLane(Capacity);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        var second = lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None);

        await firstStarted.Task;
        lane.Dispose();
        releaseFirst.SetResult();

        await Assert.That(second).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(await first).IsEqualTo(FirstResult);
    }

    /// <summary>Verifies idle waits complete when active work drains and observe caller cancellation without canceling the work.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task WhenIdleAsyncWaitsForDrainAndHonorsCallerCancellation()
    {
        using var lane = new BoundedSerializedStreamWorkLane(Capacity);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource waitSource = new();

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        await firstStarted.Task;

        var idle = lane.WhenIdleAsync(CancellationToken.None);
        var canceledIdle = lane.WhenIdleAsync(waitSource.Token);
        await waitSource.CancelAsync();

        await Assert.That(canceledIdle).Throws<OperationCanceledException>();
        await Assert.That(idle.IsCompleted).IsFalse();

        releaseFirst.SetResult();
        await idle;
        await Assert.That(await first).IsEqualTo(FirstResult);
        await lane.WhenIdleAsync(CancellationToken.None);
    }

    /// <summary>Verifies delegate failures fault the admitted work item and release the lane.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncReportsDelegateFailureAndContinues()
    {
        using var lane = new BoundedSerializedStreamWorkLane(Capacity);

        var failed = lane.EnqueueAsync<int>(static _ => throw new InvalidOperationException("work failed"), CancellationToken.None);
        var next = lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None);

        await Assert.That(failed).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await next).IsEqualTo(SecondResult);
    }

    /// <summary>Verifies delegate cancellation completes the admitted work item as canceled.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncReportsDelegateCancellation()
    {
        using var lane = new BoundedSerializedStreamWorkLane(Capacity);
        using CancellationTokenSource source = new();

        var canceled = lane.EnqueueAsync<int>(static token => throw new OperationCanceledException(token), source.Token);

        await Assert.That(canceled).Throws<OperationCanceledException>();
        await lane.WhenIdleAsync(CancellationToken.None);
    }

    /// <summary>Verifies caller cancellation removes queued work before it is selected.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncCancelsQueuedWorkBeforeSelection()
    {
        var scheduler = new ControlledWorkLaneScheduler();
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, scheduler.Schedule);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource secondSource = new();
        var secondInvoked = false;

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        scheduler.RunOne();
        var second = lane.EnqueueAsync(
            token =>
            {
                secondInvoked = true;
                return ValueTask.FromResult(SecondResult);
            },
            secondSource.Token);

        await firstStarted.Task;
        await secondSource.CancelAsync();
        releaseFirst.SetResult();

        await Assert.That(second).Throws<OperationCanceledException>();
        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(secondInvoked).IsFalse();
    }

    /// <summary>Verifies queued cancellation releases capacity before active work drains.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncReleasesCapacityWhenQueuedWorkIsCanceled()
    {
        var scheduler = new ControlledWorkLaneScheduler();
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, scheduler.Schedule);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource secondSource = new();
        var secondInvoked = false;

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        scheduler.RunOne();
        var second = lane.EnqueueAsync(
            token =>
            {
                secondInvoked = true;
                return ValueTask.FromResult(SecondResult);
            },
            secondSource.Token);

        await firstStarted.Task.WaitAsync(GuardTimeout);
        await secondSource.CancelAsync();

        var third = lane.EnqueueAsync(static _ => ValueTask.FromResult(ThirdResult), CancellationToken.None);
        scheduler.ResetScheduled();
        releaseFirst.SetResult();
        await scheduler.Scheduled.Task.WaitAsync(GuardTimeout);
        scheduler.RunOne();

        await Assert.That(second).Throws<OperationCanceledException>();
        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(await third).IsEqualTo(ThirdResult);
        await Assert.That(secondInvoked).IsFalse();
    }

    /// <summary>Verifies a queued cancellation prevents delegate invocation when the item reaches execution.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncChecksCancellationBeforeInvokingDequeuedWork()
    {
        var scheduler = new ControlledWorkLaneScheduler();
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, scheduler.Schedule);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource secondSource = new();
        var secondInvoked = false;

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        scheduler.RunOne();
        var second = lane.EnqueueAsync(
            token =>
            {
                secondInvoked = true;
                return ValueTask.FromResult(SecondResult);
            },
            secondSource.Token);

        await firstStarted.Task;
        scheduler.ResetScheduled();
        releaseFirst.SetResult();
        await scheduler.Scheduled.Task.WaitAsync(GuardTimeout);
        await secondSource.CancelAsync();
        scheduler.RunOne();

        await Assert.That(second).Throws<OperationCanceledException>();
        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(secondInvoked).IsFalse();
    }

    /// <summary>Verifies initial scheduler rejection faults work and releases the lane.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncFaultsWorkAndReleasesLaneWhenInitialScheduleFails()
    {
        var scheduler = new ControlledWorkLaneScheduler { RejectNextSchedule = true };
        using var lane = new BoundedSerializedStreamWorkLane(1, scheduler.Schedule);

        var rejected = lane.EnqueueAsync(static _ => ValueTask.FromResult(FirstResult), CancellationToken.None);
        var accepted = lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None);
        scheduler.RunOne();

        await Assert.That(rejected).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await accepted).IsEqualTo(SecondResult);
    }

    /// <summary>Verifies scheduler rejection for dequeued work faults that item and continues draining.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncFaultsDequeuedWorkAndContinuesWhenSchedulerFails()
    {
        var scheduler = new ControlledWorkLaneScheduler();
        using var lane = new BoundedSerializedStreamWorkLane(ThreeItemCapacity, scheduler.Schedule);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        scheduler.RunOne();
        var rejected = lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None);
        var accepted = lane.EnqueueAsync(static _ => ValueTask.FromResult(ThirdResult), CancellationToken.None);

        await firstStarted.Task.WaitAsync(GuardTimeout);
        scheduler.RejectNextSchedule = true;
        scheduler.ResetScheduled();
        releaseFirst.SetResult();
        await scheduler.Scheduled.Task.WaitAsync(GuardTimeout);
        scheduler.RunOne();

        await Assert.That(rejected).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(await accepted).IsEqualTo(ThirdResult);
    }

    /// <summary>Verifies a scheduler exception after invoking work does not claim the started item.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncIgnoresSchedulerFailureAfterWorkStarts()
    {
        var scheduler = new ControlledWorkLaneScheduler { InvokeThenRejectNextSchedule = true };
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, scheduler.Schedule);
        TaskCompletionSource firstStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInvoked = false;

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                firstStarted.SetResult();
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        await firstStarted.Task.WaitAsync(GuardTimeout);

        var second = lane.EnqueueAsync(
            token =>
            {
                token.ThrowIfCancellationRequested();
                secondInvoked = true;
                return ValueTask.FromResult(SecondResult);
            },
            CancellationToken.None);

        await Assert.That(secondInvoked).IsFalse();
        scheduler.ResetScheduled();
        releaseFirst.SetResult();
        await scheduler.Scheduled.Task.WaitAsync(GuardTimeout);
        await Assert.That(secondInvoked).IsFalse();
        scheduler.RunOne();

        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(await second).IsEqualTo(SecondResult);
        await Assert.That(secondInvoked).IsTrue();
    }

    /// <summary>Verifies a late callback queued before scheduler rejection is inert.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncIgnoresCallbackQueuedBeforeSchedulerRejection()
    {
        var scheduler = new ControlledWorkLaneScheduler { QueueThenRejectNextSchedule = true };
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, scheduler.Schedule);
        var rejectedInvoked = false;
        var acceptedInvoked = false;

        var rejected = lane.EnqueueAsync(
            token =>
            {
                token.ThrowIfCancellationRequested();
                rejectedInvoked = true;
                return ValueTask.FromResult(FirstResult);
            },
            CancellationToken.None);
        var accepted = lane.EnqueueAsync(
            token =>
            {
                token.ThrowIfCancellationRequested();
                acceptedInvoked = true;
                return ValueTask.FromResult(SecondResult);
            },
            CancellationToken.None);

        scheduler.RunOne();
        await Assert.That(rejectedInvoked).IsFalse();
        await Assert.That(acceptedInvoked).IsFalse();
        scheduler.RunOne();

        await Assert.That(rejected).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await accepted).IsEqualTo(SecondResult);
        await Assert.That(rejectedInvoked).IsFalse();
        await Assert.That(acceptedInvoked).IsTrue();
    }

    /// <summary>Verifies synchronous schedulers preserve FIFO ordering for queued work.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncPreservesFifoOrderWithSynchronousScheduler()
    {
        using var lane = new BoundedSerializedStreamWorkLane(Capacity, static action => action());
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = new List<int>();

        var first = lane.EnqueueAsync(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                observed.Add(FirstResult);
                await releaseFirst.Task.ConfigureAwait(false);
                return FirstResult;
            },
            CancellationToken.None);
        var second = lane.EnqueueAsync(
            token =>
            {
                token.ThrowIfCancellationRequested();
                observed.Add(SecondResult);
                return ValueTask.FromResult(SecondResult);
            },
            CancellationToken.None);

        releaseFirst.SetResult();

        await Assert.That(await first).IsEqualTo(FirstResult);
        await Assert.That(await second).IsEqualTo(SecondResult);
        await Assert.That(observed).Count().IsEqualTo(Capacity);
        await Assert.That(observed[0]).IsEqualTo(FirstResult);
        await Assert.That(observed[1]).IsEqualTo(SecondResult);
    }

    /// <summary>Verifies task completion is published after capacity is released.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncReleasesCapacityBeforePublishingCompletion()
    {
        var lane = new BoundedSerializedStreamWorkLane(1);
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            var first = lane.EnqueueAsync(
                async token =>
                {
                    token.ThrowIfCancellationRequested();
                    await releaseFirst.Task.ConfigureAwait(false);
                    return FirstResult;
                },
                CancellationToken.None);
            var second = EnqueueAfterCompletionAsync(lane, first);

            releaseFirst.SetResult();

            await Assert.That(await first).IsEqualTo(FirstResult);
            await Assert.That(await second).IsEqualTo(SecondResult);
        }
        finally
        {
            lane.Dispose();
        }
    }

    /// <summary>Verifies synchronous work completion does not recursively run the next item on the completing stack.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task EnqueueAsyncTrampolinesSynchronousCompletions()
    {
        using var lane = new BoundedSerializedStreamWorkLane(SynchronousWorkItems);
        var tasks = new Task<int>[SynchronousWorkItems];
        for (var i = 0; i < tasks.Length; i++)
        {
            var value = i;
            tasks[i] = lane.EnqueueAsync(_ => ValueTask.FromResult(value), CancellationToken.None);
        }

        var results = await Task.WhenAll(tasks);

        await Assert.That(results.Length).IsEqualTo(SynchronousWorkItems);
        await Assert.That(results[0]).IsEqualTo(0);
        await Assert.That(results[^1]).IsEqualTo(SynchronousWorkItems - 1);
    }

    /// <summary>Enqueues follow-up work after a prior task completion resumes.</summary>
    /// <param name="lane">The work lane.</param>
    /// <param name="previous">The previous task.</param>
    /// <returns>The follow-up work result.</returns>
    private static async Task<int> EnqueueAfterCompletionAsync(
        BoundedSerializedStreamWorkLane lane,
        Task<int> previous)
    {
        _ = await previous.ConfigureAwait(false);
        return await lane.EnqueueAsync(static _ => ValueTask.FromResult(SecondResult), CancellationToken.None)
            .ConfigureAwait(false);
    }

    /// <summary>Schedules lane work under test control.</summary>
    private sealed class ControlledWorkLaneScheduler
    {
        /// <summary>Stores scheduled work.</summary>
        private readonly Queue<Action> _actions = new();

        /// <summary>Gets the signal completed when work is scheduled.</summary>
        internal TaskCompletionSource Scheduled { get; private set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets or sets a value indicating whether the next schedule call is rejected.</summary>
        internal bool RejectNextSchedule { get; set; }

        /// <summary>Gets or sets a value indicating whether the next schedule call invokes work before rejection.</summary>
        internal bool InvokeThenRejectNextSchedule { get; set; }

        /// <summary>Gets or sets a value indicating whether the next schedule call queues work before rejection.</summary>
        internal bool QueueThenRejectNextSchedule { get; set; }

        /// <summary>Schedules one action.</summary>
        /// <param name="action">The action to schedule.</param>
        /// <exception cref="InvalidOperationException">The next schedule call is configured to be rejected.</exception>
        internal void Schedule(Action action)
        {
            if (InvokeThenRejectNextSchedule)
            {
                InvokeThenRejectNextSchedule = false;
                action();
                throw new InvalidOperationException("schedule failed after invocation");
            }

            if (QueueThenRejectNextSchedule)
            {
                QueueThenRejectNextSchedule = false;
                _actions.Enqueue(action);
                _ = Scheduled.TrySetResult();
                throw new InvalidOperationException("schedule failed after queueing");
            }

            if (RejectNextSchedule)
            {
                RejectNextSchedule = false;
                throw new InvalidOperationException("schedule failed");
            }

            _actions.Enqueue(action);
            _ = Scheduled.TrySetResult();
        }

        /// <summary>Resets the scheduled signal.</summary>
        internal void ResetScheduled() => Scheduled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Runs the next scheduled action.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RunOne() => _actions.Dequeue()();
    }
}
