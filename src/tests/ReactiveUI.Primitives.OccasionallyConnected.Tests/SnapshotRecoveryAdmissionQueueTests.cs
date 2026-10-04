// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Transitions for bounded FIFO snapshot recovery admission.</summary>
public sealed class SnapshotRecoveryAdmissionQueueTests
{
    /// <summary>The finite bound for admission transition waits.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a canceled queued waiter is removed before the next grant.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task QueuedCancellationReleasesNextWaiterInFifoOrder()
    {
        var gate = new Lock();
        var queue = new SnapshotRecoveryAdmissionQueue(gate, 1);
        using var cancellation = new CancellationTokenSource();
        var first = Enqueue(queue, gate, CancellationToken.None);
        var canceled = Enqueue(queue, gate, cancellation.Token);
        var next = Enqueue(queue, gate, CancellationToken.None);

        await cancellation.CancelAsync();
        await Assert.That(canceled.Task.IsCanceled).IsTrue();
        await Assert.That(next.Task.IsCompleted).IsFalse();
        Complete(queue, gate, first);
        await next.Task.WaitAsync(GuardTimeout);
        await Assert.That(next.OwnsPermit).IsTrue();
        Complete(queue, gate, next);
    }

    /// <summary>Verifies cancellation before registration publication removes a queued waiter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationBeforeRegistrationPublicationRemovesQueuedWaiter()
    {
        var gate = new Lock();
        var queue = new SnapshotRecoveryAdmissionQueue(gate, 1);
        using var cancellation = new CancellationTokenSource();
        var first = Enqueue(queue, gate, CancellationToken.None);
        SnapshotRecoveryAdmissionQueue.Admission canceled;
        lock (gate)
        {
            canceled = queue.EnqueueLocked(cancellation.Token);
        }

        await cancellation.CancelAsync();
        canceled.PublishInitial();
        await Assert.That(canceled.Task.IsCanceled).IsTrue();
        Complete(queue, gate, first);
        var next = Enqueue(queue, gate, CancellationToken.None);
        await next.Task.WaitAsync(GuardTimeout);
        Complete(queue, gate, next);
    }

    /// <summary>Verifies a granted waiter keeps its permit when an in-flight callback arrives late.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GrantWinningCancellationKeepsPermitUntilCompletion()
    {
        var gate = new Lock();
        var queue = new SnapshotRecoveryAdmissionQueue(gate, 1);
        using var cancellation = new CancellationTokenSource();
        var first = Enqueue(queue, gate, CancellationToken.None);
        var granted = Enqueue(queue, gate, cancellation.Token);
        var next = Enqueue(queue, gate, CancellationToken.None);
        SnapshotRecoveryAdmissionQueue.CompletionActions actions;
        lock (gate)
        {
            actions = queue.CompleteLocked(first);
        }

        granted.TryCancel();
        actions.Publish();
        await granted.Task.WaitAsync(GuardTimeout);
        await Assert.That(granted.OwnsPermit).IsTrue();
        await Assert.That(next.Task.IsCompleted).IsFalse();
        Complete(queue, gate, granted);
        await next.Task.WaitAsync(GuardTimeout);
        Complete(queue, gate, next);
    }

    /// <summary>Verifies queued cleanup cancels the waiter and preserves the next FIFO grant.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task QueuedCompletionRemovesWaiterBeforeCancellationCallback()
    {
        var gate = new Lock();
        var queue = new SnapshotRecoveryAdmissionQueue(gate, 1);
        using var cancellation = new CancellationTokenSource();
        var first = Enqueue(queue, gate, CancellationToken.None);
        var removed = Enqueue(queue, gate, cancellation.Token);
        var next = Enqueue(queue, gate, CancellationToken.None);

        SnapshotRecoveryAdmissionQueue.CompletionActions actions;
        lock (gate)
        {
            actions = queue.CompleteLocked(removed);
        }

        removed.TryCancel();
        actions.Publish();
        await cancellation.CancelAsync();
        await Assert.That(removed.Task.IsCanceled).IsTrue();
        Complete(queue, gate, first);
        await next.Task.WaitAsync(GuardTimeout);
        Complete(queue, gate, next);
    }

    /// <summary>Verifies repeated completion does not release another permit or publish under the gate.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompletionPublishesOutsideGateAndDoubleCompleteDoesNotGrantAgain()
    {
        var gate = new Lock();
        var queue = new SnapshotRecoveryAdmissionQueue(gate, 1);
        var first = Enqueue(queue, gate, CancellationToken.None);
        var second = Enqueue(queue, gate, CancellationToken.None);
        var third = Enqueue(queue, gate, CancellationToken.None);
        SnapshotRecoveryAdmissionQueue.CompletionActions actions;
        bool publishedInsideGate;
        lock (gate)
        {
            actions = queue.CompleteLocked(first);
            publishedInsideGate = second.Task.IsCompleted;
        }

        await Assert.That(publishedInsideGate).IsFalse();
        actions.Publish();
        await second.Task.WaitAsync(GuardTimeout);
        Complete(queue, gate, first);
        await Assert.That(third.Task.IsCompleted).IsFalse();
        Complete(queue, gate, second);
        await third.Task.WaitAsync(GuardTimeout);
        Complete(queue, gate, third);
    }

    /// <summary>Queues one admission while holding the production gate, then publishes outside it.</summary>
    /// <param name="queue">The admission queue.</param>
    /// <param name="gate">The shared gate.</param>
    /// <param name="cancellationToken">The admission cancellation token.</param>
    /// <returns>The waiter.</returns>
    private static SnapshotRecoveryAdmissionQueue.Admission Enqueue(
        SnapshotRecoveryAdmissionQueue queue,
        Lock gate,
        CancellationToken cancellationToken)
    {
        SnapshotRecoveryAdmissionQueue.Admission admission;
        lock (gate)
        {
            admission = queue.EnqueueLocked(cancellationToken);
        }

        admission.PublishInitial();
        return admission;
    }

    /// <summary>Completes one admission under the production gate, then publishes outside it.</summary>
    /// <param name="queue">The admission queue.</param>
    /// <param name="gate">The shared gate.</param>
    /// <param name="admission">The waiter to complete.</param>
    private static void Complete(
        SnapshotRecoveryAdmissionQueue queue,
        Lock gate,
        SnapshotRecoveryAdmissionQueue.Admission admission)
    {
        SnapshotRecoveryAdmissionQueue.CompletionActions actions;
        lock (gate)
        {
            actions = queue.CompleteLocked(admission);
        }

        actions.Publish();
    }
}
