// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests queue accounting during upload reconciliation.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The queue accounting rejection message.</summary>
    private const string QueueUnderflowMessage = "underflow";

    /// <summary>Verifies a repeated terminal result cannot consume already released queue capacity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsQueueCountUnderflowBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = new SyncBatch(Guid.NewGuid(), [committed.Operation]);
        var result = new RemoteSyncResult(batch.BatchId, [new(committed.Operation.OperationId, OperationResultKind.Accepted, null, null)], null, null);
        _ = await committer.ApplySyncResultAsync(batch, result, CancellationToken.None);
        var queue = committer.RecoveredQueueSnapshot;
        var state = committer.Current;

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(batch, result, CancellationToken.None).AsTask());

        await Assert.That(failure?.Message).Contains(QueueUnderflowMessage);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(1);
        await Assert.That(committer.RecoveredQueueSnapshot).IsEqualTo(queue);
        await Assert.That(queue.PendingOperations).IsEqualTo(0);
        await Assert.That(queue.PendingBytes).IsEqualTo(0);
        await Assert.That(committer.Current).IsEqualTo(state);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
    }

    /// <summary>Verifies inconsistent upload bytes cannot make retained queue bytes negative.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplySyncResultAsyncRejectsQueueByteUnderflowBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var changed = committed.Operation with { Metadata = new Dictionary<string, string> { ["unexpected"] = "additional retained bytes" } };
        var batch = new SyncBatch(Guid.NewGuid(), [changed]);
        var result = new RemoteSyncResult(batch.BatchId, [new(changed.OperationId, OperationResultKind.Accepted, null, null)], null, null);
        var queue = committer.RecoveredQueueSnapshot;
        var state = committer.Current;

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(batch, result, CancellationToken.None).AsTask());

        await Assert.That(failure?.Message).Contains(QueueUnderflowMessage);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.RecoveredQueueSnapshot).IsEqualTo(queue);
        await Assert.That(committer.Current).IsEqualTo(state);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.Recovery.PendingOperations[0]).IsSameReferenceAs(committed.Operation);
    }

    /// <summary>Verifies a missing dead-letter identity cannot remove another retained operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncRejectsMissingReplayIdentityBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var queue = committer.RecoveredQueueSnapshot;
        var state = committer.Current;

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.DeadLetterOperationAsync(Guid.NewGuid(), OperationId.New(), DeadLetterReason, CancellationToken.None).AsTask());

        await Assert.That(failure?.Message).Contains("not present in recovered replay state");
        await Assert.That(store.DeadLetterApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.RecoveredQueueSnapshot).IsEqualTo(queue);
        await Assert.That(committer.Current).IsEqualTo(state);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.Recovery.PendingOperations[0]).IsSameReferenceAs(committed.Operation);
    }

    /// <summary>Verifies dead-lettering already acknowledged work cannot release queue capacity twice.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncRejectsQueueCountUnderflowBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var batch = new SyncBatch(Guid.NewGuid(), [committed.Operation]);
        var result = new RemoteSyncResult(batch.BatchId, [new(committed.Operation.OperationId, OperationResultKind.Accepted, null, null)], null, null);
        _ = await committer.ApplySyncResultAsync(batch, result, CancellationToken.None);
        var queue = committer.RecoveredQueueSnapshot;
        var state = committer.Current;

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.DeadLetterOperationAsync(batch.BatchId, committed.Operation.OperationId, DeadLetterReason, CancellationToken.None).AsTask());

        await Assert.That(failure?.Message).Contains(QueueUnderflowMessage);
        await Assert.That(store.DeadLetterApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.RecoveredQueueSnapshot).IsEqualTo(queue);
        await Assert.That(committer.Current).IsEqualTo(state);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
    }

    /// <summary>Verifies corrupt replay metadata cannot make retained queue bytes negative.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncRejectsQueueByteUnderflowBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var changed = committed.Operation with { Metadata = new Dictionary<string, string> { ["unexpected"] = "additional retained bytes" } };
        store.Recovery = store.Recovery with { ReplayOperations = [changed] };
        var queue = committer.RecoveredQueueSnapshot;
        var state = committer.Current;

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.DeadLetterOperationAsync(Guid.NewGuid(), committed.Operation.OperationId, DeadLetterReason, CancellationToken.None).AsTask());

        await Assert.That(failure?.Message).Contains(QueueUnderflowMessage);
        await Assert.That(store.DeadLetterApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.RecoveredQueueSnapshot).IsEqualTo(queue);
        await Assert.That(committer.Current).IsEqualTo(state);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.Recovery.PendingOperations[0]).IsSameReferenceAs(committed.Operation);
    }
}
