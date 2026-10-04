// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
/// <content>Local dead-letter reconciliation tests.</content>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The accepted server version used by dead-letter follow-up result assertions.</summary>
    private const string DeadLetterAcceptedServerVersion = "server-dead-letter";

    /// <summary>Verifies a local dead-letter rebuild removes only the poisoned edit from a mixed lease.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncRebuildsReplacementStateAndKeepsOtherLeaseMembers()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        var subscription = await InitializeResultStoreAsync(store);
        var options = CreateResultOptions(store, subscription, new ReplacementProjection());
        var committer = CreateLocalCommitter(options);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var second = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var lease = await LeaseResultBatchAsync(store, PendingReplacementCount);

        var state = await committer.DeadLetterOperationAsync(
            lease.LeaseId,
            second.Operation.OperationId,
            DeadLetterReason,
            CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.Operation.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.Operation.OperationId);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(second.Operation.OperationId);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(DeadLetterReason);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(0);

        var accepted = new RemoteSyncResult(lease.LeaseId, [new(first.Operation.OperationId, OperationResultKind.Accepted, null, DeadLetterAcceptedServerVersion)], null, null);
        var snapshots = await store.ApplySyncResultAsync(lease.LeaseId, accepted, [], CancellationToken.None);
        await Assert.That(snapshots.Count).IsEqualTo(0);
        var final = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(final.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(final.ReplayOperations.Count).IsEqualTo(1);
    }

    /// <summary>Verifies malformed dead-letter receipts poison the committer before exposing rebuilt state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncMalformedReceiptPoisonsCommitter()
    {
        var store = new ScriptedLocalStore { DeadLetterSnapshotRevisionOffset = 1 };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.DeadLetterOperationAsync(Guid.NewGuid(), first.Operation.OperationId, DeadLetterReason, CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.DeadLetterApplyCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies dead-letter reconciliation requires an authoritative checkpoint before store mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncWithoutAuthoritativeCheckpointFailsBeforeStoreMutation()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.DeadLetterOperationAsync(Guid.NewGuid(), OperationId.New(), DeadLetterReason, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("authoritative checkpoint");
        await Assert.That(store.DeadLetterApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation after the store dead-letter commit returns the durable receipt.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeadLetterOperationAsyncCancellationAfterStoreCommitReturnsReceipt()
    {
        using CancellationTokenSource source = new();
        var store = new ScriptedLocalStore { CancelAfterSuccessfulDeadLetterApply = source };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);

        var state = await committer.DeadLetterOperationAsync(Guid.NewGuid(), first.Operation.OperationId, DeadLetterReason, source.Token);

        await Assert.That(source.IsCancellationRequested).IsTrue();
        await Assert.That(state.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.Revision).IsEqualTo(first.State.Revision + 1);
    }
}
