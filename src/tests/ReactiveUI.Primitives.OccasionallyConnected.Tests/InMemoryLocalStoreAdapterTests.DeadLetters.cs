// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
/// <content>Dead-letter reconciliation tests.</content>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The initial authoritative payload used by dead-letter tests.</summary>
    private const string DeadLetterAuthoritativeInitialText = "authoritative-initial";

    /// <summary>The initial optimistic payload used by dead-letter tests.</summary>
    private const string DeadLetterOptimisticInitialText = "optimistic-initial";

    /// <summary>The second optimistic payload used by dead-letter tests.</summary>
    private const string DeadLetterOptimisticLocalText = "optimistic-local";

    /// <summary>The stable local reason code used by dead-letter tests.</summary>
    private const string DeadLetterReasonCode = "OC.LocalPoison";

    /// <summary>The first oversized reason length rejected by the in-memory store.</summary>
    private const int OversizedDeadLetterReasonLength = 1025;

    /// <summary>Verifies a dead-letter transition keeps the surviving lease member and durable recovery data.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterCommits_ThenSurvivorLeaseAndDeadLetterRecover()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence) with { Payload = CreatePayload("first-operation") };
        var second = CreateOperation(SecondClientSequence) with { Payload = CreatePayload("second-operation") };
        _ = await store.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, DeadLetterOptimisticInitialText) with { AuthoritativeState = CreatePayload(DeadLetterAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, DeadLetterOptimisticLocalText), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, ExpectedLeasedOperationCount, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        _ = await store.DeadLetterOperationAsync(
            lease.LeaseId,
            second.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(SecondClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(second.OperationId);
        await Assert.That(recovered.DeadLetters[0].Operation.Metadata["origin"]).IsEqualTo("unit-test");
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(DeadLetterReasonCode);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(0);
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(first.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
    }

    /// <summary>Verifies malformed reason codes are rejected before the leased operation is mutated.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterReasonIsMalformed_ThenOperationStaysLeased()
    {
        await using var store = await CreateInitializedStoreAsync();
        var (operation, lease) = await CreateDeadLetterTargetAsync(store, includeAuthoritative: true);
        var oversizedReason = new string('x', OversizedDeadLetterReasonLength);
        Func<Task> whitespace = () => store.DeadLetterOperationAsync(
            lease.LeaseId,
            operation.OperationId,
            " ",
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();
        Func<Task> oversized = () => store.DeadLetterOperationAsync(
            lease.LeaseId,
            operation.OperationId,
            oversizedReason,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();
        Func<Task> malformed = () => store.DeadLetterOperationAsync(
            lease.LeaseId,
            operation.OperationId,
            "\ud800",
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(whitespace).ThrowsExactly<ArgumentException>();
        await Assert.That(oversized).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(malformed).ThrowsExactly<ArgumentException>();
        await AcceptLeasedOperationAsync(store, lease, operation);
    }

    /// <summary>Verifies snapshot checkpoint fences are rejected without mutating the lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterSnapshotFenceFails_ThenOperationStaysLeased()
    {
        await using var staleStore = await CreateInitializedStoreAsync();
        var stale = await CreateDeadLetterTargetAsync(staleStore, includeAuthoritative: true);
        Func<Task> staleRevision = () => staleStore.DeadLetterOperationAsync(
            stale.Lease.LeaseId,
            stale.Operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(expectedRevision: 0, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(staleRevision).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(staleStore, stale.Lease, stale.Operation);

        await using var replacingStore = await CreateInitializedStoreAsync();
        var replacing = await CreateDeadLetterTargetAsync(replacingStore, includeAuthoritative: true);
        Func<Task> replaceAuthoritative = () => replacingStore.DeadLetterOperationAsync(
            replacing.Lease.LeaseId,
            replacing.Operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText) with { AuthoritativeState = CreatePayload("changed-authoritative") },
            CancellationToken.None).AsTask();

        await Assert.That(replaceAuthoritative).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(replacingStore, replacing.Lease, replacing.Operation);

        await using var missingStore = await CreateInitializedStoreAsync();
        var missing = await CreateDeadLetterTargetAsync(missingStore, includeAuthoritative: false);
        Func<Task> missingAuthoritative = () => missingStore.DeadLetterOperationAsync(
            missing.Lease.LeaseId,
            missing.Operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(missingAuthoritative).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(missingStore, missing.Lease, missing.Operation);
    }

    /// <summary>Verifies mismatched lease, inclusion, state, and stream checks reject without mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterTargetDoesNotMatch_ThenOperationStaysLeased()
    {
        await using var wrongLeaseStore = await CreateInitializedStoreAsync();
        var wrongLeaseSubscription = await wrongLeaseStore.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence) with { Payload = CreatePayload("wrong-lease-first") };
        var second = CreateOperation(SecondClientSequence) with { Payload = CreatePayload("wrong-lease-second") };
        _ = await wrongLeaseStore.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, DeadLetterOptimisticInitialText) with { AuthoritativeState = CreatePayload(DeadLetterAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await wrongLeaseStore.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, DeadLetterOptimisticLocalText), CancellationToken.None);
        var wrongLease = RequireBatch(await LeaseSingleBatchAsync(wrongLeaseStore, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        Func<Task> unowned = () => wrongLeaseStore.DeadLetterOperationAsync(
            wrongLease.LeaseId,
            second.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(SecondClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(unowned).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(wrongLeaseStore, wrongLease, first);
        var wrongLeaseRecovery = await wrongLeaseStore.RecoverStreamAsync(Stream, wrongLeaseSubscription, CancellationToken.None);
        await Assert.That(wrongLeaseRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(wrongLeaseRecovery.PendingOperations[0].OperationId).IsEqualTo(second.OperationId);

        await using var mismatchedStreamStore = await CreateInitializedStoreAsync();
        var mismatched = await CreateDeadLetterTargetAsync(mismatchedStreamStore, includeAuthoritative: true);
        Func<Task> mismatchedStream = () => mismatchedStreamStore.DeadLetterOperationAsync(
            mismatched.Lease.LeaseId,
            mismatched.Operation.OperationId,
            DeadLetterReasonCode,
            new(OtherStream, CreatePayload(DeadLetterOptimisticInitialText), FormatVersion: 1, ExpectedRevision: FirstClientSequence),
            CancellationToken.None).AsTask();

        await Assert.That(mismatchedStream).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(mismatchedStreamStore, mismatched.Lease, mismatched.Operation);
    }

    /// <summary>Verifies a terminal stale lease member cannot be moved to the dead-letter list.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationIsTerminal_ThenStateIsPreserved()
    {
        await using var store = await CreateInitializedStoreAsync();
        var (operation, lease) = await CreateDeadLetterTargetAsync(
            store,
            includeAuthoritative: true,
            OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce });
        var attempt = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
        Func<Task> terminal = () => store.DeadLetterOperationAsync(
            lease.LeaseId,
            operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(terminal).ThrowsExactly<InvalidOperationException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
    }

    /// <summary>Verifies prior send-attempt evidence survives release, re-lease, and dead-letter rejection.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee that may have produced a remote effect.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtLeastOnce)]
    [Arguments(DeliveryGuarantee.ExactlyOnce)]
    public async Task WhenAttemptedOperationIsReLeasedForDeadLetter_ThenStateIsPreserved(DeliveryGuarantee deliveryGuarantee)
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var (operation, lease) = await CreateDeadLetterTargetAsync(
            store,
            includeAuthoritative: true,
            OperationPolicy.Default with { DeliveryGuarantee = deliveryGuarantee });
        var attempt = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, FirstClientSequence, CancellationToken.None);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var nextLease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        Func<Task> attempted = () => store.DeadLetterOperationAsync(
            nextLease.LeaseId,
            operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticLocalText),
            CancellationToken.None).AsTask();

        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(attempted).ThrowsExactly<InvalidOperationException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(status?.State is SyncOperationState.QueuedForUpload or SyncOperationState.Uploading).IsTrue();
        await Assert.That(status?.Attempt).IsEqualTo(FirstClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a receive-before-ACK inclusion cannot be moved to the dead-letter list.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterOperationIsAlreadyIncluded_ThenStateIsPreserved()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var included = await CreateDeadLetterTargetAsync(store, includeAuthoritative: true);
        var remoteEvent = CreateOriginEvent(RemoteCursor, included.Operation.OperationId, ClientId);
        var remoteBatch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, included.Operation.OperationId), [remoteEvent.EventId])],
        };
        _ = await store.ApplyRemoteBatchAsync(
            remoteBatch,
            CreateSnapshotMutation(FirstClientSequence, AuthoritativePayloadText) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        Func<Task> includedTransition = () => store.DeadLetterOperationAsync(
            included.Lease.LeaseId,
            included.Operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(SecondClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None).AsTask();

        await Assert.That(includedTransition).ThrowsExactly<InvalidOperationException>();
        await AcceptLeasedOperationAsync(store, included.Lease, included.Operation);
    }

    /// <summary>Verifies dead-lettering the only leased operation releases that lease.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterSingleOperationLeaseCommits_ThenLeaseIsReleased()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var (operation, lease) = await CreateDeadLetterTargetAsync(store, includeAuthoritative: true);

        _ = await store.DeadLetterOperationAsync(
            lease.LeaseId,
            operation.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(FirstClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None);
        Func<Task> reuseLease = () => store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None).AsTask();

        await Assert.That(reuseLease).ThrowsExactly<InvalidOperationException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies recovery returns multiple dead-lettered operations in client sequence order.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenMultipleDeadLettersRecover_ThenRecordsAreOrderedByClientSequence()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateOperation(FirstClientSequence) with { Payload = CreatePayload("ordered-first") };
        var second = CreateOperation(SecondClientSequence) with { Payload = CreatePayload("ordered-second") };
        _ = await store.CommitLocalOperationAsync(
            first,
            CreateSnapshotMutation(0, DeadLetterOptimisticInitialText) with { AuthoritativeState = CreatePayload(DeadLetterAuthoritativeInitialText) },
            CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, DeadLetterOptimisticLocalText), CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, ExpectedLeasedOperationCount, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        _ = await store.DeadLetterOperationAsync(
            lease.LeaseId,
            second.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(SecondClientSequence, "after-second-dead-letter"),
            CancellationToken.None);
        _ = await store.DeadLetterOperationAsync(
            lease.LeaseId,
            first.OperationId,
            DeadLetterReasonCode,
            CreateSnapshotMutation(ThirdClientSequence, DeadLetterOptimisticInitialText),
            CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(ExpectedLeasedOperationCount);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.DeadLetters[1].Operation.OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Creates one leased dead-letter target.</summary>
    /// <param name="store">The store.</param>
    /// <param name="includeAuthoritative">Whether the committed snapshot includes an authoritative checkpoint.</param>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The operation and lease.</returns>
    private static async Task<(SyncOperation Operation, LeasedOperationBatch Lease)> CreateDeadLetterTargetAsync(
        InMemoryLocalStoreAdapter store,
        bool includeAuthoritative,
        OperationPolicy? policy = null)
    {
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence, policy: policy) with { Payload = CreatePayload("dead-letter-target") };
        var mutation = CreateSnapshotMutation(0, DeadLetterOptimisticInitialText);
        if (includeAuthoritative)
        {
            mutation = mutation with { AuthoritativeState = CreatePayload(DeadLetterAuthoritativeInitialText) };
        }

        _ = await store.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        return (operation, lease);
    }

    /// <summary>Accepts a leased operation.</summary>
    /// <param name="store">The store.</param>
    /// <param name="lease">The lease.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The asynchronous acceptance.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task AcceptLeasedOperationAsync(
        InMemoryLocalStoreAdapter store,
        LeasedOperationBatch lease,
        SyncOperation operation) =>
        store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None).AsTask();
}
