// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests replay-only operation roles for <see cref="SnapshotRecoveryValidator"/>.</summary>
public sealed partial class SnapshotRecoveryValidatorTests
{
    /// <summary>The payload size used to prove union logical byte accounting.</summary>
    private const int UnionBudgetPayloadBytes = 600;

    /// <summary>The logical byte budget that admits one role but not both roles.</summary>
    private const int SingleRoleLogicalByteBudget = 1000;

    /// <summary>Verifies request validation applies caller limits to the pending and replay union.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsPendingAndReplayUnionAboveCallerLimit()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var limits = new SnapshotRecoveryLimits { MaximumPendingOperations = FirstSequence };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies logical byte accounting counts pending and replay roles together.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsPendingAndReplayUnionAboveLogicalByteLimit()
    {
        var pending = CreateOperation(FirstSequence) with
        {
            Payload = CreatePayload(OperationContractId, OperationSchemaVersion, UnionBudgetPayloadBytes),
        };
        var replay = CreateOperation(SecondSequence) with
        {
            Payload = CreatePayload(OperationContractId, OperationSchemaVersion, UnionBudgetPayloadBytes),
        };
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SingleRoleLogicalByteBudget, MaximumPayloadBytes = UnionBudgetPayloadBytes };
        var pendingOnly = CreateRequest([pending]) with { MaximumResponseBytes = SingleRoleLogicalByteBudget };
        var replayOnly = CreateRequest([], replay: [replay]) with { MaximumResponseBytes = SingleRoleLogicalByteBudget };
        var combined = CreateRequest([pending], replay: [replay]) with { MaximumResponseBytes = SingleRoleLogicalByteBudget };

        SnapshotRecoveryValidator.Validate(pendingOnly, limits);
        SnapshotRecoveryValidator.Validate(replayOnly, limits);
        await Assert.That(() => SnapshotRecoveryValidator.Validate(combined, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies request logical byte accounting charges both role collection counts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateCountsReplayCollectionHeaderAtExactLogicalBoundary()
    {
        var replay = CreateOperation(FirstSequence);
        var exactLogicalBytes = GetSingleReplayRequestLogicalBytes();
        var passing = CreateRequest([], replay: [replay]) with { MaximumResponseBytes = exactLogicalBytes };
        var passingLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = exactLogicalBytes, MaximumPayloadBytes = PayloadByteLength };
        var failing = passing with { MaximumResponseBytes = exactLogicalBytes - FirstSequence };
        var failingLimits = passingLimits with { MaximumLogicalBytes = exactLogicalBytes - FirstSequence };

        SnapshotRecoveryValidator.Validate(passing, passingLimits);
        await Assert.That(() => SnapshotRecoveryValidator.Validate(failing, failingLimits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies request validation rejects duplicate operation ids across pending and replay roles.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsDuplicateOperationAcrossPendingAndReplay()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence) with { OperationId = pending.OperationId };
        var request = CreateRequest([pending], replay: [replay]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies request validation rejects duplicate client sequences across pending and replay roles.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsDuplicateSequenceAcrossPendingAndReplay()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(FirstSequence);
        var request = CreateRequest([pending], replay: [replay]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies request validation rejects replay operations from another stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsForeignReplayOperation()
    {
        var replay = CreateOperation(FirstSequence) with { StreamId = ForeignStream };
        var request = CreateRequest([], replay: [replay]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies remote recovered results bind to the exact union without requiring wire order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsRecoveredResultWithUnorderedPendingAndReplayDispositions()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Included(replay.OperationId, OperationResultKind.Accepted),
                Unknown(pending.OperationId),
            ]);

        SnapshotRecoveryValidator.Validate(request, result, new());

        await Assert.That(result.OperationDispositions[0].OperationId).IsEqualTo(replay.OperationId);
    }

    /// <summary>Verifies pending unknown remains valid when replay-only work is proven accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsPendingUnknownWithReplayAcceptedProof()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Unknown(pending.OperationId),
                Included(replay.OperationId, OperationResultKind.Accepted),
            ]);

        SnapshotRecoveryValidator.Validate(request, result, new());

        await Assert.That(result.OperationDispositions).Count().IsEqualTo(SecondSequence);
    }

    /// <summary>Verifies replay-only unknown dispositions fail closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsReplayUnknownDisposition()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Unknown(pending.OperationId),
                Unknown(replay.OperationId),
            ]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies replay-only rejected or conflict proof fails closed for already accepted local work.</summary>
    /// <param name="proof">The contradictory proof selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("rejected")]
    [Arguments("conflict")]
    public async Task ValidateRejectsContradictoryReplayProof(string proof)
    {
        var replay = CreateOperation(FirstSequence);
        var request = CreateRequest([], replay: [replay]);
        var disposition = proof == "rejected"
            ? Rejected(replay.OperationId)
            : Included(replay.OperationId, OperationResultKind.Conflict);
        var result = CreateRecoveredResult(request, [disposition]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies recovered pending count is checked before recovered operation scans.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected count exception was not observed.</exception>
    [Test]
    public async Task ValidateRejectsOversizedRecoveredPendingBeforeScanningOperations()
    {
        var pending = CreateOperation(FirstSequence);
        var request = CreateRequest([pending]);
        var result = CreateRecoveredResult(request, [Unknown(pending.OperationId)]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending, NullReference<SyncOperation>()], Stream, replay: []);
        var limits = new SnapshotRecoveryLimits { MaximumPendingOperations = FirstSequence };

        var exception = await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, limits))
            .ThrowsExactly<ArgumentException>() ?? throw new InvalidOperationException("No recovered count exception.");
        await Assert.That(exception.ParamName).IsEqualTo("recovered");
        await Assert.That(exception.Message).Contains("pending operation limit");
    }

    /// <summary>Verifies recovered replay count is checked before recovered operation scans.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected count exception was not observed.</exception>
    [Test]
    public async Task ValidateRejectsOversizedRecoveredReplayBeforeScanningOperations()
    {
        var pending = CreateOperation(FirstSequence);
        var request = CreateRequest([pending]);
        var result = CreateRecoveredResult(request, [Unknown(pending.OperationId)]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending], Stream, replay: [pending, NullReference<SyncOperation>()]);
        var limits = new SnapshotRecoveryLimits { MaximumPendingOperations = FirstSequence };

        var exception = await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, limits))
            .ThrowsExactly<ArgumentException>() ?? throw new InvalidOperationException("No recovered count exception.");
        await Assert.That(exception.ParamName).IsEqualTo("recovered");
        await Assert.That(exception.Message).Contains("pending operation limit");
    }

    /// <summary>Verifies local mutation validation accepts unordered exact pending and replay disposition sets.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsLocalMutationWithUnorderedPendingAndReplayDispositions()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Included(replay.OperationId, OperationResultKind.Accepted),
                Unknown(pending.OperationId),
            ]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending], Stream, replay: [pending, replay]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);

        SnapshotRecoveryValidator.Validate(mutation, recovered, new());

        await Assert.That(mutation.OperationDispositions[0].OperationId).IsEqualTo(replay.OperationId);
    }

    /// <summary>Verifies local mutation validation rejects replay-only unknown dispositions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLocalReplayUnknownDisposition()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Unknown(pending.OperationId),
                Unknown(replay.OperationId),
            ]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending], Stream, replay: [pending, replay]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutation validation ignores pending duplicates in replay-visible state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsLocalMutationWhenRecoveredReplayIncludesPendingOperation()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(
            request,
            [
                Unknown(pending.OperationId),
                Included(replay.OperationId, OperationResultKind.Accepted),
            ]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending], Stream, replay: [pending, replay]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);

        SnapshotRecoveryValidator.Validate(mutation, recovered, new());

        await Assert.That(mutation.OperationDispositions[1].OperationId).IsEqualTo(replay.OperationId);
    }

    /// <summary>Verifies local mutation validation rejects duplicate recovered pending operation ids.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLocalRecoveredPendingDuplicateOperationIds()
    {
        var pending = CreateOperation(FirstSequence);
        var duplicatePending = CreateOperation(SecondSequence) with { OperationId = pending.OperationId };
        var request = CreateRequest([pending]);
        var result = CreateRecoveredResult(request, [Unknown(pending.OperationId)]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending, duplicatePending], Stream, replay: []);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutation validation rejects duplicate recovered replay-only operation ids.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLocalRecoveredReplayOnlyDuplicateOperationIds()
    {
        var pending = CreateOperation(FirstSequence);
        var replay = CreateOperation(SecondSequence);
        var duplicateReplay = CreateOperation(SecondSequence + FirstSequence) with { OperationId = replay.OperationId };
        var request = CreateRequest([pending], replay: [replay]);
        var result = CreateRecoveredResult(request, [Unknown(pending.OperationId), Included(replay.OperationId, OperationResultKind.Accepted)]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [pending], Stream, replay: [pending, replay, duplicateReplay]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies replay-only included dispositions require an accepted result proof.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsReplayOnlyIncludedDispositionWithoutResult()
    {
        var replay = CreateOperation(FirstSequence);
        var request = CreateRequest([], replay: [replay]);
        var result = CreateRecoveredResult(request, [MissingResult(replay.OperationId)]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Creates a terminal rejected disposition fixture.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The rejected disposition fixture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SnapshotOperationDisposition Rejected(OperationId operationId) =>
        Disposition(operationId, SnapshotOperationDispositionKind.TerminalRejected, OperationResultKind.Rejected);

    /// <summary>Gets the exact logical byte count for one replay-only request fixture.</summary>
    /// <returns>The request logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetSingleReplayRequestLogicalBytes() =>
        GetRequestHeaderLogicalBytes() + Int32LogicalBytes + Int32LogicalBytes + GetOperationLogicalBytes();

    /// <summary>Gets the logical byte count for the standard request header fixture.</summary>
    /// <returns>The request header logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRequestHeaderLogicalBytes() =>
        Stream.Value.Length + GuidLogicalBytes + Cursor.Length + ClientStateContractId.Length + Int32LogicalBytes + Int32LogicalBytes + Int64LogicalBytes;

    /// <summary>Gets the logical byte count for one standard operation fixture.</summary>
    /// <returns>The operation logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOperationLogicalBytes() =>
        GuidLogicalBytes
        + Stream.Value.Length
        + Int64LogicalBytes
        + DateTimeOffsetLogicalBytes
        + Int32LogicalBytes
        + GetOperationPayloadLogicalBytes()
        + GetOperationMetadataLogicalBytes()
        + GetOperationPolicyLogicalBytes();

    /// <summary>Gets the logical byte count for one standard operation payload fixture.</summary>
    /// <returns>The operation payload logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOperationPayloadLogicalBytes() =>
        Int32LogicalBytes + Int64LogicalBytes + OperationContractId.Length + ContentType.Length + PayloadHash.Length + PayloadByteLength;

    /// <summary>Gets the logical byte count for one standard operation metadata fixture.</summary>
    /// <returns>The operation metadata logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOperationMetadataLogicalBytes() =>
        Int32LogicalBytes + "kind".Length + "order".Length;

    /// <summary>Gets the logical byte count for one operation policy fixture.</summary>
    /// <returns>The operation policy logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetOperationPolicyLogicalBytes() =>
        Int32LogicalBytes + Int32LogicalBytes + Int32LogicalBytes + Int32LogicalBytes;
}
