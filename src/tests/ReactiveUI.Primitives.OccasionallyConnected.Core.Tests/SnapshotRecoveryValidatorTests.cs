// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SnapshotRecoveryValidator"/>.</summary>
public sealed partial class SnapshotRecoveryValidatorTests
{
    /// <summary>The invalid status selector used by test fixtures.</summary>
    private const int InvalidStatusValue = 99;

    /// <summary>The logical byte width counted for Int32 and enum scalar values.</summary>
    private const int Int32LogicalBytes = 4;

    /// <summary>The logical byte width counted for Int64 scalar values.</summary>
    private const int Int64LogicalBytes = 8;

    /// <summary>The logical byte width counted for Guid scalar values.</summary>
    private const int GuidLogicalBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset scalar values.</summary>
    private const int DateTimeOffsetLogicalBytes = 16;

    /// <summary>The first operation sequence used by test fixtures.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second operation sequence used by test fixtures.</summary>
    private const int SecondSequence = 2;

    /// <summary>The schema version used by client state payload fixtures.</summary>
    private const int ClientStateSchemaVersion = 2;

    /// <summary>The schema version used by operation payload fixtures.</summary>
    private const int OperationSchemaVersion = 1;

    /// <summary>The snapshot format version used by test fixtures.</summary>
    private const int FormatVersion = 3;

    /// <summary>The stream revision used by test fixtures.</summary>
    private const long Revision = 7;

    /// <summary>The next stream revision used by mismatched test fixtures.</summary>
    private const long NextRevision = 8;

    /// <summary>The maximum response byte limit used by test requests.</summary>
    private const int MaximumResponseBytes = 1024 * 1024;

    /// <summary>The maximum aggregate metadata byte limit used by precise metadata overflow tests.</summary>
    private const int MaximumMetadataAggregateBytes = 20;

    /// <summary>The metadata value length used to exceed the aggregate metadata limit.</summary>
    private const int MetadataAggregateValueLength = MaximumMetadataAggregateBytes / (FirstSequence + FirstSequence);

    /// <summary>The largest pending-operation limit allowed by snapshot recovery ownership guards.</summary>
    private const int MaximumOwnedOperations = 4096;

    /// <summary>The payload byte length used by normal test fixtures.</summary>
    private const int PayloadByteLength = 3;

    /// <summary>The frontier cursor used by test fixtures.</summary>
    private const string Cursor = "frontier";

    /// <summary>The client state contract identifier used by test fixtures.</summary>
    private const string ClientStateContractId = "order-state";

    /// <summary>The operation payload contract identifier used by test fixtures.</summary>
    private const string OperationContractId = "order-command";

    /// <summary>The content type used by payload fixtures.</summary>
    private const string ContentType = "application/json";

    /// <summary>The payload hash used by payload fixtures.</summary>
    private const string PayloadHash = "sha256-test";

    /// <summary>The server version used by result and checkpoint fixtures.</summary>
    private const string ServerVersionValue = "v7";

    /// <summary>The stream identifier used by test fixtures.</summary>
    private static readonly StreamId Stream = new("orders/live");

    /// <summary>The foreign stream identifier used by invalid fixtures.</summary>
    private static readonly StreamId ForeignStream = new("orders/archive");

    /// <summary>Malformed request header selectors.</summary>
    public enum InvalidRequest
    {
        /// <summary>A default stream identifier.</summary>
        Stream = 0,

        /// <summary>A default subscription identifier.</summary>
        Subscription = 1,

        /// <summary>An empty contract identifier.</summary>
        Contract = 2,

        /// <summary>A non-positive schema version.</summary>
        Schema = 3,

        /// <summary>A non-positive format version.</summary>
        Format = 4,

        /// <summary>A non-positive response byte limit.</summary>
        ResponseBytes = 5,

        /// <summary>A response byte limit above caller limits.</summary>
        ResponseLimit = 6,
    }

    /// <summary>Malformed pending operation selectors.</summary>
    public enum InvalidOperationShape
    {
        /// <summary>A default operation identifier.</summary>
        Operation = 0,

        /// <summary>A foreign stream identifier.</summary>
        Stream = 1,

        /// <summary>A non-positive sequence.</summary>
        Sequence = 2,

        /// <summary>An undefined operation type.</summary>
        Type = 3,

        /// <summary>A duplicated operation identifier.</summary>
        DuplicateOperation = 4,

        /// <summary>A duplicated client sequence.</summary>
        DuplicateSequence = 5,

        /// <summary>A non-positive payload schema.</summary>
        PayloadSchema = 6,

        /// <summary>An oversized payload.</summary>
        PayloadSize = 7,

        /// <summary>Too many metadata entries.</summary>
        MetadataEntries = 8,

        /// <summary>Too many metadata bytes.</summary>
        MetadataBytes = 9,

        /// <summary>Too many aggregate metadata bytes.</summary>
        MetadataAggregate = 10,

        /// <summary>An empty base version.</summary>
        BaseVersion = 11,
    }

    /// <summary>Malformed checkpoint selectors.</summary>
    public enum InvalidCheckpoint
    {
        /// <summary>A foreign stream identifier.</summary>
        Stream = 0,

        /// <summary>A mismatched subscription identifier.</summary>
        Subscription = 1,

        /// <summary>A mismatched snapshot format.</summary>
        Format = 2,

        /// <summary>A mismatched payload contract.</summary>
        Contract = 3,

        /// <summary>A mismatched payload schema.</summary>
        Schema = 4,

        /// <summary>An empty frontier cursor.</summary>
        Cursor = 5,

        /// <summary>An empty server version.</summary>
        ServerVersion = 6,
    }

    /// <summary>Malformed disposition selectors.</summary>
    public enum InvalidDisposition
    {
        /// <summary>A default operation identifier.</summary>
        Operation = 0,

        /// <summary>An undefined disposition kind.</summary>
        Kind = 1,

        /// <summary>A duplicated disposition operation.</summary>
        Duplicate = 2,

        /// <summary>An omitted pending operation.</summary>
        Omitted = 3,

        /// <summary>A proven disposition without a result.</summary>
        MissingResult = 4,

        /// <summary>A proven disposition with a result for another operation.</summary>
        MismatchedResult = 5,
    }

    /// <summary>Malformed local mutation selectors.</summary>
    public enum InvalidMutation
    {
        /// <summary>A default stream identifier.</summary>
        DefaultStream = 0,

        /// <summary>A default subscription identifier.</summary>
        DefaultSubscription = 1,

        /// <summary>A negative expected revision.</summary>
        NegativeRevision = 2,

        /// <summary>A foreign stream identifier.</summary>
        Stream = 3,

        /// <summary>A mismatched subscription identifier.</summary>
        Subscription = 4,

        /// <summary>A mismatched revision.</summary>
        Revision = 5,

        /// <summary>A mismatched previous cursor.</summary>
        Cursor = 6,

        /// <summary>A mismatched snapshot format.</summary>
        Format = 7,

        /// <summary>A mismatched optimistic payload contract.</summary>
        OptimisticContract = 8,

        /// <summary>A mismatched optimistic payload schema.</summary>
        OptimisticSchema = 9,
    }

    /// <summary>Verifies a recovered response accepts conflict-resolved inclusion and unknown without proof.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsRecoveredResultWithExactDispositions()
    {
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        var request = CreateRequest([first, second]);
        var result = CreateRecoveredResult(
            request,
            [
                Included(first.OperationId, OperationResultKind.Conflict),
                Unknown(second.OperationId),
            ]);

        SnapshotRecoveryValidator.Validate(request, result, new());

        await Assert.That(result.Checkpoint?.SnapshotFormatVersion).IsEqualTo(FormatVersion);
    }

    /// <summary>Verifies non-recovered responses cannot carry checkpoint or pending-operation proof.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNonRecoveredResultWithDispositions()
    {
        var request = CreateRequest([CreateOperation(FirstSequence)]);
        var result = new RemoteSnapshotRecoveryResult
        {
            Status = RemoteSnapshotRecoveryStatus.RetentionExpired,
            OperationDispositions = [Unknown(request.PendingOperations[0].OperationId)],
            ReasonCode = "OC.RetentionExpired",
        };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies included, rejected, and unknown dispositions have distinct allowed result shapes.</summary>
    /// <param name="kind">The disposition kind.</param>
    /// <param name="resultKind">The optional operation result kind.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(SnapshotOperationDispositionKind.IncludedAccepted, OperationResultKind.Retryable)]
    [Arguments(SnapshotOperationDispositionKind.TerminalRejected, OperationResultKind.Accepted)]
    [Arguments(SnapshotOperationDispositionKind.TerminalRejected, OperationResultKind.Conflict)]
    [Arguments(SnapshotOperationDispositionKind.Unknown, OperationResultKind.Rejected)]
    public async Task ValidateRejectsInvalidDispositionResultPair(
        SnapshotOperationDispositionKind kind,
        OperationResultKind resultKind)
    {
        var operation = CreateOperation(FirstSequence);
        var request = CreateRequest([operation]);
        var result = CreateRecoveredResult(request, [Disposition(operation.OperationId, kind, resultKind)]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies a local mutation must bind exactly to the recovered stream and pending operation set.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsLocalMutationBoundToRecoveredState()
    {
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        var request = CreateRequest([first, second]);
        var result = CreateRecoveredResult(request, [Included(first.OperationId, OperationResultKind.Accepted), Unknown(second.OperationId)]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [first, second]);
        var mutation = CreateMutation(request, result.Checkpoint, result.OperationDispositions);

        SnapshotRecoveryValidator.Validate(mutation, recovered, new());

        await Assert.That(mutation.OptimisticState.ContractId).IsEqualTo(request.ClientStateContractId);
    }

    /// <summary>Verifies a local mutation cannot omit an unresolved recovered pending operation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLocalMutationMissingRecoveredPendingOperation()
    {
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        var request = CreateRequest([first, second]);
        var result = CreateRecoveredResult(request, [Unknown(first.OperationId), Unknown(second.OperationId)]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [first, second]);
        var mutation = CreateMutation(request, result.Checkpoint, [Unknown(first.OperationId)]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies empty cursors are invalid while null expired cursors remain distinct.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsEmptyCursorsAndAcceptsNullExpiredCursor()
    {
        SnapshotRecoveryValidator.Validate(CreateRequest([], expiredCursor: null), new());
        var request = CreateRequest([], expiredCursor: string.Empty);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies every non-recovered status carries no checkpoint or disposition data.</summary>
    /// <param name="status">The non-recovered status.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(RemoteSnapshotRecoveryStatus.UnsupportedProjection)]
    [Arguments(RemoteSnapshotRecoveryStatus.RetentionExpired)]
    [Arguments(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation)]
    [Arguments(RemoteSnapshotRecoveryStatus.ValidationRejected)]
    [Arguments(RemoteSnapshotRecoveryStatus.CapacityExceeded)]
    [Arguments(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange)]
    public async Task ValidateAcceptsNonRecoveredResultWithoutCheckpoint(RemoteSnapshotRecoveryStatus status)
    {
        var result = new RemoteSnapshotRecoveryResult { Status = status, ReasonCode = "OC.NotRecovered" };

        SnapshotRecoveryValidator.Validate(CreateRequest([]), result, new());

        await Assert.That(result.OperationDispositions).IsEmpty();
    }

    /// <summary>Verifies recovered responses require a checkpoint.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRecoveredResultWithoutCheckpoint()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.Recovered };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(CreateRequest([]), result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies non-recovered responses still honor the request response byte ceiling.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNonRecoveredResultAboveRequestResponseBytes()
    {
        var request = CreateRequest([]) with { MaximumResponseBytes = FirstSequence };
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired, ReasonCode = "OC.NotRecovered" };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies undefined recovery statuses are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsUndefinedRecoveryStatus()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = (RemoteSnapshotRecoveryStatus)InvalidStatusValue };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(CreateRequest([]), result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed request headers are rejected.</summary>
    /// <param name="selector">The malformed request selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(InvalidRequest.Stream)]
    [Arguments(InvalidRequest.Subscription)]
    [Arguments(InvalidRequest.Contract)]
    [Arguments(InvalidRequest.Schema)]
    [Arguments(InvalidRequest.Format)]
    [Arguments(InvalidRequest.ResponseBytes)]
    [Arguments(InvalidRequest.ResponseLimit)]
    public async Task ValidateRejectsMalformedRequestHeaders(InvalidRequest selector)
    {
        var request = selector switch
        {
            InvalidRequest.Stream => CreateRequest([]) with { StreamId = default },
            InvalidRequest.Subscription => CreateRequest([]) with { SubscriptionId = default },
            InvalidRequest.Contract => CreateRequest([]) with { ClientStateContractId = string.Empty },
            InvalidRequest.Schema => CreateRequest([]) with { ClientStateSchemaVersion = 0 },
            InvalidRequest.Format => CreateRequest([]) with { SnapshotFormatVersion = 0 },
            InvalidRequest.ResponseBytes => CreateRequest([]) with { MaximumResponseBytes = 0 },
            _ => CreateRequest([]) with { MaximumResponseBytes = MaximumResponseBytes },
        };
        var limits = selector == InvalidRequest.ResponseLimit
            ? new SnapshotRecoveryLimits { MaximumLogicalBytes = MaximumResponseBytes - FirstSequence }
            : new SnapshotRecoveryLimits();

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, limits))
            .Throws<ArgumentException>();
    }

    /// <summary>Verifies malformed pending operation sets are rejected.</summary>
    /// <param name="selector">The malformed operation selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(InvalidOperationShape.Operation)]
    [Arguments(InvalidOperationShape.Stream)]
    [Arguments(InvalidOperationShape.Sequence)]
    [Arguments(InvalidOperationShape.Type)]
    [Arguments(InvalidOperationShape.DuplicateOperation)]
    [Arguments(InvalidOperationShape.DuplicateSequence)]
    [Arguments(InvalidOperationShape.PayloadSchema)]
    [Arguments(InvalidOperationShape.PayloadSize)]
    [Arguments(InvalidOperationShape.MetadataEntries)]
    [Arguments(InvalidOperationShape.MetadataBytes)]
    [Arguments(InvalidOperationShape.MetadataAggregate)]
    [Arguments(InvalidOperationShape.BaseVersion)]
    public async Task ValidateRejectsMalformedPendingOperations(InvalidOperationShape selector)
    {
        var first = CreateOperation(FirstSequence);
        var second = selector switch
        {
            InvalidOperationShape.Operation => CreateOperation(SecondSequence) with { OperationId = default },
            InvalidOperationShape.Stream => CreateOperation(SecondSequence) with { StreamId = ForeignStream },
            InvalidOperationShape.Sequence => CreateOperation(SecondSequence) with { ClientSequence = 0 },
            InvalidOperationShape.Type => CreateOperation(SecondSequence) with { Type = (SyncOperationType)InvalidStatusValue },
            InvalidOperationShape.DuplicateOperation => CreateOperation(SecondSequence) with { OperationId = first.OperationId },
            InvalidOperationShape.DuplicateSequence => CreateOperation(FirstSequence),
            InvalidOperationShape.PayloadSchema => CreateOperation(SecondSequence) with { Payload = CreatePayload(OperationContractId, 0) },
            InvalidOperationShape.PayloadSize => CreateOperation(SecondSequence) with { Payload = CreatePayload(OperationContractId, OperationSchemaVersion, MaximumResponseBytes) },
            InvalidOperationShape.MetadataEntries => CreateOperation(SecondSequence) with { Metadata = CreateMetadata(SecondSequence) },
            InvalidOperationShape.MetadataBytes => CreateOperation(SecondSequence) with { Metadata = CreateMetadata(FirstSequence, MaximumResponseBytes) },
            InvalidOperationShape.MetadataAggregate => CreateOperation(SecondSequence) with { Metadata = CreateMetadata(SecondSequence, MetadataAggregateValueLength) },
            _ => CreateOperation(SecondSequence) with { BaseVersion = string.Empty },
        };
        var request = CreateRequest([first, second]);
        var limits = new SnapshotRecoveryLimits
        {
            MaximumPendingOperations = MaximumOwnedOperations,
            MaximumPayloadBytes = MaximumResponseBytes - FirstSequence,
            MaximumMetadataEntries = selector == InvalidOperationShape.MetadataEntries ? FirstSequence : MaximumResponseBytes,
            MaximumMetadataBytes = selector == InvalidOperationShape.MetadataAggregate ? MaximumMetadataAggregateBytes : MaximumResponseBytes - FirstSequence,
            MaximumLogicalBytes = MaximumResponseBytes,
        };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, limits))
            .Throws<ArgumentException>();
    }

    /// <summary>Verifies the caller-specific pending operation ceiling is enforced after owned DTO copying.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsPendingOperationsAboveCallerLimit()
    {
        var request = CreateRequest([CreateOperation(FirstSequence), CreateOperation(SecondSequence)]);
        var limits = new SnapshotRecoveryLimits { MaximumPendingOperations = FirstSequence };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed checkpoints are rejected.</summary>
    /// <param name="selector">The malformed checkpoint selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(InvalidCheckpoint.Stream)]
    [Arguments(InvalidCheckpoint.Subscription)]
    [Arguments(InvalidCheckpoint.Format)]
    [Arguments(InvalidCheckpoint.Contract)]
    [Arguments(InvalidCheckpoint.Schema)]
    [Arguments(InvalidCheckpoint.Cursor)]
    [Arguments(InvalidCheckpoint.ServerVersion)]
    public async Task ValidateRejectsMalformedCheckpoint(InvalidCheckpoint selector)
    {
        var request = CreateRequest([]);
        var checkpoint = selector switch
        {
            InvalidCheckpoint.Stream => CreateCheckpoint(request) with { StreamId = ForeignStream },
            InvalidCheckpoint.Subscription => CreateCheckpoint(request) with { SubscriptionId = SubscriptionId.New() },
            InvalidCheckpoint.Format => CreateCheckpoint(request) with { SnapshotFormatVersion = FormatVersion + FirstSequence },
            InvalidCheckpoint.Contract => CreateCheckpoint(request) with { ClientState = CreatePayload(OperationContractId, ClientStateSchemaVersion) },
            InvalidCheckpoint.Schema => CreateCheckpoint(request) with { ClientState = CreatePayload(ClientStateContractId, OperationSchemaVersion) },
            InvalidCheckpoint.Cursor => CreateCheckpoint(request) with { FrontierCursor = string.Empty },
            _ => CreateCheckpoint(request) with { ServerVersion = string.Empty },
        };
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.Recovered, Checkpoint = checkpoint };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed dispositions are rejected.</summary>
    /// <param name="selector">The malformed disposition selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(InvalidDisposition.Operation)]
    [Arguments(InvalidDisposition.Kind)]
    [Arguments(InvalidDisposition.Duplicate)]
    [Arguments(InvalidDisposition.Omitted)]
    [Arguments(InvalidDisposition.MissingResult)]
    [Arguments(InvalidDisposition.MismatchedResult)]
    public async Task ValidateRejectsMalformedDispositions(InvalidDisposition selector)
    {
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        var request = CreateRequest([first, second]);
        IReadOnlyList<SnapshotOperationDisposition> dispositions = selector switch
        {
            InvalidDisposition.Operation => [Unknown(default), Unknown(second.OperationId)],
            InvalidDisposition.Kind => [Disposition(first.OperationId, (SnapshotOperationDispositionKind)InvalidStatusValue, OperationResultKind.Accepted), Unknown(second.OperationId)],
            InvalidDisposition.Duplicate => [Unknown(first.OperationId), Unknown(first.OperationId)],
            InvalidDisposition.Omitted => [Unknown(first.OperationId), Unknown(OperationId.New())],
            InvalidDisposition.MissingResult => [MissingResult(first.OperationId), Unknown(second.OperationId)],
            _ => [RejectedForAnotherOperation(first.OperationId, second.OperationId), Unknown(second.OperationId)],
        };
        var result = CreateRecoveredResult(request, dispositions);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutations reject mismatched format, identity, revision, and optimistic payload.</summary>
    /// <param name="selector">The malformed mutation selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(InvalidMutation.DefaultStream)]
    [Arguments(InvalidMutation.DefaultSubscription)]
    [Arguments(InvalidMutation.NegativeRevision)]
    [Arguments(InvalidMutation.Stream)]
    [Arguments(InvalidMutation.Subscription)]
    [Arguments(InvalidMutation.Revision)]
    [Arguments(InvalidMutation.Cursor)]
    [Arguments(InvalidMutation.Format)]
    [Arguments(InvalidMutation.OptimisticContract)]
    [Arguments(InvalidMutation.OptimisticSchema)]
    public async Task ValidateRejectsMalformedLocalMutation(InvalidMutation selector)
    {
        var operation = CreateOperation(FirstSequence);
        var request = CreateRequest([operation]);
        var result = CreateRecoveredResult(request, [Unknown(operation.OperationId)]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [operation]);
        var mutation = selector switch
        {
            InvalidMutation.DefaultStream => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { StreamId = default },
            InvalidMutation.DefaultSubscription => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { SubscriptionId = default },
            InvalidMutation.NegativeRevision => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { ExpectedRevision = -FirstSequence },
            InvalidMutation.Stream => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { StreamId = ForeignStream },
            InvalidMutation.Subscription => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { SubscriptionId = SubscriptionId.New() },
            InvalidMutation.Revision => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { ExpectedRevision = NextRevision },
            InvalidMutation.Cursor => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { ExpectedPreviousCursor = "other" },
            InvalidMutation.Format => CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { SnapshotFormatVersion = FormatVersion + FirstSequence },
            InvalidMutation.OptimisticContract => CreateMutationWithOptimisticState(request, result, OperationContractId, ClientStateSchemaVersion),
            _ => CreateMutationWithOptimisticState(request, result, ClientStateContractId, OperationSchemaVersion),
        };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local validation accepts an initial recovered cursor binding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsNullPreviousCursorBinding()
    {
        var request = CreateRequest([], expiredCursor: null);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []) with { ExpectedPreviousCursor = null };
        var recovered = CreateRecoveredStream(request.SubscriptionId, [], Stream, null);

        SnapshotRecoveryValidator.Validate(mutation, recovered, new());

        await Assert.That(mutation.ExpectedPreviousCursor).IsNull();
    }

    /// <summary>Verifies local validation rejects a null cursor when the recovered stream has a cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNullPreviousCursorMismatch()
    {
        var request = CreateRequest([], expiredCursor: null);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []) with { ExpectedPreviousCursor = null };
        var recovered = CreateRecoveredStream(request.SubscriptionId, []);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local validation rejects a cursor when the recovered stream has an initial cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsPresentPreviousCursorMismatch()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [], Stream, null);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local validation accepts the initial state before a durable snapshot exists.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsRecoveredStreamWithoutSnapshot()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []) with { ExpectedRevision = 0 };
        var recovered = new RecoveredStream(request.SubscriptionId, Cursor, null, [], [], FormatVersion);

        SnapshotRecoveryValidator.Validate(mutation, recovered, new());

        await Assert.That(recovered.Snapshot).IsNull();
    }

    /// <summary>Verifies local mutations reject runtime-null required fields as argument failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRuntimeNullMutationCheckpoint()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, []);
        mutation = mutation with { Checkpoint = NullReference<RemoteSnapshotCheckpoint>() };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies recovered checkpoints reject runtime-null client-state payloads as argument failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRuntimeNullCheckpointClientState()
    {
        var request = CreateRequest([]);
        var checkpoint = CreateCheckpoint(request) with { ClientState = NullReference<PayloadEnvelope>() };
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.Recovered, Checkpoint = checkpoint };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutations reject recovered snapshots from another stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRecoveredSnapshotStreamMismatch()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [], ForeignStream);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutations reject recovered pending operations from another stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRecoveredPendingStreamMismatch()
    {
        var operation = CreateOperation(FirstSequence) with { StreamId = ForeignStream };
        var request = CreateRequest([CreateOperation(FirstSequence)]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), [Unknown(request.PendingOperations[0].OperationId)]);
        var recovered = CreateRecoveredStream(request.SubscriptionId, [operation]);

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies local mutations reject recovered replay operations from another stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsRecoveredReplayStreamMismatch()
    {
        var request = CreateRequest([]);
        var mutation = CreateMutation(request, CreateCheckpoint(request), []);
        var recovered = CreateRecoveredStream(request.SubscriptionId, []) with
        {
            ReplayOperations = [CreateOperation(FirstSequence) with { StreamId = ForeignStream }],
        };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(mutation, recovered, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies reason codes must be stable protocol identifiers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedReasonCode()
    {
        var result = new RemoteSnapshotRecoveryResult { Status = RemoteSnapshotRecoveryStatus.RetentionExpired, ReasonCode = "OC bad" };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(CreateRequest([]), result, new()))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies logical byte accounting rejects aggregate overflow.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsLogicalByteOverflow()
    {
        var request = CreateRequest([CreateOperation(FirstSequence)]) with { MaximumResponseBytes = FirstSequence };
        var result = CreateRecoveredResult(request, [Unknown(request.PendingOperations[0].OperationId)]);
        var limits = new SnapshotRecoveryLimits { MaximumLogicalBytes = MaximumResponseBytes, MaximumPayloadBytes = MaximumResponseBytes };

        await Assert.That(() => SnapshotRecoveryValidator.Validate(request, result, limits))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Creates a recovery request fixture with the supplied pending and replay operations.</summary>
    /// <param name="pending">The pending operations.</param>
    /// <param name="expiredCursor">The optional expired cursor.</param>
    /// <param name="replay">The replay-only operations.</param>
    /// <returns>The recovery request fixture.</returns>
    private static RemoteSnapshotRecoveryRequest CreateRequest(
        IReadOnlyList<SyncOperation> pending,
        string? expiredCursor = Cursor,
        IReadOnlyList<SyncOperation>? replay = null) => new()
    {
        StreamId = Stream,
        SubscriptionId = SubscriptionId.New(),
        ExpiredCursor = expiredCursor,
        ClientStateContractId = ClientStateContractId,
        ClientStateSchemaVersion = ClientStateSchemaVersion,
        SnapshotFormatVersion = FormatVersion,
        PendingOperations = pending,
        ReplayOperations = replay ?? [],
        MaximumResponseBytes = MaximumResponseBytes,
    };

    /// <summary>Creates a recovered result fixture.</summary>
    /// <param name="request">The request to bind.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The recovered result fixture.</returns>
    private static RemoteSnapshotRecoveryResult CreateRecoveredResult(
        RemoteSnapshotRecoveryRequest request,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) => new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateCheckpoint(request),
            OperationDispositions = dispositions,
        };

    /// <summary>Creates a local snapshot recovery mutation fixture.</summary>
    /// <param name="request">The request to bind.</param>
    /// <param name="checkpoint">The recovered checkpoint.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The mutation fixture.</returns>
    private static LocalSnapshotRecoveryMutation CreateMutation(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotCheckpoint? checkpoint,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) => new()
        {
            StreamId = request.StreamId,
            SubscriptionId = request.SubscriptionId,
            ExpectedRevision = Revision,
            ExpectedPreviousCursor = Cursor,
            Checkpoint = RequireCheckpoint(checkpoint),
            OptimisticState = CreatePayload(ClientStateContractId, ClientStateSchemaVersion),
            SnapshotFormatVersion = FormatVersion,
            OperationDispositions = dispositions,
        };

    /// <summary>Creates a mutation fixture with a caller-selected optimistic payload.</summary>
    /// <param name="request">The request to bind.</param>
    /// <param name="result">The result carrying checkpoint and dispositions.</param>
    /// <param name="contractId">The optimistic payload contract identifier.</param>
    /// <param name="schemaVersion">The optimistic payload schema version.</param>
    /// <returns>The mutation fixture.</returns>
    private static LocalSnapshotRecoveryMutation CreateMutationWithOptimisticState(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotRecoveryResult result,
        string contractId,
        int schemaVersion) =>
        CreateMutation(request, result.Checkpoint, result.OperationDispositions) with { OptimisticState = CreatePayload(contractId, schemaVersion) };

    /// <summary>Creates a checkpoint fixture bound to the supplied request.</summary>
    /// <param name="request">The request to bind.</param>
    /// <returns>The checkpoint fixture.</returns>
    private static RemoteSnapshotCheckpoint CreateCheckpoint(RemoteSnapshotRecoveryRequest request) => new()
    {
        StreamId = request.StreamId,
        SubscriptionId = request.SubscriptionId,
        FrontierCursor = Cursor,
        ServerVersion = ServerVersionValue,
        SnapshotFormatVersion = request.SnapshotFormatVersion,
        ClientState = CreatePayload(request.ClientStateContractId, request.ClientStateSchemaVersion),
        ObservedAtUtc = DateTimeOffset.UnixEpoch,
    };

    /// <summary>Creates a recovered stream fixture.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pending">The recovered pending operations.</param>
    /// <returns>The recovered stream fixture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RecoveredStream CreateRecoveredStream(SubscriptionId subscriptionId, IReadOnlyList<SyncOperation> pending) =>
        CreateRecoveredStream(subscriptionId, pending, Stream);

    /// <summary>Creates a recovered stream fixture with a caller-selected snapshot stream.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pending">The recovered pending operations.</param>
    /// <param name="snapshotStream">The snapshot stream identifier.</param>
    /// <param name="serverCursor">The recovered server cursor.</param>
    /// <param name="replay">The recovered replay operations.</param>
    /// <returns>The recovered stream fixture.</returns>
    private static RecoveredStream CreateRecoveredStream(
        SubscriptionId subscriptionId,
        IReadOnlyList<SyncOperation> pending,
        StreamId snapshotStream,
        string? serverCursor = Cursor,
        IReadOnlyList<SyncOperation>? replay = null) =>
        new(
            subscriptionId,
            serverCursor,
            new(snapshotStream, FormatVersion, serverCursor, CreatePayload(ClientStateContractId, ClientStateSchemaVersion), Revision, DateTimeOffset.UnixEpoch),
            pending,
            [],
            FormatVersion) { ReplayOperations = replay ?? pending };

    /// <summary>Creates an included disposition fixture.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="kind">The server result kind.</param>
    /// <returns>The included disposition fixture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SnapshotOperationDisposition Included(OperationId operationId, OperationResultKind kind) =>
        Disposition(operationId, SnapshotOperationDispositionKind.IncludedAccepted, kind);

    /// <summary>Creates an unknown disposition fixture.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The unknown disposition fixture.</returns>
    private static SnapshotOperationDisposition Unknown(OperationId operationId) => new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown };

    /// <summary>Creates a disposition fixture.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="kind">The disposition kind.</param>
    /// <param name="resultKind">The optional operation result kind.</param>
    /// <returns>The disposition fixture.</returns>
    private static SnapshotOperationDisposition Disposition(
        OperationId operationId,
        SnapshotOperationDispositionKind kind,
        OperationResultKind resultKind) => new() { OperationId = operationId, Kind = kind, Result = new(operationId, resultKind, null, ServerVersionValue) };

    /// <summary>Creates a proven disposition without the required result.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The malformed disposition fixture.</returns>
    private static SnapshotOperationDisposition MissingResult(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted };

    /// <summary>Creates a rejected disposition whose result belongs to another operation.</summary>
    /// <param name="operationId">The disposition operation identifier.</param>
    /// <param name="resultOperationId">The result operation identifier.</param>
    /// <returns>The malformed disposition fixture.</returns>
    private static SnapshotOperationDisposition RejectedForAnotherOperation(OperationId operationId, OperationId resultOperationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.TerminalRejected, Result = new(resultOperationId, OperationResultKind.Rejected, "OC.Rejected", ServerVersionValue) };

    /// <summary>Requires a checkpoint for mutation fixtures.</summary>
    /// <param name="checkpoint">The optional checkpoint.</param>
    /// <returns>The checkpoint.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="checkpoint"/> is null.</exception>
    private static RemoteSnapshotCheckpoint RequireCheckpoint(RemoteSnapshotCheckpoint? checkpoint) =>
        checkpoint ?? throw new InvalidOperationException("The test requires a checkpoint.");

    /// <summary>Creates a pending operation fixture.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation fixture.</returns>
    private static SyncOperation CreateOperation(long sequence) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Custom,
        Payload = CreatePayload(OperationContractId, OperationSchemaVersion),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string> { ["kind"] = "order" },
    };

    /// <summary>Creates a payload fixture.</summary>
    /// <param name="contractId">The payload contract identifier.</param>
    /// <param name="schemaVersion">The payload schema version.</param>
    /// <returns>The payload fixture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreatePayload(string contractId, int schemaVersion) =>
        CreatePayload(contractId, schemaVersion, PayloadByteLength);

    /// <summary>Creates a payload fixture with a caller-selected payload byte count.</summary>
    /// <param name="contractId">The payload contract identifier.</param>
    /// <param name="schemaVersion">The payload schema version.</param>
    /// <param name="payloadLength">The payload length.</param>
    /// <returns>The payload fixture.</returns>
    private static PayloadEnvelope CreatePayload(string contractId, int schemaVersion, int payloadLength) =>
        new(contractId, schemaVersion, ContentType, new byte[payloadLength], PayloadHash);

    /// <summary>Creates metadata with a caller-selected item count.</summary>
    /// <param name="count">The metadata entry count.</param>
    /// <returns>The metadata fixture.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Dictionary<string, string> CreateMetadata(int count) =>
        CreateMetadata(count, FirstSequence);

    /// <summary>Creates metadata with caller-selected count and value length.</summary>
    /// <param name="count">The metadata entry count.</param>
    /// <param name="valueLength">The metadata value length.</param>
    /// <returns>The metadata fixture.</returns>
    private static Dictionary<string, string> CreateMetadata(int count, int valueLength)
    {
        var metadata = new Dictionary<string, string>(capacity: count);
        var value = new string('x', valueLength);
        for (var index = 0; index < count; index++)
        {
            metadata.Add($"key-{index}", value);
        }

        return metadata;
    }

    /// <summary>Creates a typed null reference for runtime-null contract regression tests.</summary>
    /// <typeparam name="T">The reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }
}
