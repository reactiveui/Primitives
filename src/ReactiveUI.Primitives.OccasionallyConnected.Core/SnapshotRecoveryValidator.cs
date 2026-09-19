// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates structural snapshot recovery contracts before transport or local transaction boundaries.</summary>
public static class SnapshotRecoveryValidator
{
    /// <summary>The logical byte width counted for Int32 and enum scalar values.</summary>
    private const long Int32LogicalBytes = 4;

    /// <summary>The logical byte width counted for Int64 scalar values.</summary>
    private const long Int64LogicalBytes = 8;

    /// <summary>The logical byte width counted for Guid scalar values.</summary>
    private const long GuidLogicalBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset scalar values.</summary>
    private const long DateTimeOffsetLogicalBytes = 16;

    /// <summary>The strict UTF-8 encoder used for protocol string byte accounting.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Validates a snapshot recovery request.</summary>
    /// <param name="request">The request to validate.</param>
    /// <param name="limits">The finite caller-supplied validation limits.</param>
    /// <exception cref="ArgumentException">The request is malformed or exceeds a limit.</exception>
    public static void Validate(RemoteSnapshotRecoveryRequest request, SnapshotRecoveryLimits limits)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ValidateLimits(limits);
        var logicalBytes = ValidateRequestHeader(request, limits);
        logicalBytes += ValidateOperations(request.PendingOperations, request.ReplayOperations, request.StreamId, limits);
        ThrowIfLogicalBytesExceeded(logicalBytes, limits.MaximumLogicalBytes, nameof(request));
    }

    /// <summary>Validates a response against the exact request that produced it.</summary>
    /// <param name="request">The request sent to the remote peer.</param>
    /// <param name="result">The response to validate.</param>
    /// <param name="limits">The finite caller-supplied validation limits.</param>
    /// <exception cref="ArgumentException">The response is malformed or not bound to the request.</exception>
    public static void Validate(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryLimits limits)
    {
        Validate(request, limits);
        ArgumentExceptionHelper.ThrowIfNull(result);
        ValidateRecoveryStatus(result.Status);

        var dispositions = result.OperationDispositions;
        var logicalBytes = Int32LogicalBytes + ValidateReasonCode(result.ReasonCode, limits);
        if (result.Status != RemoteSnapshotRecoveryStatus.Recovered)
        {
            if (result.Checkpoint is not null || dispositions.Count != 0)
            {
                throw new ArgumentException("A non-recovered snapshot result must not carry a checkpoint or operation dispositions.", nameof(result));
            }

            logicalBytes += Int32LogicalBytes;
            ThrowIfLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes, nameof(result));
            ThrowIfLogicalBytesExceeded(logicalBytes, limits.MaximumLogicalBytes, nameof(result));
            return;
        }

        if (result.Checkpoint is null)
        {
            throw new ArgumentException("A recovered snapshot result must carry a checkpoint.", nameof(result));
        }

        logicalBytes += ValidateCheckpoint(result.Checkpoint, request, limits);
        logicalBytes += ValidateDispositions(dispositions, request.PendingOperations, request.ReplayOperations, limits);
        ThrowIfLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes, nameof(result));
        ThrowIfLogicalBytesExceeded(logicalBytes, limits.MaximumLogicalBytes, nameof(result));
    }

    /// <summary>Validates a local recovery mutation against the recovered local stream state.</summary>
    /// <param name="mutation">The local mutation to validate.</param>
    /// <param name="recovered">The recovered local stream state used to build the mutation.</param>
    /// <param name="limits">The finite caller-supplied validation limits.</param>
    /// <exception cref="ArgumentException">The mutation is malformed or not bound to recovered state.</exception>
    /// <remarks>
    /// This method performs structural checks only. Store implementations remain responsible for checking durable
    /// local ownership and committing the mutation transactionally. The caller authenticates the remote response;
    /// the server validates its durable cursor offer when recovery is acknowledged.
    /// </remarks>
    public static void Validate(
        LocalSnapshotRecoveryMutation mutation,
        RecoveredStream recovered,
        SnapshotRecoveryLimits limits)
    {
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        ArgumentExceptionHelper.ThrowIfNull(recovered);
        ValidateLimits(limits);

        var logicalBytes = ValidateMutationHeader(mutation, limits);
        ValidateMutationRequiredFields(mutation);
        ValidateRecoveredBinding(mutation, recovered);

        var request = CreateBindingRequest(mutation, recovered, limits);
        Validate(request, limits);
        logicalBytes += ValidateCheckpoint(mutation.Checkpoint, request, limits);
        if (mutation.Checkpoint.SnapshotFormatVersion != mutation.SnapshotFormatVersion)
        {
            throw new ArgumentException("The local snapshot recovery mutation uses a different snapshot format than its checkpoint.", nameof(mutation));
        }

        logicalBytes += ValidatePayload(mutation.OptimisticState, limits);
        ValidateOptimisticPayload(mutation);

        logicalBytes += ValidateDispositions(mutation.OperationDispositions, recovered.PendingOperations, recovered.ReplayOperations, limits);
        ThrowIfLogicalBytesExceeded(logicalBytes, limits.MaximumLogicalBytes, nameof(mutation));
    }

    /// <summary>Validates the local mutation identity and revision fields.</summary>
    /// <param name="mutation">The local mutation.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The local mutation header logical byte count.</returns>
    /// <exception cref="ArgumentException">The mutation header is malformed.</exception>
    private static long ValidateMutationHeader(LocalSnapshotRecoveryMutation mutation, SnapshotRecoveryLimits limits)
    {
        if (mutation.SubscriptionId.Value == Guid.Empty || mutation.ExpectedRevision < 0 || mutation.SnapshotFormatVersion <= 0)
        {
            throw new ArgumentException("The local snapshot recovery mutation header is malformed.", nameof(mutation));
        }

        var logicalBytes = ValidateStreamId(mutation.StreamId, nameof(mutation.StreamId), limits);
        logicalBytes += GuidLogicalBytes + Int64LogicalBytes;
        logicalBytes += ValidateCursor(mutation.ExpectedPreviousCursor, allowNull: true, nameof(mutation.ExpectedPreviousCursor), limits);
        logicalBytes += Int32LogicalBytes;
        return logicalBytes;
    }

    /// <summary>Validates runtime-required mutation references before later binding dereferences them.</summary>
    /// <param name="mutation">The local mutation.</param>
    /// <exception cref="ArgumentException">A required mutation reference is missing.</exception>
    private static void ValidateMutationRequiredFields(LocalSnapshotRecoveryMutation mutation) =>
        _ = mutation.Checkpoint is not null
            && mutation.Checkpoint.ClientState is not null
            && mutation.OptimisticState is not null
            && mutation.OperationDispositions is not null
            ? true
            : throw new ArgumentException("The local snapshot recovery mutation is missing required payload or disposition state.", nameof(mutation));

    /// <summary>Validates caller-supplied limits before any bounded structural checks use them.</summary>
    /// <param name="limits">The configured limits.</param>
    /// <exception cref="ArgumentNullException"><paramref name="limits"/> is null.</exception>
    private static void ValidateLimits(SnapshotRecoveryLimits limits)
    {
        ArgumentExceptionHelper.ThrowIfNull(limits);
        limits.Validate();
    }

    /// <summary>Validates the request identity, cursor, schema, and response capacity fields.</summary>
    /// <param name="request">The request to validate.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The logical byte count for request header fields.</returns>
    /// <exception cref="ArgumentException">The request header is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric request limit or version is invalid.</exception>
    private static long ValidateRequestHeader(RemoteSnapshotRecoveryRequest request, SnapshotRecoveryLimits limits)
    {
        if (request.SubscriptionId.Value == Guid.Empty || request.PendingOperations is null || request.ReplayOperations is null)
        {
            throw new ArgumentException("The snapshot recovery request identity is malformed.", nameof(request));
        }

        var logicalBytes = ValidateStreamId(request.StreamId, nameof(request.StreamId), limits);
        logicalBytes += GuidLogicalBytes;
        logicalBytes += ValidateCursor(request.ExpiredCursor, allowNull: true, nameof(request.ExpiredCursor), limits);
        logicalBytes += ValidateProtocolString(request.ClientStateContractId, nameof(request.ClientStateContractId), limits.MaximumContractUtf8Bytes);
        logicalBytes += Int32LogicalBytes + Int32LogicalBytes + Int64LogicalBytes;
        if (request.ClientStateSchemaVersion <= 0 || request.SnapshotFormatVersion <= 0 || request.MaximumResponseBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Snapshot recovery versions and byte limits must be positive.");
        }

        if (request.MaximumResponseBytes > limits.MaximumLogicalBytes)
        {
            throw new ArgumentException("The requested response byte limit exceeds configured snapshot recovery limits.", nameof(request));
        }

        return logicalBytes;
    }

    /// <summary>Validates every pending and replay operation in one bounded recovery request.</summary>
    /// <param name="operations">The pending operations.</param>
    /// <param name="replayOperations">The replay operations.</param>
    /// <param name="streamId">The expected stream identifier.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The operation logical byte count.</returns>
    /// <exception cref="ArgumentException">An operation is malformed, duplicated, foreign, or over a limit.</exception>
    private static long ValidateOperations(
        IReadOnlyList<SyncOperation> operations,
        IReadOnlyList<SyncOperation> replayOperations,
        StreamId streamId,
        SnapshotRecoveryLimits limits)
    {
        var totalCount = operations.Count + replayOperations.Count;
        if (totalCount > limits.MaximumPendingOperations)
        {
            throw new ArgumentException("The snapshot recovery request exceeds the pending operation limit.", nameof(operations));
        }

        var logicalBytes = Int32LogicalBytes + Int32LogicalBytes;
        HashSet<OperationId> operationIds = [];
        HashSet<long> clientSequences = [];
        logicalBytes += ValidateOperationRole(operations, streamId, limits, operationIds, clientSequences);
        logicalBytes += ValidateOperationRole(replayOperations, streamId, limits, operationIds, clientSequences);

        return logicalBytes;
    }

    /// <summary>Validates one operation role in a bounded recovery request.</summary>
    /// <param name="operations">The role operations.</param>
    /// <param name="streamId">The expected stream identifier.</param>
    /// <param name="limits">The configured limits.</param>
    /// <param name="operationIds">The operation ids already used by earlier roles.</param>
    /// <param name="clientSequences">The client sequences already used by earlier roles.</param>
    /// <returns>The operation role logical byte count.</returns>
    /// <exception cref="ArgumentException">An operation is malformed, duplicated, foreign, or over a limit.</exception>
    private static long ValidateOperationRole(
        IReadOnlyList<SyncOperation> operations,
        StreamId streamId,
        SnapshotRecoveryLimits limits,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences)
    {
        var logicalBytes = 0L;
        for (var index = 0; index < operations.Count; index++)
        {
            var operation = operations[index];
            if (IsMalformedOperation(operation, streamId))
            {
                throw new ArgumentException("A snapshot recovery operation is malformed.", nameof(operations));
            }

            operation.Policy.Validate();
            ValidateOperationUniqueness(operation, operationIds, clientSequences);

            logicalBytes += GuidLogicalBytes;
            logicalBytes += ValidateStreamId(operation.StreamId, nameof(operation.StreamId), limits);
            logicalBytes += Int64LogicalBytes + DateTimeOffsetLogicalBytes + Int32LogicalBytes;
            logicalBytes += ValidatePayload(operation.Payload, limits);
            logicalBytes += ValidateProtocolString(operation.BaseVersion, nameof(operation.BaseVersion), limits.MaximumContractUtf8Bytes, allowNull: true);
            logicalBytes += ValidateMetadata(operation.Metadata, limits);
            logicalBytes += GetOperationPolicyLogicalBytes();
        }

        return logicalBytes;
    }

    /// <summary>Validates that a pending operation has well-formed structural fields for the expected stream.</summary>
    /// <param name="operation">The operation to validate.</param>
    /// <param name="streamId">The expected stream identifier.</param>
    /// <returns>Whether the operation is malformed.</returns>
    private static bool IsMalformedOperation(SyncOperation? operation, StreamId streamId) =>
        operation is null
        || operation.OperationId.Value == Guid.Empty
        || operation.StreamId != streamId
        || operation.ClientSequence <= 0
        || !IsDefined(operation.Type)
        || operation.Payload is null
        || operation.Policy is null;

    /// <summary>Validates that an operation id and client sequence have not appeared earlier in the same request.</summary>
    /// <param name="operation">The operation to index.</param>
    /// <param name="operationIds">The operation id set.</param>
    /// <param name="clientSequences">The client sequence set.</param>
    /// <exception cref="ArgumentException">The operation id or client sequence is duplicated.</exception>
    private static void ValidateOperationUniqueness(
        SyncOperation operation,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences) =>
        _ = operationIds.Add(operation.OperationId) && clientSequences.Add(operation.ClientSequence)
            ? true
            : throw new ArgumentException("Snapshot recovery pending operations contain duplicate identifiers or sequences.", nameof(operation));

    /// <summary>Validates that the local mutation matches the recovered stream fences supplied by the caller.</summary>
    /// <param name="mutation">The mutation to validate.</param>
    /// <param name="recovered">The recovered stream state.</param>
    /// <exception cref="ArgumentException">The mutation does not match recovered state.</exception>
    private static void ValidateRecoveredBinding(LocalSnapshotRecoveryMutation mutation, RecoveredStream recovered) =>
        _ = mutation.SubscriptionId == recovered.SubscriptionId
            && (recovered.Snapshot is null || mutation.StreamId == recovered.Snapshot.StreamId)
            && ProtocolStringsEqual(mutation.ExpectedPreviousCursor, recovered.ServerCursor)
            && mutation.ExpectedRevision == (recovered.Snapshot?.Revision ?? 0)
            ? true
            : throw new ArgumentException("The local snapshot recovery mutation does not match recovered durable state.", nameof(mutation));

    /// <summary>Creates a request-shaped binding context for local mutation validation.</summary>
    /// <param name="mutation">The local mutation.</param>
    /// <param name="recovered">The recovered stream state.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The synthetic request binding context.</returns>
    private static RemoteSnapshotRecoveryRequest CreateBindingRequest(
        LocalSnapshotRecoveryMutation mutation,
        RecoveredStream recovered,
        SnapshotRecoveryLimits limits)
    {
        ValidateRecoveredOperationCounts(recovered, limits);
        ValidateRecoveredOperationStreams(mutation, recovered);

        return new()
        {
            StreamId = mutation.StreamId,
            SubscriptionId = mutation.SubscriptionId,
            ExpiredCursor = mutation.ExpectedPreviousCursor,
            ClientStateContractId = mutation.Checkpoint.ClientState.ContractId,
            ClientStateSchemaVersion = mutation.Checkpoint.ClientState.SchemaVersion,
            SnapshotFormatVersion = mutation.Checkpoint.SnapshotFormatVersion,
            PendingOperations = recovered.PendingOperations,
            ReplayOperations = GetReplayOnlyOperations(recovered.PendingOperations, recovered.ReplayOperations),
            MaximumResponseBytes = limits.MaximumLogicalBytes,
        };
    }

    /// <summary>Validates the optimistic payload against the authoritative checkpoint payload identity.</summary>
    /// <param name="mutation">The local mutation.</param>
    /// <exception cref="ArgumentException">The optimistic payload targets a different contract or schema.</exception>
    private static void ValidateOptimisticPayload(LocalSnapshotRecoveryMutation mutation) =>
        _ = ProtocolStringsEqual(mutation.OptimisticState.ContractId, mutation.Checkpoint.ClientState.ContractId)
            && mutation.OptimisticState.SchemaVersion == mutation.Checkpoint.ClientState.SchemaVersion
            ? true
            : throw new ArgumentException("The optimistic snapshot payload must match the checkpoint client-state contract and schema.", nameof(mutation));

    /// <summary>Validates a checkpoint against the recovery request it answers.</summary>
    /// <param name="checkpoint">The checkpoint to validate.</param>
    /// <param name="request">The original request.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The checkpoint logical byte count.</returns>
    /// <exception cref="ArgumentException">The checkpoint is malformed or not bound to the request.</exception>
    private static long ValidateCheckpoint(
        RemoteSnapshotCheckpoint checkpoint,
        RemoteSnapshotRecoveryRequest request,
        SnapshotRecoveryLimits limits)
    {
        if (checkpoint.ClientState is null)
        {
            throw new ArgumentException("The snapshot checkpoint client-state payload is required.", nameof(checkpoint));
        }

        if (checkpoint.StreamId != request.StreamId || checkpoint.SubscriptionId != request.SubscriptionId)
        {
            throw new ArgumentException("The snapshot checkpoint does not match the recovery request.", nameof(checkpoint));
        }

        if (checkpoint.SnapshotFormatVersion != request.SnapshotFormatVersion)
        {
            throw new ArgumentException("The snapshot checkpoint format does not match the recovery request.", nameof(checkpoint));
        }

        var logicalBytes = ValidateStreamId(checkpoint.StreamId, nameof(checkpoint.StreamId), limits);
        logicalBytes += GuidLogicalBytes;
        logicalBytes += ValidateCursor(checkpoint.FrontierCursor, allowNull: false, nameof(checkpoint.FrontierCursor), limits);
        logicalBytes += ValidateProtocolString(checkpoint.ServerVersion, nameof(checkpoint.ServerVersion), limits.MaximumContractUtf8Bytes);
        logicalBytes += Int32LogicalBytes + DateTimeOffsetLogicalBytes;
        logicalBytes += ValidatePayload(checkpoint.ClientState, limits);
        if (!ProtocolStringsEqual(checkpoint.ClientState.ContractId, request.ClientStateContractId)
            || checkpoint.ClientState.SchemaVersion != request.ClientStateSchemaVersion)
        {
            throw new ArgumentException("The snapshot checkpoint client-state contract does not match the recovery request.", nameof(checkpoint));
        }

        return logicalBytes;
    }

    /// <summary>Validates exact disposition membership and per-disposition result shape.</summary>
    /// <param name="dispositions">The dispositions to validate.</param>
    /// <param name="operations">The pending operations to match.</param>
    /// <param name="replayOperations">The replay operations to match.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The disposition logical byte count.</returns>
    /// <exception cref="ArgumentException">The disposition set is malformed or does not exactly match.</exception>
    private static long ValidateDispositions(
        IReadOnlyList<SnapshotOperationDisposition> dispositions,
        IReadOnlyList<SyncOperation> operations,
        IReadOnlyList<SyncOperation> replayOperations,
        SnapshotRecoveryLimits limits)
    {
        var operationIds = CreateOperationIdSet(operations);
        var replayOnlyIds = CreateReplayOnlyOperationIdSet(operationIds, replayOperations);
        ValidateDispositionCollections(dispositions, operationIds.Count + replayOnlyIds.Count, limits);
        var dispositionCount = dispositions.Count;
        var logicalBytes = Int32LogicalBytes;
        Dictionary<OperationId, int> resultCounts = [with(capacity: dispositionCount)];
        for (var index = 0; index < dispositionCount; index++)
        {
            var disposition = dispositions[index];
            ValidateDispositionShape(disposition);
            ValidateDispositionBinding(disposition, operationIds, replayOnlyIds);
            logicalBytes += ValidateDispositionResult(disposition, limits);
            logicalBytes += GuidLogicalBytes + Int32LogicalBytes;
            if (resultCounts.ContainsKey(disposition.OperationId))
            {
                throw new ArgumentException("Snapshot recovery dispositions contain a duplicate operation.", nameof(dispositions));
            }

            resultCounts.Add(disposition.OperationId, 1);
        }

        return logicalBytes;
    }

    /// <summary>Validates disposition collection presence and cardinality.</summary>
    /// <param name="dispositions">The dispositions to validate.</param>
    /// <param name="operationCount">The operation count to match.</param>
    /// <param name="limits">The configured limits.</param>
    /// <exception cref="ArgumentException">The collections are missing or do not exactly match.</exception>
    private static void ValidateDispositionCollections(
        IReadOnlyList<SnapshotOperationDisposition> dispositions,
        int operationCount,
        SnapshotRecoveryLimits limits) =>
        _ = dispositions.Count == operationCount && dispositions.Count <= limits.MaximumPendingOperations
            ? true
            : throw new ArgumentException("Snapshot recovery dispositions must exactly match requested operations.", nameof(dispositions));

    /// <summary>Creates the set of pending operation ids used for disposition membership validation.</summary>
    /// <param name="operations">The pending operations.</param>
    /// <returns>The pending operation id set.</returns>
    /// <exception cref="ArgumentException">A pending operation id is duplicated.</exception>
    private static HashSet<OperationId> CreateOperationIdSet(IReadOnlyList<SyncOperation> operations)
    {
        HashSet<OperationId> operationIds = [];
        for (var index = 0; index < operations.Count; index++)
        {
            if (!operationIds.Add(operations[index].OperationId))
            {
                throw new ArgumentException("Snapshot recovery pending operations contain duplicate operation ids.", nameof(operations));
            }
        }

        return operationIds;
    }

    /// <summary>Creates the set of replay-only operation ids after filtering ids already present as pending.</summary>
    /// <param name="operationIds">The pending operation ids.</param>
    /// <param name="replayOperations">The replay operations.</param>
    /// <returns>The replay-only operation id set.</returns>
    /// <exception cref="ArgumentException">A replay-only operation id is duplicated.</exception>
    private static HashSet<OperationId> CreateReplayOnlyOperationIdSet(
        HashSet<OperationId> operationIds,
        IReadOnlyList<SyncOperation> replayOperations)
    {
        HashSet<OperationId> replayOnlyIds = [];
        for (var index = 0; index < replayOperations.Count; index++)
        {
            var operationId = replayOperations[index].OperationId;
            if (operationIds.Contains(operationId))
            {
                continue;
            }

            if (!replayOnlyIds.Add(operationId))
            {
                throw new ArgumentException("Snapshot recovery replay operations contain duplicate operation ids.", nameof(replayOperations));
            }
        }

        return replayOnlyIds;
    }

    /// <summary>Gets the replay-only operations after filtering ids already present as pending.</summary>
    /// <param name="operations">The pending operations.</param>
    /// <param name="replayOperations">The recovered replay operations.</param>
    /// <returns>The replay-only operations.</returns>
    private static SyncOperation[] GetReplayOnlyOperations(
        IReadOnlyList<SyncOperation> operations,
        IReadOnlyList<SyncOperation> replayOperations)
    {
        var operationIds = CreateOperationIdSet(operations);
        var replayOnlyIds = CreateReplayOnlyOperationIdSet(operationIds, replayOperations);
        var replayOnlyOperations = new SyncOperation[replayOnlyIds.Count];
        var replayOnlyIndex = 0;
        for (var index = 0; index < replayOperations.Count; index++)
        {
            var operation = replayOperations[index];
            if (!replayOnlyIds.Contains(operation.OperationId))
            {
                continue;
            }

            replayOnlyOperations[replayOnlyIndex] = operation;
            replayOnlyIndex++;
        }

        return replayOnlyOperations;
    }

    /// <summary>Validates one disposition against the pending or replay-only operation role that owns it.</summary>
    /// <param name="disposition">The disposition to validate.</param>
    /// <param name="operationIds">The pending operation ids.</param>
    /// <param name="replayOnlyIds">The replay-only operation ids.</param>
    /// <exception cref="ArgumentException">The disposition is omitted, unknown, or contradicts replay-only accepted proof.</exception>
    private static void ValidateDispositionBinding(
        SnapshotOperationDisposition disposition,
        HashSet<OperationId> operationIds,
        HashSet<OperationId> replayOnlyIds)
    {
        if (operationIds.Contains(disposition.OperationId))
        {
            return;
        }

        if (!replayOnlyIds.Contains(disposition.OperationId))
        {
            throw new ArgumentException("Snapshot recovery dispositions contain an unrequested operation.", nameof(disposition));
        }

        if (disposition.Kind != SnapshotOperationDispositionKind.IncludedAccepted)
        {
            throw new ArgumentException("Replay-only snapshot recovery dispositions must prove accepted inclusion.", nameof(disposition));
        }

        var result = disposition.Result;
        if (result is null || result.Kind != OperationResultKind.Accepted)
        {
            throw new ArgumentException("Replay-only snapshot recovery dispositions must prove accepted inclusion.", nameof(disposition));
        }
    }

    /// <summary>Validates one disposition's structural fields.</summary>
    /// <param name="disposition">The disposition to validate.</param>
    /// <exception cref="ArgumentException">The disposition is malformed.</exception>
    private static void ValidateDispositionShape(SnapshotOperationDisposition disposition) =>
        _ = disposition is not null && disposition.OperationId.Value != Guid.Empty && IsDefined(disposition.Kind)
            ? true
            : throw new ArgumentException("A snapshot recovery disposition is malformed.", nameof(disposition));

    /// <summary>Validates the operation result allowed for one disposition kind.</summary>
    /// <param name="disposition">The disposition to validate.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The disposition result logical byte count.</returns>
    /// <exception cref="ArgumentException">The disposition and result combination is invalid.</exception>
    private static long ValidateDispositionResult(SnapshotOperationDisposition disposition, SnapshotRecoveryLimits limits)
    {
        var result = disposition.Result;
        if (disposition.Kind == SnapshotOperationDispositionKind.Unknown)
        {
            if (result is not null)
            {
                throw new ArgumentException("Unknown snapshot recovery dispositions must not carry a server result.", nameof(disposition));
            }

            return 0;
        }

        if (result is null || result.OperationId != disposition.OperationId)
        {
            throw new ArgumentException("Proven snapshot recovery dispositions must carry a matching server result.", nameof(disposition));
        }

        const long logicalBytes = GuidLogicalBytes + Int32LogicalBytes;

        if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted
            && result.Kind is not (OperationResultKind.Accepted or OperationResultKind.Conflict))
        {
            throw new ArgumentException("Included snapshot recovery dispositions require an accepted or conflict result.", nameof(disposition));
        }

        if (disposition.Kind != SnapshotOperationDispositionKind.TerminalRejected || result.Kind == OperationResultKind.Rejected)
        {
            return logicalBytes
                + ValidateReasonCode(result.ReasonCode, limits)
                + ValidateProtocolString(result.ServerVersion, nameof(result.ServerVersion), limits.MaximumContractUtf8Bytes, allowNull: true);
        }

        throw new ArgumentException("Terminal snapshot recovery dispositions require a rejected result.", nameof(disposition));
    }

    /// <summary>Validates a payload envelope and returns its logical byte contribution.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The payload logical byte count.</returns>
    /// <exception cref="ArgumentException">The payload is malformed or exceeds a limit.</exception>
    private static long ValidatePayload(PayloadEnvelope payload, SnapshotRecoveryLimits limits)
    {
        var logicalBytes = Int32LogicalBytes + Int64LogicalBytes;
        logicalBytes += ValidateProtocolString(payload.ContractId, nameof(payload.ContractId), limits.MaximumContractUtf8Bytes);
        logicalBytes += ValidateProtocolString(payload.ContentType, nameof(payload.ContentType), limits.MaximumContractUtf8Bytes);
        logicalBytes += ValidateProtocolString(payload.PayloadHash, nameof(payload.PayloadHash), limits.MaximumContractUtf8Bytes);
        if (payload.SchemaVersion > 0 && payload.PayloadLength <= limits.MaximumPayloadBytes)
        {
            return logicalBytes + payload.PayloadLength;
        }

        throw new ArgumentException("A snapshot recovery payload is malformed or exceeds the payload limit.", nameof(payload));
    }

    /// <summary>Validates operation metadata and returns its logical byte contribution.</summary>
    /// <param name="metadata">The metadata to validate.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The metadata logical byte count.</returns>
    /// <exception cref="ArgumentException">The metadata is malformed or exceeds a limit.</exception>
    private static long ValidateMetadata(IReadOnlyDictionary<string, string> metadata, SnapshotRecoveryLimits limits)
    {
        if (metadata is null || metadata.Count > limits.MaximumMetadataEntries)
        {
            throw new ArgumentException("Snapshot recovery operation metadata is missing or exceeds the metadata entry limit.", nameof(metadata));
        }

        var logicalBytes = Int32LogicalBytes;
        foreach (var pair in metadata)
        {
            logicalBytes += ValidateProtocolString(pair.Key, "metadata key", limits.MaximumMetadataBytes);
            logicalBytes += ValidateProtocolString(pair.Value, "metadata value", limits.MaximumMetadataBytes);
        }

        if (logicalBytes > limits.MaximumMetadataBytes)
        {
            throw new ArgumentException("Snapshot recovery operation metadata exceeds the metadata byte limit.", nameof(metadata));
        }

        return logicalBytes;
    }

    /// <summary>Validates a stream identity and returns its logical byte contribution.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The stream identifier logical byte count.</returns>
    /// <exception cref="ArgumentException">The stream identifier is malformed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ValidateStreamId(StreamId streamId, string parameterName, SnapshotRecoveryLimits limits) =>
        ValidateProtocolString(streamId.Value, parameterName, limits.MaximumStreamIdUtf8Bytes);

    /// <summary>Validates recovered pending and replay operation counts before proportional allocations or scans.</summary>
    /// <param name="recovered">The recovered stream state.</param>
    /// <param name="limits">The configured limits.</param>
    /// <exception cref="ArgumentException">A recovered operation role exceeds the configured operation limit.</exception>
    private static void ValidateRecoveredOperationCounts(RecoveredStream recovered, SnapshotRecoveryLimits limits)
    {
        if (recovered.PendingOperations.Count > limits.MaximumPendingOperations || recovered.ReplayOperations.Count > limits.MaximumPendingOperations)
        {
            throw new ArgumentException("Recovered snapshot recovery operations exceed the pending operation limit.", nameof(recovered));
        }
    }

    /// <summary>Validates recovered pending and replay operation streams against the mutation stream.</summary>
    /// <param name="mutation">The local mutation.</param>
    /// <param name="recovered">The recovered stream state.</param>
    /// <exception cref="ArgumentException">A recovered operation belongs to another stream.</exception>
    private static void ValidateRecoveredOperationStreams(LocalSnapshotRecoveryMutation mutation, RecoveredStream recovered)
    {
        for (var index = 0; index < recovered.PendingOperations.Count; index++)
        {
            var operation = recovered.PendingOperations[index];
            if (operation is null || operation.StreamId != mutation.StreamId)
            {
                throw new ArgumentException("Recovered pending operations must belong to the recovered stream.", nameof(recovered));
            }
        }

        for (var index = 0; index < recovered.ReplayOperations.Count; index++)
        {
            var operation = recovered.ReplayOperations[index];
            if (operation is null || operation.StreamId != mutation.StreamId)
            {
                throw new ArgumentException("Recovered replay operations must belong to the recovered stream.", nameof(recovered));
            }
        }
    }

    /// <summary>Returns the logical byte contribution for an operation policy.</summary>
    /// <returns>The operation policy logical byte count.</returns>
    private static long GetOperationPolicyLogicalBytes() =>
        Int32LogicalBytes + Int32LogicalBytes + Int32LogicalBytes + Int32LogicalBytes;

    /// <summary>Validates an optional stable reason code and returns its logical byte contribution.</summary>
    /// <param name="reasonCode">The reason code to validate.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The reason code logical byte count.</returns>
    /// <exception cref="ArgumentException">The reason code is malformed or exceeds a limit.</exception>
    private static long ValidateReasonCode(string? reasonCode, SnapshotRecoveryLimits limits)
    {
        var bytes = ValidateProtocolString(reasonCode, nameof(reasonCode), limits.MaximumReasonCodeUtf8Bytes, allowNull: true);
        if (reasonCode is null)
        {
            return bytes;
        }

        for (var index = 0; index < reasonCode.Length; index++)
        {
            var character = reasonCode[index];
            if (!char.IsLetterOrDigit(character) && character != '.' && character != '_' && character != '-')
            {
                throw new ArgumentException("Snapshot recovery reason codes must be stable protocol identifiers.", nameof(reasonCode));
            }
        }

        return bytes;
    }

    /// <summary>Validates an optional or required cursor string.</summary>
    /// <param name="cursor">The cursor to validate.</param>
    /// <param name="allowNull">Whether null is accepted.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="limits">The configured limits.</param>
    /// <returns>The cursor logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ValidateCursor(string? cursor, bool allowNull, string parameterName, SnapshotRecoveryLimits limits) =>
        ValidateProtocolString(cursor, parameterName, limits.MaximumCursorUtf8Bytes, allowNull);

    /// <summary>Validates a bounded protocol string and returns its UTF-8 byte count.</summary>
    /// <param name="value">The protocol string.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="maximumUtf8Bytes">The maximum UTF-8 byte count.</param>
    /// <param name="allowNull">Whether null is accepted.</param>
    /// <returns>The UTF-8 byte count.</returns>
    /// <exception cref="ArgumentException">The value is malformed or over the byte limit.</exception>
    private static long ValidateProtocolString(
        string? value,
        string parameterName,
        int maximumUtf8Bytes,
        bool allowNull = false)
    {
        if (value is null)
        {
            if (allowNull)
            {
                return 0;
            }

            throw new ArgumentException("A snapshot recovery protocol string is required.", parameterName);
        }

        if (value.Length == 0)
        {
            throw new ArgumentException("A snapshot recovery protocol string must be non-empty.", parameterName);
        }

        var bytes = StrictUtf8.GetByteCount(value);
        if (bytes > maximumUtf8Bytes)
        {
            throw new ArgumentException("A snapshot recovery protocol string exceeds its byte limit.", parameterName);
        }

        return bytes;
    }

    /// <summary>Validates a remote snapshot recovery status enum value.</summary>
    /// <param name="status">The status to validate.</param>
    /// <exception cref="ArgumentException">The status is undefined.</exception>
    private static void ValidateRecoveryStatus(RemoteSnapshotRecoveryStatus status) =>
        _ = status switch
        {
            RemoteSnapshotRecoveryStatus.Recovered
            or RemoteSnapshotRecoveryStatus.UnsupportedProjection
            or RemoteSnapshotRecoveryStatus.RetentionExpired
            or RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation
            or RemoteSnapshotRecoveryStatus.ValidationRejected
            or RemoteSnapshotRecoveryStatus.CapacityExceeded
            or RemoteSnapshotRecoveryStatus.RetryableConcurrentChange => true,
            _ => throw new ArgumentException("Snapshot recovery status must be a defined value.", nameof(status)),
        };

    /// <summary>Determines whether a disposition kind is defined.</summary>
    /// <param name="kind">The disposition kind.</param>
    /// <returns>Whether the kind is defined.</returns>
    private static bool IsDefined(SnapshotOperationDispositionKind kind) => kind is
        SnapshotOperationDispositionKind.IncludedAccepted
        or SnapshotOperationDispositionKind.TerminalRejected
        or SnapshotOperationDispositionKind.Unknown;

    /// <summary>Determines whether a synchronization operation type is defined.</summary>
    /// <param name="type">The operation type.</param>
    /// <returns>Whether the type is defined.</returns>
    private static bool IsDefined(SyncOperationType type) => type is
        SyncOperationType.Append
        or SyncOperationType.Update
        or SyncOperationType.Delete
        or SyncOperationType.Custom;

    /// <summary>Compares optional protocol strings ordinally.</summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right value.</param>
    /// <returns>Whether both values are equal.</returns>
    private static bool ProtocolStringsEqual(string? left, string? right) =>
        left is null || right is null
            ? left is null && right is null
            : StringComparer.Ordinal.Equals(left, right);

    /// <summary>Throws when logical byte accounting exceeds the configured limit.</summary>
    /// <param name="logicalBytes">The counted logical bytes.</param>
    /// <param name="maximumLogicalBytes">The maximum logical bytes.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException"><paramref name="logicalBytes"/> exceeds <paramref name="maximumLogicalBytes"/>.</exception>
    private static void ThrowIfLogicalBytesExceeded(long logicalBytes, long maximumLogicalBytes, string parameterName) =>
        _ = logicalBytes <= maximumLogicalBytes
            ? true
            : throw new ArgumentException("Snapshot recovery logical byte accounting exceeds the configured limit.", parameterName);
}
