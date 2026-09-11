// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates remote synchronization results before they enter local store transactions.</summary>
public static class SyncBatchValidator
{
    /// <summary>The operation result count for one result per operation.</summary>
    private const int ResultCountPerOperation = 1;

    /// <summary>Validates that a remote result exactly matches the pushed synchronization batch.</summary>
    /// <param name="batch">The pushed synchronization batch.</param>
    /// <param name="result">The remote synchronization result.</param>
    /// <exception cref="SyncBatchValidationException">The result does not exactly match the pushed batch.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Validate(SyncBatch batch, RemoteSyncResult result) =>
        Validate(batch, result, OperationPolicy.MinimumPriority, OperationPolicy.MaximumPriority);

    /// <summary>Validates that a remote result exactly matches the pushed synchronization batch.</summary>
    /// <param name="batch">The pushed synchronization batch.</param>
    /// <param name="result">The remote synchronization result.</param>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <exception cref="SyncBatchValidationException">The result does not exactly match the pushed batch.</exception>
    public static void Validate(SyncBatch batch, RemoteSyncResult result, int minimumPriority, int maximumPriority)
    {
        if (batch is null || result is null || batch.BatchId == Guid.Empty || batch.Operations.Count == 0)
        {
            throw Create(SyncBatchValidationError.MalformedBatch, "The synchronization batch and result must be present and well formed.");
        }

        if (batch.BatchId != result.BatchId)
        {
            throw Create(SyncBatchValidationError.MismatchingBatchId, "The synchronization result batch identifier does not match the pushed batch.");
        }

        if (result.RetryAfter < TimeSpan.Zero)
        {
            throw Create(SyncBatchValidationError.MalformedOperationResult, "The synchronization result contains a negative retry delay.");
        }

        var expectedOperations = CreateExpectedOperationSet(batch, minimumPriority, maximumPriority);
        var resultCounts = CountResultOperations(result);
        ValidateResultMembership(expectedOperations, resultCounts);
    }

    /// <summary>Creates the expected operation set from a batch.</summary>
    /// <param name="batch">The pushed synchronization batch.</param>
    /// <param name="minimumPriority">The inclusive minimum accepted priority.</param>
    /// <param name="maximumPriority">The inclusive maximum accepted priority.</param>
    /// <returns>The expected operation identifiers.</returns>
    private static HashSet<OperationId> CreateExpectedOperationSet(
        SyncBatch batch,
        int minimumPriority,
        int maximumPriority)
    {
        HashSet<OperationId> expectedOperations = new();
        HashSet<long> clientSequences = new();
        StreamId? streamId = null;
        var previousSequence = 0L;
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            var operation = batch.Operations[index];
            if (IsMalformed(operation))
            {
                throw Create(SyncBatchValidationError.MalformedBatch, "The synchronization batch contains a malformed operation.");
            }

            operation.Policy.Validate(minimumPriority, maximumPriority);

            if (IsMixedStream(ref streamId, operation.StreamId))
            {
                throw Create(SyncBatchValidationError.MixedStreams, "The synchronization batch contains operations for multiple streams.");
            }

            if (!expectedOperations.Add(operation.OperationId))
            {
                throw Create(SyncBatchValidationError.DuplicateOperation, "The synchronization batch contains a duplicate operation identifier.");
            }

            if (!clientSequences.Add(operation.ClientSequence))
            {
                throw Create(SyncBatchValidationError.DuplicateClientSequence, "The synchronization batch contains a duplicate client sequence.");
            }

            if (operation.ClientSequence < previousSequence)
            {
                throw Create(SyncBatchValidationError.MalformedBatch, "The synchronization batch is not in client sequence order.");
            }

            previousSequence = operation.ClientSequence;
        }

        return expectedOperations;
    }

    /// <summary>Determines whether a batch operation is malformed.</summary>
    /// <param name="operation">The operation to inspect.</param>
    /// <returns>Whether the operation is malformed.</returns>
    private static bool IsMalformed(SyncOperation? operation) =>
        operation is null
        || operation.OperationId.Value == Guid.Empty
        || operation.StreamId.Value is null
        || operation.ClientSequence <= 0
        || !IsDefined(operation.Type)
        || operation.Payload is null
        || operation.Policy is null;

    /// <summary>Determines whether the operation stream differs from the batch stream.</summary>
    /// <param name="batchStreamId">The batch stream identifier discovered so far.</param>
    /// <param name="operationStreamId">The operation stream identifier.</param>
    /// <returns>Whether the operation belongs to a different stream.</returns>
    private static bool IsMixedStream(ref StreamId? batchStreamId, StreamId operationStreamId)
    {
        if (batchStreamId is not null)
        {
            return batchStreamId.Value != operationStreamId;
        }

        batchStreamId = operationStreamId;
        return false;
    }

    /// <summary>Counts operation results by operation identifier.</summary>
    /// <param name="result">The remote synchronization result.</param>
    /// <returns>The result count by operation identifier.</returns>
    private static Dictionary<OperationId, int> CountResultOperations(RemoteSyncResult result)
    {
        Dictionary<OperationId, int> resultCounts = new();
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var operationResult = result.Operations[index];
            if (operationResult is null || operationResult.OperationId.Value == Guid.Empty || !IsDefined(operationResult.Kind))
            {
                throw Create(SyncBatchValidationError.MalformedOperationResult, "The synchronization result contains a malformed operation result.");
            }

            if (!resultCounts.TryGetValue(operationResult.OperationId, out var count))
            {
                resultCounts.Add(operationResult.OperationId, ResultCountPerOperation);
                continue;
            }

            resultCounts[operationResult.OperationId] = count + ResultCountPerOperation;
        }

        return resultCounts;
    }

    /// <summary>Validates that result membership exactly matches the expected operation set.</summary>
    /// <param name="expectedOperations">The expected operation identifiers.</param>
    /// <param name="resultCounts">The result count by operation identifier.</param>
    private static void ValidateResultMembership(
        HashSet<OperationId> expectedOperations,
        Dictionary<OperationId, int> resultCounts)
    {
        foreach (var pair in resultCounts)
        {
            if (pair.Value != ResultCountPerOperation)
            {
                throw Create(SyncBatchValidationError.DuplicateOperationResult, "The synchronization result contains a duplicate operation result.");
            }

            if (!expectedOperations.Contains(pair.Key))
            {
                throw Create(SyncBatchValidationError.UnknownOperationResult, "The synchronization result contains an unknown operation result.");
            }
        }

        if (resultCounts.Count == expectedOperations.Count)
        {
            return;
        }

        throw Create(SyncBatchValidationError.OmittedOperationResult, "The synchronization result omitted one or more operation results.");
    }

    /// <summary>Determines whether an operation result kind is defined.</summary>
    /// <param name="kind">The operation result kind.</param>
    /// <returns><see langword="true"/> when the kind is defined; otherwise, <see langword="false"/>.</returns>
    private static bool IsDefined(OperationResultKind kind) => kind is
        OperationResultKind.Accepted or
        OperationResultKind.Conflict or
        OperationResultKind.Rejected or
        OperationResultKind.Retryable;

    /// <summary>Determines whether an operation type is defined.</summary>
    /// <param name="type">The operation type.</param>
    /// <returns><see langword="true"/> when the type is defined; otherwise, <see langword="false"/>.</returns>
    private static bool IsDefined(SyncOperationType type) => type is
        SyncOperationType.Append or
        SyncOperationType.Update or
        SyncOperationType.Delete;

    /// <summary>Creates a synchronization batch validation exception.</summary>
    /// <param name="error">The validation error.</param>
    /// <param name="message">The validation message.</param>
    /// <returns>The validation exception.</returns>
    private static SyncBatchValidationException Create(SyncBatchValidationError error, string message) => new(error, message);
}
