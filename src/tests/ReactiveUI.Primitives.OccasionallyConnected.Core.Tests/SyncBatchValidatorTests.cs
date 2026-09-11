// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncBatchValidator"/>.</summary>
public sealed class SyncBatchValidatorTests
{
    /// <summary>The expected result count in the complete matching batch test.</summary>
    private const int CompleteOperationResultCount = 2;

    /// <summary>An operation result kind value that is outside the defined enum range.</summary>
    private const int UndefinedOperationResultKindValue = 42;

    /// <summary>An operation type value that is outside the defined enum range.</summary>
    private const int UndefinedOperationTypeValue = 42;

    /// <summary>The first client sequence used in representative operations.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The second client sequence used in representative operations.</summary>
    private const int SecondClientSequence = 2;

    /// <summary>The stream name used in representative operations.</summary>
    private const string TemperatureStream = "sensor/temperature";

    /// <summary>The alternate stream name used in mixed stream tests.</summary>
    private const string HumidityStream = "sensor/humidity";

    /// <summary>Verifies already-terminal operations may leave gaps in the pending sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsIncreasingSequencesWithTerminalGaps()
    {
        const long firstPendingSequence = 41;
        const long secondPendingSequence = 43;
        var first = CreateOperation(OperationId.New(), firstPendingSequence, new(TemperatureStream));
        var second = CreateOperation(OperationId.New(), secondPendingSequence, new(TemperatureStream));
        var batch = new SyncBatch(Guid.NewGuid(), [first, second]);

        SyncBatchValidator.Validate(batch, CreateCompleteResult(batch));

        await Assert.That(batch.Operations[0].ClientSequence).IsEqualTo(firstPendingSequence);
        await Assert.That(batch.Operations[1].ClientSequence).IsEqualTo(secondPendingSequence);
    }

    /// <summary>Verifies malformed persisted operations cannot reach result application.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedPersistedOperationFields()
    {
        var operation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream));
        SyncOperation[] malformed =
        [
            operation with { StreamId = default },
            operation with { ClientSequence = 0 },
            operation with { Payload = null! },
            operation with { Policy = null! },
        ];

        foreach (var candidate in malformed)
        {
            var batch = new SyncBatch(Guid.NewGuid(), [candidate]);
            await AssertValidationError(() => SyncBatchValidator.Validate(batch, CreateCompleteResult(batch)), SyncBatchValidationError.MalformedBatch);
        }
    }

    /// <summary>Verifies an empty correlation identifier cannot be accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsEmptyBatchIdentifier()
    {
        var batch = new SyncBatch(Guid.Empty, CreateBatch(OperationId.New()).Operations);

        await AssertValidationError(() => SyncBatchValidator.Validate(batch, CreateCompleteResult(batch)), SyncBatchValidationError.MalformedBatch);
    }

    /// <summary>Verifies a default identifier in the result cannot acknowledge a real operation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsEmptyResultOperationIdentifier()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(batch.BatchId, [new(default, OperationResultKind.Accepted, null, null)], null, null);

        await AssertValidationError(() => SyncBatchValidator.Validate(batch, result), SyncBatchValidationError.MalformedOperationResult);
    }

    /// <summary>Verifies priority cannot reorder mutations within a stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsDescendingClientSequences()
    {
        var first = CreateOperation(OperationId.New(), SecondClientSequence, new(TemperatureStream));
        var second = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream));
        var batch = new SyncBatch(Guid.NewGuid(), [first, second]);

        await Assert.That(() => SyncBatchValidator.Validate(batch, CreateCompleteResult(batch)))
            .ThrowsExactly<SyncBatchValidationException>();
    }

    /// <summary>Verifies invalid retry hints cannot enter a durable result transaction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNegativeRetryHint()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(batch.BatchId, CreateCompleteResult(batch).Operations, null, -TimeSpan.FromTicks(1));

        await Assert.That(() => SyncBatchValidator.Validate(batch, result))
            .ThrowsExactly<SyncBatchValidationException>();
    }

    /// <summary>Verifies complete results for the batch operations are accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsCompleteMatchingOperationResults()
    {
        var firstOperation = OperationId.New();
        var secondOperation = OperationId.New();
        var batch = CreateBatch(firstOperation, secondOperation);
        var result = new RemoteSyncResult(
            batch.BatchId,
            [
                new OperationSyncResult(secondOperation, OperationResultKind.Retryable, "OC.Retry", null),
                new OperationSyncResult(firstOperation, OperationResultKind.Accepted, null, "v1"),
            ],
            "cursor-1",
            TimeSpan.FromSeconds(1));

        SyncBatchValidator.Validate(batch, result);

        await Assert.That(result.Operations).Count().IsEqualTo(CompleteOperationResultCount);
    }

    /// <summary>Verifies a mismatched batch identity is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMismatchingBatchId()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(
            Guid.NewGuid(),
            [new OperationSyncResult(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, "v1")],
            null,
            null);

        var action = () => SyncBatchValidator.Validate(batch, result);

        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(SyncBatchValidationError.MismatchingBatchId);
    }

    /// <summary>Verifies duplicate operation results are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsDuplicateOperationResults()
    {
        var operationId = OperationId.New();
        var batch = CreateBatch(operationId);
        var result = new RemoteSyncResult(
            batch.BatchId,
            [
                new OperationSyncResult(operationId, OperationResultKind.Accepted, null, "v1"),
                new OperationSyncResult(operationId, OperationResultKind.Accepted, null, "v1"),
            ],
            null,
            null);

        var action = () => SyncBatchValidator.Validate(batch, result);

        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(SyncBatchValidationError.DuplicateOperationResult);
    }

    /// <summary>Verifies missing operation results are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsOmittedOperationResults()
    {
        var firstOperation = OperationId.New();
        var secondOperation = OperationId.New();
        var batch = CreateBatch(firstOperation, secondOperation);
        var result = new RemoteSyncResult(
            batch.BatchId,
            [new OperationSyncResult(firstOperation, OperationResultKind.Accepted, null, "v1")],
            null,
            null);

        var action = () => SyncBatchValidator.Validate(batch, result);

        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(SyncBatchValidationError.OmittedOperationResult);
    }

    /// <summary>Verifies unknown operation results are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsUnknownOperationResults()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(
            batch.BatchId,
            [
                new OperationSyncResult(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, "v1"),
                new OperationSyncResult(OperationId.New(), OperationResultKind.Accepted, null, "v2"),
            ],
            null,
            null);

        var action = () => SyncBatchValidator.Validate(batch, result);

        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(SyncBatchValidationError.UnknownOperationResult);
    }

    /// <summary>Verifies malformed operation results are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedOperationResults()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(
            batch.BatchId,
            [new OperationSyncResult(batch.Operations[0].OperationId, (OperationResultKind)UndefinedOperationResultKindValue, null, null)],
            null,
            null);

        var action = () => SyncBatchValidator.Validate(batch, result);

        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(SyncBatchValidationError.MalformedOperationResult);
    }

    /// <summary>Verifies malformed batches are rejected before result application.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedBatches()
    {
        var validResult = new RemoteSyncResult(Guid.NewGuid(), [], null, null);
        var emptyBatch = new SyncBatch(validResult.BatchId, []);
        var defaultOperation = CreateBatch(default(OperationId));
        var nullOperation = new SyncBatch(Guid.NewGuid(), [null!]);
        var duplicateOperation = CreateBatch(OperationId.New());
        duplicateOperation = new(
            duplicateOperation.BatchId,
            [duplicateOperation.Operations[0], duplicateOperation.Operations[0]]);

        await AssertValidationError(() => SyncBatchValidator.Validate(null!, validResult), SyncBatchValidationError.MalformedBatch);
        await AssertValidationError(() => SyncBatchValidator.Validate(emptyBatch, null!), SyncBatchValidationError.MalformedBatch);
        await AssertValidationError(() => SyncBatchValidator.Validate(emptyBatch, validResult), SyncBatchValidationError.MalformedBatch);
        await AssertValidationError(() => SyncBatchValidator.Validate(defaultOperation, new(defaultOperation.BatchId, [], null, null)), SyncBatchValidationError.MalformedBatch);
        await AssertValidationError(() => SyncBatchValidator.Validate(nullOperation, new(nullOperation.BatchId, [], null, null)), SyncBatchValidationError.MalformedBatch);
        await AssertValidationError(() => SyncBatchValidator.Validate(duplicateOperation, new(duplicateOperation.BatchId, [], null, null)), SyncBatchValidationError.DuplicateOperation);
    }

    /// <summary>Verifies batches cannot mix operations from different streams.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMixedStreamBatch()
    {
        var firstOperation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream));
        var secondOperation = CreateOperation(OperationId.New(), SecondClientSequence, new(HumidityStream));
        var batch = new SyncBatch(Guid.NewGuid(), [firstOperation, secondOperation]);
        var result = CreateCompleteResult(batch);

        await AssertValidationError(() => SyncBatchValidator.Validate(batch, result), SyncBatchValidationError.MixedStreams);
    }

    /// <summary>Verifies batches cannot contain duplicate client sequences for the same stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsDuplicateClientSequences()
    {
        var firstOperation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream));
        var secondOperation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream));
        var batch = new SyncBatch(Guid.NewGuid(), [firstOperation, secondOperation]);
        var result = CreateCompleteResult(batch);

        await AssertValidationError(() => SyncBatchValidator.Validate(batch, result), SyncBatchValidationError.DuplicateClientSequence);
    }

    /// <summary>Verifies batches cannot contain operations with malformed persisted policies.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsMalformedOperationPolicy()
    {
        var operation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream)) with
        {
            Policy = new(DeliveryGuarantee.ExactlyOnce, OperationDurability.Volatile, 0, ConflictPolicy.Merge),
        };
        var batch = new SyncBatch(Guid.NewGuid(), [operation]);
        var result = CreateCompleteResult(batch);

        var action = () => SyncBatchValidator.Validate(batch, result);

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies configured priority bounds are used when validating persisted policies.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsConfiguredPriorityBounds()
    {
        var operation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream)) with
        {
            Policy = OperationPolicy.Default with { Priority = OperationPolicy.MaximumPriority + FirstClientSequence },
        };
        var batch = new SyncBatch(Guid.NewGuid(), [operation]);
        var result = CreateCompleteResult(batch);

        SyncBatchValidator.Validate(batch, result, OperationPolicy.MinimumPriority, OperationPolicy.MaximumPriority + FirstClientSequence);

        await Assert.That(result.Operations).Count().IsEqualTo(FirstClientSequence);
    }

    /// <summary>Verifies operations with undefined operation types are rejected as malformed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsUndefinedOperationType()
    {
        var operation = CreateOperation(OperationId.New(), FirstClientSequence, new(TemperatureStream)) with
        {
            Type = (SyncOperationType)UndefinedOperationTypeValue,
        };
        var batch = new SyncBatch(Guid.NewGuid(), [operation]);
        var result = CreateCompleteResult(batch);

        await AssertValidationError(() => SyncBatchValidator.Validate(batch, result), SyncBatchValidationError.MalformedBatch);
    }

    /// <summary>Verifies null result entries are rejected as malformed operation results.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateRejectsNullOperationResult()
    {
        var batch = CreateBatch(OperationId.New());
        var result = new RemoteSyncResult(batch.BatchId, [null!], null, null);

        await AssertValidationError(
            () => SyncBatchValidator.Validate(batch, result),
            SyncBatchValidationError.MalformedOperationResult);
    }

    /// <summary>Creates a batch containing representative operations.</summary>
    /// <param name="operationIds">The operation identifiers to include.</param>
    /// <returns>A synchronization batch.</returns>
    private static SyncBatch CreateBatch(params OperationId[] operationIds)
    {
        var operations = new SyncOperation[operationIds.Length];
        for (var index = 0; index < operationIds.Length; index++)
        {
            operations[index] = CreateOperation(operationIds[index], index + FirstClientSequence, new(TemperatureStream));
        }

        return new(Guid.NewGuid(), operations);
    }

    /// <summary>Creates a representative operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation(OperationId operationId, long clientSequence, StreamId streamId) => new()
    {
        OperationId = operationId,
        StreamId = streamId,
        ClientSequence = clientSequence,
        TimestampUtc = DateTimeOffset.UnixEpoch.AddSeconds(clientSequence),
        Type = SyncOperationType.Append,
        Payload = new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
        Policy = OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };

    /// <summary>Creates a complete remote result for a batch.</summary>
    /// <param name="batch">The batch to acknowledge.</param>
    /// <returns>A remote synchronization result.</returns>
    private static RemoteSyncResult CreateCompleteResult(SyncBatch batch)
    {
        var operations = new OperationSyncResult[batch.Operations.Count];
        for (var index = 0; index < batch.Operations.Count; index++)
        {
            operations[index] = new(batch.Operations[index].OperationId, OperationResultKind.Accepted, null, "v1");
        }

        return new(batch.BatchId, operations, "cursor-1", null);
    }

    /// <summary>Asserts a validation error.</summary>
    /// <param name="action">The validation action.</param>
    /// <param name="error">The expected validation error.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertValidationError(Action action, SyncBatchValidationError error)
    {
        var exception = await Assert.That(action).ThrowsExactly<SyncBatchValidationException>();

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Error).IsEqualTo(error);
    }
}
