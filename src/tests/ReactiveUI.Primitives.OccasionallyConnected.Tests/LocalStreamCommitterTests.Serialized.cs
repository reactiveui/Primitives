// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The sequence expected after one serialized commit.</summary>
    private const int SerializedNextSequence = 2;

    /// <summary>The caller-supplied serialized operation priority.</summary>
    private const int SerializedPriority = 3;

    /// <summary>The serialized operation sequence after one accepted commit.</summary>
    private const long SerializedSecondClientSequence = 2;

    /// <summary>The unsupported input schema version.</summary>
    private const int UnsupportedInputSchemaVersion = InputSchemaVersion + 1;

    /// <summary>A different stream identifier.</summary>
    private const string OtherSerializedStream = "sensor/humidity";

    /// <summary>The serialized metadata source key.</summary>
    private const string SerializedSourceKey = "source";

    /// <summary>The serialized metadata source value.</summary>
    private const string SerializedSourceValue = "serialized";

    /// <summary>The stable serialized operation identifier.</summary>
    private static readonly OperationId SerializedOperationId = new(new Guid("4a30e951-96e1-4fa8-8d58-92c48cf6ad1b"));

    /// <summary>The second stable serialized operation identifier.</summary>
    private static readonly OperationId SecondSerializedOperationId = new(new Guid("25620566-72ad-4c43-b9b2-90fc78ec1aa3"));

    /// <summary>Verifies a caller-supplied serialized operation is projected and stored with its original envelope.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncPreservesCallerEnvelopeAndPayload()
    {
        var payload = CreateSerializedInputPayload(FirstReadingValue);
        var operation = CreateSerializedOperation(payload);
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);

        var result = await committer.CommitSerializedAsync(operation, CancellationToken.None);

        await Assert.That(result.Operation).IsSameReferenceAs(operation);
        await Assert.That(result.Operation.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(result.Operation.ClientSequence).IsEqualTo(operation.ClientSequence);
        await Assert.That(result.Operation.TimestampUtc).IsEqualTo(operation.TimestampUtc);
        await Assert.That(result.Operation.BaseVersion).IsEqualTo(operation.BaseVersion);
        await Assert.That(result.Operation.Type).IsEqualTo(operation.Type);
        await Assert.That(result.Operation.Policy).IsEqualTo(operation.Policy);
        await Assert.That(result.Operation.Metadata[SerializedSourceKey]).IsEqualTo(SerializedSourceValue);
        await Assert.That(PayloadEnvelopeComparison.ContentEquals(result.Operation.Payload, payload)).IsTrue();
        await Assert.That(store.CommittedOperation).IsSameReferenceAs(operation);
        await Assert.That(projection.LocalOperation).IsSameReferenceAs(operation);
        await Assert.That(serializer.InputSerializeCount).IsEqualTo(0);
        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(1);
        await Assert.That(result.Input.Value).IsEqualTo(FirstReadingValue);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(result.State.NextClientSequence).IsEqualTo(SerializedNextSequence);
    }

    /// <summary>Verifies stale serialized operations are rejected before projection or store mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsStaleSequenceBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var first = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue));
        _ = await committer.CommitSerializedAsync(first, CancellationToken.None);
        var stale = CreateSerializedOperation(CreateSerializedInputPayload(SecondReadingValue));

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(stale, CancellationToken.None).AsTask());

        await Assert.That(projection.LocalApplyCount).IsEqualTo(1);
        await Assert.That(store.CommitCallCount).IsEqualTo(1);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(SerializedNextSequence);
    }

    /// <summary>Verifies invalid serialized payloads are rejected before projection or store mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsInvalidPayloadBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer { RejectInputHash = true };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var payload = CreateSerializedInputPayload(FirstReadingValue) with { PayloadHash = "hash-corrupt" };
        var operation = CreateSerializedOperation(payload);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(1);
    }

    /// <summary>Verifies serialized operations survive SQLite reopen with their original payload and metadata.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncPersistsCallerEnvelopeAcrossSqliteReopen()
    {
        var directory = SqliteTestDirectory.Create("oc-serialized-commit-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var payload = CreateSerializedInputPayload(FirstReadingValue);
            var operation = CreateSerializedOperation(payload);
            await CommitSerializedSqliteAsync(databasePath, operation);
            await using var reopened = new SqliteLocalStoreAdapter(databasePath);
            var subscription = await InitializeResultStoreAsync(reopened);
            var committer = CreateLocalCommitter(CreateResultOptions(reopened, subscription, new SumProjection()));
            var state = await committer.RecoverAsync(CancellationToken.None);
            var recovery = await reopened.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

            await Assert.That(state.State.Sum).IsEqualTo(FirstReadingValue);
            await Assert.That(state.Revision).IsEqualTo(1);
            await Assert.That(state.NextClientSequence).IsEqualTo(SerializedNextSequence);
            await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
            await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
            await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(operation.ClientSequence);
            await Assert.That(recovery.PendingOperations[0].TimestampUtc).IsEqualTo(operation.TimestampUtc);
            await Assert.That(recovery.PendingOperations[0].BaseVersion).IsEqualTo(operation.BaseVersion);
            await Assert.That(recovery.PendingOperations[0].Type).IsEqualTo(operation.Type);
            await Assert.That(recovery.PendingOperations[0].Policy).IsEqualTo(operation.Policy);
            await Assert.That(recovery.PendingOperations[0].Metadata[SerializedSourceKey]).IsEqualTo(SerializedSourceValue);
            await Assert.That(PayloadEnvelopeComparison.ContentEquals(recovery.PendingOperations[0].Payload, payload)).IsTrue();
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Verifies serialized commits reject operations for another stream before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsWrongStreamBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue), streamId: new StreamId(OtherSerializedStream));

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject non-UTC timestamps before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsNonUtcTimestampBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue)) with
        {
            TimestampUtc = new(2026, 9, 13, 2, 15, 0, TimeSpan.FromHours(1)),
        };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject malformed base-version text before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsMalformedBaseVersionBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(
            CreateSerializedInputPayload(FirstReadingValue),
            baseVersion: "\uD800");

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject malformed metadata before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsMalformedMetadataBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var metadata = new Dictionary<string, string> { [string.Empty] = SerializedSourceValue };
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue), metadata: metadata);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits accept operations without an optional base version.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncAcceptsMissingBaseVersion()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue), baseVersion: null);

        var result = await committer.CommitSerializedAsync(operation, CancellationToken.None);

        await Assert.That(result.Operation.BaseVersion).IsNull();
        await Assert.That(projection.LocalApplyCount).IsEqualTo(1);
        await Assert.That(store.CommitCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies serialized commits reject missing metadata values before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsMissingMetadataValueBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var metadata = new Dictionary<string, string> { [SerializedSourceKey] = CreateMissingString() };
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue), metadata: metadata);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject a missing payload before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsMissingPayloadBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue)) with { Payload = CreateMissingPayload() };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies every defined serialized operation type can use the local projection path.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncAcceptsEveryDefinedOperationType()
    {
        var operationTypes = new[]
        {
            SyncOperationType.Append,
            SyncOperationType.Update,
            SyncOperationType.Delete,
            SyncOperationType.Custom,
        };

        foreach (var operationType in operationTypes)
        {
            var projection = new RecordingProjection();
            var serializer = new ScriptedPayloadSerializer();
            var store = new ScriptedLocalStore();
            var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
            var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue), type: operationType);

            var result = await committer.CommitSerializedAsync(operation, CancellationToken.None);

            await Assert.That(result.Operation.Type).IsEqualTo(operationType);
            await Assert.That(projection.LocalApplyCount).IsEqualTo(1);
            await Assert.That(store.CommitCallCount).IsEqualTo(1);
        }
    }

    /// <summary>Verifies undefined serialized operation types are rejected before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsUndefinedOperationTypeBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var operation = CreateSerializedOperation(
            CreateSerializedInputPayload(FirstReadingValue),
            type: (SyncOperationType)17);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject unsupported input schema versions before decoding or projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsUnsupportedSchemaBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var payload = CreateSerializedInputPayload(FirstReadingValue) with { SchemaVersion = UnsupportedInputSchemaVersion };
        var operation = CreateSerializedOperation(payload);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(serializer.InputDeserializeCount).IsEqualTo(0);
        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits reject malformed payload bytes before projection or store mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsMalformedPayloadBeforeProjection()
    {
        var projection = new RecordingProjection();
        var serializer = new ScriptedPayloadSerializer();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, projection);
        var payload = "not-a-reading"u8.ToArray();
        var envelope = new PayloadEnvelope(InputContract, InputSchemaVersion, TestContentType, payload, "hash-malformed");
        var operation = CreateSerializedOperation(envelope);

        _ = await Assert.ThrowsExactlyAsync<FormatException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(projection.LocalApplyCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies serialized commits isolate mutable projection state when the store rejects the commit.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncStoreFailureLeavesMutatingProjectionIsolated()
    {
        var store = new ScriptedLocalStore { CommitException = new InvalidOperationException("commit failed") };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, new MutatingProjection());
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FailedCommitValue));

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation before the serialized store commit leaves state unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncCancellationBeforeStoreLeavesStateUnchanged()
    {
        using CancellationTokenSource source = new();
        var serializer = new ScriptedPayloadSerializer { CancelAfterStateSerialization = source };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer, new RecordingProjection());
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(CanceledCommitValue));

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.CommitSerializedAsync(operation, source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(1);
    }

    /// <summary>Verifies post-commit cancellation returns the known serialized commit receipt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncCancellationAfterStoreCommitReturnsReceipt()
    {
        using CancellationTokenSource source = new();
        var store = new ScriptedLocalStore { CancelAfterSuccessfulCommit = source };
        var committer = await CreateRecoveredCommitterAsync(store, new(), new RecordingProjection());
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(PostCommitCancellationValue));

        var result = await committer.CommitSerializedAsync(operation, source.Token);

        await Assert.That(source.IsCancellationRequested).IsTrue();
        await Assert.That(result.Receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(result.State.State.Sum).IsEqualTo(PostCommitCancellationValue);
        await Assert.That(committer.Current.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies overlapping serialized commits are rejected without queuing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncRejectsOverlapImmediately()
    {
        TaskCompletionSource enteredStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedLocalStore { BeforeCommitAsync = PauseAfterSignal(enteredStore, releaseStore) };
        var committer = await CreateRecoveredCommitterAsync(store, new(), new RecordingProjection());
        var firstOperation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue));
        var secondOperation = CreateSerializedOperation(
            CreateSerializedInputPayload(SecondReadingValue),
            SerializedSecondClientSequence,
            operationId: SecondSerializedOperationId);
        var first = committer.CommitSerializedAsync(firstOperation, CancellationToken.None).AsTask();
        try
        {
            await enteredStore.Task.WaitAsync(TimeSpan.FromSeconds(StoreStartWaitSeconds));
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.CommitSerializedAsync(secondOperation, CancellationToken.None).AsTask());

            await Assert.That(exception?.Message).Contains("already in progress");
            await Assert.That(first.IsCompleted).IsFalse();
            await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        }
        finally
        {
            _ = releaseStore.TrySetResult();
            await first;
        }

        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
    }

    /// <summary>Verifies malformed serialized commit receipts poison the committer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncMalformedReceiptPoisonsCommitter()
    {
        var store = new ScriptedLocalStore { ReceiptSequenceOffset = 1 };
        var committer = await CreateRecoveredCommitterAsync(store, new(), new RecordingProjection());
        var operation = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue));

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitSerializedAsync(operation, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("poisoned");
    }

    /// <summary>Verifies duplicate operation identifiers roll back without exposing projected state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitSerializedAsyncDuplicateOperationIdRollsBackState()
    {
        var directory = SqliteTestDirectory.Create("oc-serialized-duplicate-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            await using var store = new SqliteLocalStoreAdapter(databasePath);
            var subscription = await InitializeResultStoreAsync(store);
            var committer = CreateLocalCommitter(CreateResultOptions(store, subscription, new SumProjection()));
            _ = await committer.RecoverAsync(CancellationToken.None);
            var first = CreateSerializedOperation(CreateSerializedInputPayload(FirstReadingValue));
            var duplicate = CreateSerializedOperation(
                CreateSerializedInputPayload(SecondReadingValue),
                SerializedSecondClientSequence);
            _ = await committer.CommitSerializedAsync(first, CancellationToken.None);

            await Assert.That(CommitDuplicateAsync).Throws<Exception>();

            await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
            await Assert.That(committer.Current.NextClientSequence).IsEqualTo(SerializedNextSequence);

            async Task CommitDuplicateAsync() =>
                _ = await committer.CommitSerializedAsync(duplicate, CancellationToken.None);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Creates a serialized local operation.</summary>
    /// <param name="payload">The operation payload.</param>
    /// <param name="clientSequence">The caller-supplied sequence.</param>
    /// <param name="streamId">The caller-supplied stream identifier.</param>
    /// <param name="operationId">The caller-supplied operation identifier.</param>
    /// <param name="type">The caller-supplied operation type.</param>
    /// <param name="baseVersion">The caller-supplied base version.</param>
    /// <param name="metadata">The caller-supplied metadata.</param>
    /// <returns>The serialized operation.</returns>
    private static SyncOperation CreateSerializedOperation(
        PayloadEnvelope payload,
        long clientSequence = 1,
        StreamId? streamId = null,
        OperationId? operationId = null,
        SyncOperationType type = SyncOperationType.Custom,
        string? baseVersion = "server-v7",
        IReadOnlyDictionary<string, string>? metadata = null) =>
        new()
        {
            OperationId = operationId ?? SerializedOperationId,
            StreamId = streamId ?? Stream,
            ClientSequence = clientSequence,
            TimestampUtc = new(2026, 9, 13, 1, 15, 0, TimeSpan.Zero),
            BaseVersion = baseVersion,
            Type = type,
            Payload = payload,
            Policy = OperationPolicy.Default with { Priority = SerializedPriority },
            Metadata = metadata ?? new Dictionary<string, string> { [SerializedSourceKey] = SerializedSourceValue },
        };

    /// <summary>Creates a serialized input payload without invoking the configured serializer.</summary>
    /// <param name="value">The encoded reading value.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateSerializedInputPayload(int value)
    {
        var text = value.ToString(CultureInfo.InvariantCulture);
        var payload = System.Text.Encoding.UTF8.GetBytes(text);
        return new(InputContract, InputSchemaVersion, TestContentType, payload, $"hash-{text}");
    }

    /// <summary>Creates a runtime-null payload to exercise defensive validation on required operation payloads.</summary>
    /// <returns>A null payload reference.</returns>
    private static PayloadEnvelope CreateMissingPayload()
    {
        object? payload = null;
        return Unsafe.As<object?, PayloadEnvelope>(ref payload);
    }

    /// <summary>Creates a runtime-null metadata value to exercise defensive validation.</summary>
    /// <returns>A null string reference.</returns>
    private static string CreateMissingString()
    {
        object? value = null;
        return Unsafe.As<object?, string>(ref value);
    }

    /// <summary>Commits a serialized operation to a SQLite-backed committer.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="operation">The serialized operation.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task CommitSerializedSqliteAsync(string databasePath, SyncOperation operation)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        var subscription = await InitializeResultStoreAsync(store);
        var committer = CreateLocalCommitter(CreateResultOptions(store, subscription, new SumProjection()));
        _ = await committer.RecoverAsync(CancellationToken.None);
        _ = await committer.CommitSerializedAsync(operation, CancellationToken.None);
    }

    /// <summary>Creates and recovers a configured committer with a caller-supplied projection.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="projection">The projection.</param>
    /// <returns>The recovered committer.</returns>
    private static async ValueTask<LocalStreamCommitter<ReadingState, MutableReading>> CreateRecoveredCommitterAsync(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer serializer,
        ILocalProjection<ReadingState, MutableReading> projection)
    {
        var committer = CreateLocalCommitter(CreateOptions(store, serializer, projection));
        _ = await committer.RecoverAsync(CancellationToken.None);
        return committer;
    }

    /// <summary>Creates committer options with a caller-supplied projection.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="projection">The projection.</param>
    /// <returns>The committer options.</returns>
    private static LocalStreamCommitterOptions<ReadingState, MutableReading> CreateOptions(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer serializer,
        ILocalProjection<ReadingState, MutableReading> projection) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = Subscription,
            ClientId = ReconciliationClientId,
            Contracts = CreateContracts(),
            Dependencies = new()
            {
                Store = store,
                Serializer = serializer,
                Projection = projection,
                OperationIdSource = new SequenceOperationIdSource(),
                TimeProvider = new FixedTimeProvider(CommittedUtc),
            },
        };

    /// <summary>Records the exact operation passed into local projection.</summary>
    private sealed class RecordingProjection : ILocalProjection<ReadingState, MutableReading>
    {
        /// <inheritdoc/>
        public ReadingState InitialState { get; } = new(InitialSum);

        /// <summary>Gets the last local operation passed to projection.</summary>
        public SyncOperation? LocalOperation { get; private set; }

        /// <summary>Gets the number of local projection calls.</summary>
        public int LocalApplyCount { get; private set; }

        /// <inheritdoc/>
        public ReadingState ApplyLocal(ReadingState state, MutableReading input, SyncOperation operation)
        {
            LocalApplyCount++;
            LocalOperation = operation;
            return new(state.Sum + input.Value);
        }

        /// <inheritdoc/>
        public ReadingState ApplyRemote(ReadingState state, MutableReading input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState Reconcile(ReadingState state, ConflictResolutionResult result) => state;
    }
}
