// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>SQLite quarantine recovery tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The number of bytes in one kibibyte.</summary>
    private const int DeadLetterRecoveryKibibyteBytes = 1024;

    /// <summary>The number of bytes in one mebibyte.</summary>
    private const int DeadLetterRecoveryMebibyteBytes = DeadLetterRecoveryKibibyteBytes * DeadLetterRecoveryKibibyteBytes;

    /// <summary>A valid dead-letter payload size that exceeds the normal reopen capacity.</summary>
    private const int DeadLetterRecoveryPayloadBytes = 512 * DeadLetterRecoveryKibibyteBytes;

    /// <summary>A corrupt dead-letter payload size that fits the normal recovery capacity.</summary>
    private const int DeadLetterRecoveryCorruptPayloadBytes = 8 * DeadLetterRecoveryKibibyteBytes;

    /// <summary>The retained quarantine evidence prefix byte count.</summary>
    private const int DeadLetterRecoveryEvidenceBytes = 4096;

    /// <summary>The worker byte capacity used to seed rows larger than normal recovery allows.</summary>
    private const long DeadLetterRecoveryLargeWorkerBytes = 64L * DeadLetterRecoveryMebibyteBytes;

    /// <summary>The expected quarantine marker missing message.</summary>
    private const string DeadLetterRecoveryMissingMarkerMessage = "Expected dead-letter payload quarantine marker.";

    /// <summary>The payload type identifier used for dead-letter recovery rows.</summary>
    private const string DeadLetterRecoveryPayloadTypeId = "reading";

    /// <summary>The payload content type used for dead-letter recovery rows.</summary>
    private const string DeadLetterRecoveryPayloadContentType = "application/json";

    /// <summary>Verifies corrupt retained dead-letter payloads fail closed and persist a quarantine marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected quarantine marker is missing.</exception>
    [Test]
    public async Task WhenRecoveredDeadLetterPayloadIsCorrupt_ThenRecoveryQuarantinesOutboxOperation()
    {
        using var database = TempDatabase.Create();
        var timeProvider = new FixedTimeProvider(DeadLetterTimestamp);
        SubscriptionId subscriptionId;
        SyncOperation operation;

        await using (var adapter = CreateAdapter(database.Path, timeProvider))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var payload = CreateDeadLetterRecoveryPayload((byte)'d', DeadLetterRecoveryCorruptPayloadBytes);
            var target = await CreateDeadLetterRecoveryTargetAsync(adapter, payload, NormalWorkerBytes);
            subscriptionId = target.SubscriptionId;
            operation = target.Operation;
            _ = await adapter.DeadLetterOperationAsync(
                target.Lease.LeaseId,
                operation.OperationId,
                SqliteDeadLetterReasonCode,
                CreateSnapshotMutation(FirstClientSequence, ResultOptimisticInitialText),
                CancellationToken.None);
        }

        CorruptDeadLetterPayloadSchemaVersion(database.Path, operation.OperationId);

        await using (var reopened = CreateAdapter(database.Path, timeProvider))
        {
            await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

            await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
            var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
                ?? throw new InvalidOperationException(DeadLetterRecoveryMissingMarkerMessage);

            await Assert.That(marker.StreamId).IsEqualTo(Stream);
            await Assert.That(marker.SubscriptionId).IsEqualTo(subscriptionId);
            await Assert.That(marker.OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
            await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
            await Assert.That(marker.Evidence.SchemaVersion).IsEqualTo(0);
            await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(operation.Payload.Payload.Length);
            await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(DeadLetterRecoveryEvidenceBytes);
        }

        await using var verified = CreateAdapter(database.Path, timeProvider);
        await verified.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var persisted = await verified.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(DeadLetterRecoveryMissingMarkerMessage);

        await Assert.That(persisted.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(persisted.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
    }

    /// <summary>Verifies valid retained dead-letter payloads that exceed the current recovery budget are not quarantined.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenValidDeadLetterPayloadExceedsReopenCapacity_ThenRecoveryRejectsWithoutQuarantine()
    {
        using var database = TempDatabase.Create();
        var timeProvider = new FixedTimeProvider(DeadLetterTimestamp);
        var retainedPayload = CreateDeadLetterRecoveryPayload((byte)'v', DeadLetterRecoveryPayloadBytes);
        SubscriptionId subscriptionId;
        SyncOperation operation;
        int attempts;

        await using (var adapter = CreateDeadLetterRecoveryAdapter(database.Path, timeProvider, DeadLetterRecoveryLargeWorkerBytes))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var target = await CreateDeadLetterRecoveryTargetAsync(adapter, retainedPayload, DeadLetterRecoveryLargeWorkerBytes);
            subscriptionId = target.SubscriptionId;
            operation = target.Operation;
            _ = await adapter.DeadLetterOperationAsync(
                target.Lease.LeaseId,
                operation.OperationId,
                SqliteDeadLetterReasonCode,
                CreateSnapshotMutation(FirstClientSequence, ResultOptimisticInitialText),
                CancellationToken.None);
            attempts = ReadDeadLetterRecoveryAttempt(await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None));
        }

        await using (var normalReopen = CreateAdapter(database.Path, timeProvider))
        {
            await normalReopen.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            Func<Task> recover = () => normalReopen.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

            await Assert.That(recover).ThrowsExactly<QueueCapacityExceededException>();
            await Assert.That(await normalReopen.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        }

        await using var largerReopen = CreateDeadLetterRecoveryAdapter(database.Path, timeProvider, DeadLetterRecoveryLargeWorkerBytes);
        await largerReopen.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await largerReopen.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(SqliteDeadLetterReasonCode);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(attempts);
        await Assert.That(recovered.DeadLetters[0].DeadLetteredAtUtc).IsEqualTo(DeadLetterTimestamp);
        await Assert.That(recovered.DeadLetters[0].Operation.Payload.PayloadHash).IsEqualTo(operation.Payload.PayloadHash);
        await Assert.That(recovered.DeadLetters[0].Operation.Payload.Payload.ToArray().SequenceEqual(operation.Payload.Payload.ToArray())).IsTrue();
        await Assert.That(await largerReopen.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies dead-letter recovery preserves retained operation payload data and ordering.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDeadLetterPayloadIsRetainedAndStoreReopens_ThenRecoveryPreservesRecordAndOutboxOrder()
    {
        using var database = TempDatabase.Create();
        var timeProvider = new FixedTimeProvider(DeadLetterTimestamp);
        SubscriptionId subscriptionId;
        SyncOperation first;
        SyncOperation second;
        int attempts;

        await using (var adapter = CreateAdapter(database.Path, timeProvider))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            first = CreateOperation(FirstClientSequence) with
            {
                Payload = CreateDeadLetterRecoveryPayload((byte)'f', FirstClientSequence),
            };
            second = CreateOperation(SecondClientSequence) with
            {
                Payload = CreateDeadLetterRecoveryPayload((byte)'s', SecondClientSequence),
            };
            _ = await adapter.CommitLocalOperationAsync(
                first,
                CreateSnapshotMutation(0, ResultOptimisticInitialText) with
                {
                    AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText),
                },
                CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(second, CreateSnapshotMutation(1, ResultOptimisticLocalText), CancellationToken.None);
            var lease = await ReadSingleLeaseAsync(adapter, new(Stream, TwoWorkerCommands, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

            _ = await adapter.DeadLetterOperationAsync(
                lease.LeaseId,
                second.OperationId,
                SqliteDeadLetterReasonCode,
                CreateSnapshotMutation(DeadLetterTwoOperations, ResultOptimisticInitialText),
                CancellationToken.None);
            attempts = ReadDeadLetterRecoveryAttempt(await adapter.GetOperationStatusAsync(second.OperationId, CancellationToken.None));
        }

        await using var reopened = CreateAdapter(database.Path, timeProvider);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(DeadLetterRebuiltRevision);
        await Assert.That(PayloadText(recovered.Snapshot?.State)).IsEqualTo(ResultOptimisticInitialText);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].Operation.OperationId).IsEqualTo(second.OperationId);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(SqliteDeadLetterReasonCode);
        await Assert.That(recovered.DeadLetters[0].Attempts).IsEqualTo(attempts);
        await Assert.That(recovered.DeadLetters[0].DeadLetteredAtUtc).IsEqualTo(DeadLetterTimestamp);
        await Assert.That(recovered.DeadLetters[0].Operation.Payload.PayloadHash).IsEqualTo(second.Payload.PayloadHash);
        await Assert.That(recovered.DeadLetters[0].Operation.Payload.Payload.ToArray().SequenceEqual(second.Payload.Payload.ToArray())).IsTrue();
        await Assert.That(await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies recovery fails closed when only retained dead-letter rows remain.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected recovery failure was not observed.</exception>
    [Test]
    public async Task WhenOnlyDeadLetterRowsRemainWithoutStreamState_ThenRecoveryRejectsCommittedData()
    {
        using var database = TempDatabase.Create();
        var timeProvider = new FixedTimeProvider(DeadLetterTimestamp);
        SubscriptionId subscriptionId;

        await using (var adapter = CreateAdapter(database.Path, timeProvider))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var payload = CreateDeadLetterRecoveryPayload((byte)'m', FirstClientSequence);
            var target = await CreateDeadLetterRecoveryTargetAsync(adapter, payload, NormalWorkerBytes);
            subscriptionId = target.SubscriptionId;
            _ = await adapter.DeadLetterOperationAsync(
                target.Lease.LeaseId,
                target.Operation.OperationId,
                SqliteDeadLetterReasonCode,
                CreateSnapshotMutation(FirstClientSequence, ResultOptimisticInitialText),
                CancellationToken.None);
        }

        DeleteSnapshot(database.Path);
        DeleteDeadLetterRecoveryStreamWithoutCascade(database.Path);

        await Assert.That(ReadRetainedDeadLetterRecoveryRowCount(database.Path)).IsEqualTo(1);

        await using var reopened = CreateAdapter(database.Path, timeProvider);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(recover)
            ?? throw new InvalidOperationException("Expected committed data recovery to fail.");
        await Assert.That(exception.Message).IsEqualTo("Committed data has no durable stream state.");
        await Assert.That(await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Creates a committed leased operation with the supplied payload.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="payload">The operation payload.</param>
    /// <param name="leaseMaximumBytes">The lease request byte capacity.</param>
    /// <returns>The committed operation test state.</returns>
    private static async Task<DeadLetterTarget> CreateDeadLetterRecoveryTargetAsync(
        SqliteLocalStoreAdapter adapter,
        PayloadEnvelope payload,
        long leaseMaximumBytes)
    {
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence) with { Payload = payload };
        var mutation = CreateSnapshotMutation(0, ResultOptimisticInitialText) with
        {
            AuthoritativeState = CreatePayload(ResultAuthoritativeInitialText),
        };
        _ = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, leaseMaximumBytes, TimeSpan.FromMinutes(1)));

        return new(operation, lease, subscriptionId);
    }

    /// <summary>Reads the actual persisted attempt value for a dead-lettered operation.</summary>
    /// <param name="status">The persisted operation status.</param>
    /// <returns>The persisted attempt value.</returns>
    /// <exception cref="InvalidOperationException">The status was not the expected dead-letter state.</exception>
    private static int ReadDeadLetterRecoveryAttempt(SyncOperationStatus? status)
    {
        if (status?.State != SyncOperationState.DeadLettered)
        {
            throw new InvalidOperationException("Expected a dead-lettered operation status.");
        }

        return status.Attempt;
    }

    /// <summary>Creates an adapter with a supplied recovery capacity.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="timeProvider">The deterministic clock.</param>
    /// <param name="workerBytes">The worker byte capacity.</param>
    /// <returns>The configured adapter.</returns>
    private static SqliteLocalStoreAdapter CreateDeadLetterRecoveryAdapter(string path, TimeProvider timeProvider, long workerBytes) =>
        new(path, new() { TimeProvider = timeProvider, WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = workerBytes });

    /// <summary>Creates a payload envelope with a canonical SHA-256 hash.</summary>
    /// <param name="fill">The byte value used to fill the payload.</param>
    /// <param name="payloadBytes">The payload byte count.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateDeadLetterRecoveryPayload(byte fill, int payloadBytes)
    {
        var payload = new byte[payloadBytes];
        Array.Fill(payload, fill);
        return new(
            DeadLetterRecoveryPayloadTypeId,
            SchemaVersion,
            DeadLetterRecoveryPayloadContentType,
            payload,
            ComputeDeadLetterRecoverySha256PayloadHash(payload));
    }

    /// <summary>Computes a canonical SHA-256 payload hash.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The formatted payload hash.</returns>
    private static string ComputeDeadLetterRecoverySha256PayloadHash(byte[] payload) =>
        $"sha256-{Convert.ToBase64String(SHA256.HashData(payload))}";

    /// <summary>Corrupts the schema version metadata for a retained dead-letter payload.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void CorruptDeadLetterPayloadSchemaVersion(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload_schema_version = 0
            WHERE store_identity = $storeIdentity
                AND stream_id = $streamId
                AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        ThrowIfDeadLetterRecoveryMutationMissing(command.ExecuteNonQuery());
    }

    /// <summary>Deletes the stream row while retaining dead-letter outbox and state rows.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void DeleteDeadLetterRecoveryStreamWithoutCascade(string path)
    {
        using var connection = OpenRawConnection(path);
        using var disableKeys = connection.CreateCommand();
        disableKeys.CommandText = "PRAGMA foreign_keys = OFF;";
        _ = disableKeys.ExecuteNonQuery();

        using var delete = connection.CreateCommand();
        delete.CommandText = """
            DELETE FROM oc_streams
            WHERE store_identity = $storeIdentity
                AND stream_id = $streamId;
            """;
        _ = delete.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = delete.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        ThrowIfDeadLetterRecoveryMutationMissing(delete.ExecuteNonQuery());
    }

    /// <summary>Counts retained dead-letter rows for the stream.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The retained dead-letter row count.</returns>
    /// <exception cref="InvalidOperationException">The row count could not be read.</exception>
    private static long ReadRetainedDeadLetterRecoveryRowCount(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM oc_outbox AS outbox
            INNER JOIN oc_outbox_operation_states AS state
                ON state.store_identity = outbox.store_identity
                AND state.operation_id = outbox.operation_id
            WHERE outbox.store_identity = $storeIdentity
                AND outbox.stream_id = $streamId
                AND state.operation_state = 6;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        return command.ExecuteScalar() is long rowCount
            ? rowCount
            : throw new InvalidOperationException("Expected a retained dead-letter row count.");
    }

    /// <summary>Rejects a raw dead-letter recovery mutation that did not target one row.</summary>
    /// <param name="rowCount">The SQLite affected row count.</param>
    /// <exception cref="InvalidOperationException">The dead-letter row was not found.</exception>
    private static void ThrowIfDeadLetterRecoveryMutationMissing(int rowCount)
    {
        if (rowCount == 1)
        {
            return;
        }

        throw new InvalidOperationException("Expected one dead-letter outbox row.");
    }
}
