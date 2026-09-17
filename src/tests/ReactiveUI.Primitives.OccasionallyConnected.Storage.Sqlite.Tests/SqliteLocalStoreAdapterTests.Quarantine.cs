// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Payload quarantine tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The stable quarantined stream message fragment.</summary>
    private const string QuarantinedMessage = "quarantined";

    /// <summary>The byte length exposed by SQLite when integer payload evidence is coerced to text.</summary>
    private const int CoercedIntegerPayloadLength = 3;

    /// <summary>Verifies a quarantine marker persists across reopen and blocks upload leasing.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPayloadIsQuarantined_ThenReopenRecoversMarkerAndBlocksAffectedLease()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
            _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var leased = await ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(marker?.StreamId).IsEqualTo(Stream);
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(operation.Payload.PayloadLength);
        await Assert.That(recovery.Quarantine?.QuarantineId).IsEqualTo(marker?.QuarantineId);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(leased).IsNull();
    }

    /// <summary>Verifies quarantining one stream does not stop upload progress for another stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOneStreamIsQuarantined_ThenUnrelatedStreamCanStillLease()
    {
        using var database = TempDatabase.Create();
        var otherStream = new StreamId("sensor/humidity");
        var operation = CreateOperation(FirstClientSequence);
        var otherOperation = CreateOperation(FirstClientSequence) with { StreamId = otherStream };
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(otherStream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(otherOperation, CreateSnapshotMutation(otherStream, expectedRevision: 0), CancellationToken.None);
        _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);

        var leased = await ReadSingleLeaseAsync(adapter, new(null, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(leased.Operations.Count).IsEqualTo(1);
        await Assert.That(leased.Operations[0].StreamId).IsEqualTo(otherStream);
        await Assert.That(leased.Operations[0].OperationId).IsEqualTo(otherOperation.OperationId);
    }

    /// <summary>Verifies a quarantined stream rejects subsequent local commits.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPayloadIsQuarantined_ThenSameStreamCommitFailsClosed()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var firstMarker = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        var secondMarker = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> commit = () => adapter.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(commit);

        await Assert.That(exception?.Message).Contains(QuarantinedMessage);
        await Assert.That(firstMarker.Created).IsTrue();
        await Assert.That(secondMarker.Created).IsFalse();
        await Assert.That(secondMarker.Record.QuarantineId).IsEqualTo(firstMarker.Record.QuarantineId);
    }

    /// <summary>Verifies a quarantined stream rejects retry state writes for its pending operations.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPayloadIsQuarantined_ThenRetryStateWriteFailsClosed()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> saveRetry = () => adapter
            .SaveRetryStateAsync(operation.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None)
            .AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(saveRetry);
        var retry = await adapter.GetRetryStateAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(exception?.Message).Contains(QuarantinedMessage);
        await Assert.That(retry).IsNull();
    }

    /// <summary>Verifies a lease acquired before quarantine cannot be renewed after the stream is quarantined.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPayloadIsQuarantined_ThenExistingLeaseRenewalFailsClosed()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> renew = () => adapter.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(renew);

        await Assert.That(exception?.Message).Contains(QuarantinedMessage);
    }

    /// <summary>Verifies raw evidence without optional metadata round-trips through the quarantine side table.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The marker is not found.</exception>
    [Test]
    public async Task WhenRawQuarantineEvidenceOmitsOptionalMetadata_ThenNullColumnsRoundTrip()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        LocalPayloadQuarantineRequest request = new()
        {
            StreamId = Stream,
            Source = LocalPayloadQuarantineSource.Snapshot,
            Reason = LocalPayloadQuarantineReason.PersistedRecordCorrupt,
            Evidence = new(null, null, null, 0, null, ReadOnlyMemory<byte>.Empty),
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };

        var result = await adapter.QuarantinePayloadAsync(request, CancellationToken.None);
        var stored = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var marker = stored ?? throw new InvalidOperationException("Expected a quarantine marker.");

        await Assert.That(result.Created).IsTrue();
        await Assert.That(marker.SubscriptionId).IsNull();
        await Assert.That(marker.OperationId).IsNull();
        await Assert.That(marker.EventId).IsNull();
        await Assert.That(marker.ReasonCode).IsNull();
        await Assert.That(marker.Cursor).IsNull();
        await Assert.That(marker.Evidence.ContractId).IsNull();
        await Assert.That(marker.Evidence.SchemaVersion).IsNull();
        await Assert.That(marker.Evidence.ContentType).IsNull();
        await Assert.That(marker.Evidence.PayloadHash).IsNull();
        await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(0);
    }

    /// <summary>Verifies quarantine requests must carry an observation timestamp.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineRequestOmitsObservationTimestamp_ThenWriteRejectsIt()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        LocalPayloadQuarantineRequest request = new()
        {
            StreamId = Stream,
            Source = LocalPayloadQuarantineSource.Snapshot,
            Reason = LocalPayloadQuarantineReason.PersistedRecordCorrupt,
            Evidence = new(null, null, null, 0, null, ReadOnlyMemory<byte>.Empty),
            ObservedAtUtc = default,
        };
        Func<Task> write = () => adapter.QuarantinePayloadAsync(request, CancellationToken.None).AsTask();

        await Assert.That(write).ThrowsExactly<ArgumentException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies quarantine requests must carry either raw evidence or a typed payload envelope.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineRequestOmitsEvidenceAndEnvelope_ThenWriteRejectsIt()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        LocalPayloadQuarantineRequest request = new()
        {
            StreamId = Stream,
            Source = LocalPayloadQuarantineSource.Snapshot,
            Reason = LocalPayloadQuarantineReason.PersistedRecordCorrupt,
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };
        Func<Task> write = () => adapter.QuarantinePayloadAsync(request, CancellationToken.None).AsTask();

        await Assert.That(write).ThrowsExactly<ArgumentException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies invalid optional operation identifiers are rejected when reading a marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineOperationIdentifierIsInvalid_ThenReadFails()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        CorruptQuarantineOperationIdentifier(database.Path);

        Func<Task> read = () => adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None).AsTask();

        await Assert.That(read).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies raw corrupt snapshot metadata is quarantined before recovery fails closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPersistedSnapshotMetadataIsCorrupt_ThenRecoveryQuarantinesRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        }

        CorruptSnapshotPayloadSchemaVersion(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var guardedRecovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var leased = await ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker?.Evidence.SchemaVersion).IsEqualTo(0);
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(snapshot.State.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(snapshot.State.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(snapshot.State.Payload.ToArray())).IsTrue();
        await Assert.That(guardedRecovery.Quarantine?.QuarantineId).IsEqualTo(marker?.QuarantineId);
        await Assert.That(guardedRecovery.Snapshot).IsNull();
        await Assert.That(leased).IsNull();
    }

    /// <summary>Verifies raw corrupt authoritative snapshot metadata is quarantined while optimistic evidence stays untouched.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPersistedAuthoritativeSnapshotMetadataIsCorrupt_ThenRecoveryQuarantinesOriginalEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var authoritative = CreatePayload("authoritative");
        var snapshot = CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = authoritative };
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        }

        CorruptAuthoritativeSnapshotPayloadSchemaVersion(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var guardedRecovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await AssertAuthoritativeSnapshotEvidenceAsync(marker, authoritative, snapshot.State.PayloadHash);
        await AssertQuarantinedRecoveryAsync(marker, guardedRecovery);
    }

    /// <summary>Verifies out-of-range raw snapshot schema metadata is quarantined with bounded evidence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPersistedSnapshotSchemaVersionIsOutOfRange_ThenRecoveryQuarantinesRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        }

        CorruptSnapshotPayloadSchemaVersionOutOfRange(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);

        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker?.Evidence.SchemaVersion).IsNull();
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(snapshot.State.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(snapshot.State.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(snapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies a TEXT schema storage class is not accepted through provider integer coercion.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenPersistedSnapshotSchemaVersionIsTextWithSuffix_ThenRecoveryQuarantinesRawEvidence() =>
        AssertSnapshotSchemaStorageClassQuarantines(CorruptSnapshotPayloadSchemaVersionTextWithSuffix);

    /// <summary>Verifies a REAL schema storage class is not accepted through provider integer coercion.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenPersistedSnapshotSchemaVersionIsRealFraction_ThenRecoveryQuarantinesRawEvidence() =>
        AssertSnapshotSchemaStorageClassQuarantines(CorruptSnapshotPayloadSchemaVersionRealFraction);

    /// <summary>Verifies Microsoft.Data.Sqlite coerces a TEXT schema-like value when read as Int32.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenProviderReadsTextSchemaStorageClassAsInt32_ThenItCoercesValue()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT CAST('1garbage' AS TEXT);";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        _ = await reader.ReadAsync(CancellationToken.None);

        await Assert.That(reader.GetDataTypeName(0)).IsEqualTo("TEXT");
        await Assert.That(reader.GetInt32(0)).IsEqualTo(1);
    }

    /// <summary>Verifies Microsoft.Data.Sqlite coerces a REAL schema-like value when read as Int32.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenProviderReadsRealSchemaStorageClassAsInt32_ThenItCoercesValue()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT CAST(1.5 AS REAL);";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        _ = await reader.ReadAsync(CancellationToken.None);

        await Assert.That(reader.GetDataTypeName(0)).IsEqualTo("REAL");
        await Assert.That(reader.GetInt32(0)).IsEqualTo(1);
    }

    /// <summary>Verifies coerced raw SQLite payload bytes are captured as bounded evidence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPersistedSnapshotPayloadIsCoerced_ThenRecoveryQuarantinesBoundedRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        }

        CorruptSnapshotPayloadColumnTypes(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);

        await Assert.That(marker?.Evidence.SchemaVersion).IsNull();
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(CoercedIntegerPayloadLength);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual("123"u8.ToArray())).IsTrue();
    }

    /// <summary>Verifies raw corrupt outbox metadata is quarantined during lease acquisition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPersistedOutboxMetadataIsCorrupt_ThenLeaseQuarantinesRawEvidence()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        }

        CorruptOutboxPayloadSchemaVersion(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> lease = () => ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1))).AsTask();

        await Assert.That(lease).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var secondLease = await ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
        await Assert.That(marker?.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(marker?.Evidence.SchemaVersion).IsEqualTo(0);
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(operation.Payload.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(operation.Payload.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(operation.Payload.Payload.ToArray())).IsTrue();
        await Assert.That(secondLease).IsNull();
    }

    /// <summary>Verifies a TEXT outbox schema storage class is quarantined during lease acquisition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenPersistedOutboxSchemaVersionIsTextWithSuffix_ThenLeaseQuarantinesRawEvidence() =>
        AssertOutboxSchemaStorageClassQuarantines(CorruptOutboxPayloadSchemaVersionTextWithSuffix);

    /// <summary>Verifies a REAL outbox schema storage class is quarantined during lease acquisition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenPersistedOutboxSchemaVersionIsRealFraction_ThenLeaseQuarantinesRawEvidence() =>
        AssertOutboxSchemaStorageClassQuarantines(CorruptOutboxPayloadSchemaVersionRealFraction);

    /// <summary>Verifies internal raw evidence capture tolerates null columns without constructing an envelope.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRawPayloadEvidenceReaderSeesNullColumns_ThenCapturesEmptyEvidence()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;";
        await using var reader = await command.ExecuteReaderAsync(CancellationToken.None);
        _ = await reader.ReadAsync(CancellationToken.None);

        var evidence = SqliteLocalCommitSql.CapturePayloadEvidence(reader, SqlitePayloadEvidenceColumns.StartingAt(0));

        await Assert.That(evidence.ContractId).IsNull();
        await Assert.That(evidence.SchemaVersion).IsNull();
        await Assert.That(evidence.ContentType).IsNull();
        await Assert.That(evidence.PayloadLength).IsEqualTo(0);
        await Assert.That(evidence.PayloadHash).IsNull();
        await Assert.That(evidence.PayloadPrefix.Length).IsEqualTo(0);
    }

    /// <summary>Verifies a quarantine insert fails closed if the marker disappears before it can be reread.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineMarkerCannotBeReread_ThenWriteFailsClosed()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        DeleteInsertedQuarantineMarkers(database.Path);
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(quarantine);
        var marker = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);

        await Assert.That(exception?.Message).Contains("not persisted");
        await Assert.That(marker).IsNull();
    }

    /// <summary>Verifies conventional SQLite quarantine exception constructors keep empty evidence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqlitePayloadQuarantineExceptionUsesConventionalConstructors_ThenEvidenceIsEmpty()
    {
        const string Message = "custom message";
        var inner = new InvalidOperationException("inner");

        var defaultException = new SqlitePayloadQuarantineException();
        var messageException = new SqlitePayloadQuarantineException(Message);
        var innerException = new SqlitePayloadQuarantineException(Message, inner);
        var fallbackOperationId = OperationId.New();

        await Assert.That(defaultException.Message).IsEqualTo("The SQLite payload row is invalid.");
        await Assert.That(defaultException.Evidence.PayloadLength).IsEqualTo(0);
        await Assert.That(defaultException.Evidence.PayloadPrefix.Length).IsEqualTo(0);
        await Assert.That(defaultException.ResolveOperationId(fallbackOperationId)).IsEqualTo(fallbackOperationId);
        await Assert.That(messageException.Message).IsEqualTo(Message);
        await Assert.That(messageException.Evidence.PayloadLength).IsEqualTo(0);
        await Assert.That(innerException.InnerException).IsSameReferenceAs(inner);
        await Assert.That(innerException.Evidence.PayloadPrefix.Length).IsEqualTo(0);
    }

    /// <summary>Asserts that authoritative snapshot metadata corruption preserves authoritative evidence.</summary>
    /// <param name="marker">The stored quarantine marker.</param>
    /// <param name="authoritative">The authoritative payload envelope.</param>
    /// <param name="optimisticPayloadHash">The optimistic snapshot payload hash.</param>
    /// <returns>A task that represents the asynchronous assertions.</returns>
    private static async Task AssertAuthoritativeSnapshotEvidenceAsync(
        LocalPayloadQuarantineRecord? marker,
        PayloadEnvelope authoritative,
        string optimisticPayloadHash)
    {
        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker?.OperationId).IsNull();
        await Assert.That(marker?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker?.Evidence.SchemaVersion).IsEqualTo(0);
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(authoritative.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(authoritative.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(authoritative.Payload.ToArray())).IsTrue();
        await Assert.That(marker?.Evidence.PayloadHash).IsNotEqualTo(optimisticPayloadHash);
    }

    /// <summary>Asserts that a quarantined recovery returns only the durable guard marker.</summary>
    /// <param name="marker">The stored quarantine marker.</param>
    /// <param name="guardedRecovery">The guarded recovery result after quarantine was written.</param>
    /// <returns>A task that represents the asynchronous assertions.</returns>
    private static async Task AssertQuarantinedRecoveryAsync(LocalPayloadQuarantineRecord? marker, RecoveredStream guardedRecovery)
    {
        await Assert.That(guardedRecovery.Quarantine?.QuarantineId).IsEqualTo(marker?.QuarantineId);
        await Assert.That(guardedRecovery.Snapshot).IsNull();
    }

    /// <summary>Asserts that malformed snapshot schema storage class is quarantined.</summary>
    /// <param name="corruptSchemaVersion">The corruption action.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task AssertSnapshotSchemaStorageClassQuarantines(Action<string> corruptSchemaVersion)
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        }

        corruptSchemaVersion(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var guardedRecovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var leased = await ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker?.Evidence.SchemaVersion).IsNull();
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(snapshot.State.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(snapshot.State.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(snapshot.State.Payload.ToArray())).IsTrue();
        await Assert.That(guardedRecovery.Quarantine?.QuarantineId).IsEqualTo(marker?.QuarantineId);
        await Assert.That(guardedRecovery.Snapshot).IsNull();
        await Assert.That(leased).IsNull();
    }

    /// <summary>Asserts that malformed outbox schema storage class is quarantined.</summary>
    /// <param name="corruptSchemaVersion">The corruption action.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task AssertOutboxSchemaStorageClassQuarantines(Action<string> corruptSchemaVersion)
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        }

        corruptSchemaVersion(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> lease = () => ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1))).AsTask();

        await Assert.That(lease).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        var secondLease = await ReadOptionalLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(marker?.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
        await Assert.That(marker?.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(marker?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker?.Evidence.SchemaVersion).IsNull();
        await Assert.That(marker?.Evidence.PayloadLength).IsEqualTo(operation.Payload.PayloadLength);
        await Assert.That(marker?.Evidence.PayloadHash).IsEqualTo(operation.Payload.PayloadHash);
        await Assert.That(marker?.Evidence.PayloadPrefix.ToArray().SequenceEqual(operation.Payload.Payload.ToArray())).IsTrue();
        await Assert.That(secondLease).IsNull();
    }

    /// <summary>Creates a quarantine request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The quarantine request.</returns>
    private static LocalPayloadQuarantineRequest CreateQuarantineRequest(SubscriptionId subscriptionId, SyncOperation operation) =>
        new()
        {
            StreamId = operation.StreamId,
            SubscriptionId = subscriptionId,
            OperationId = operation.OperationId,
            Source = LocalPayloadQuarantineSource.OutboxOperation,
            Reason = LocalPayloadQuarantineReason.PayloadHashMismatch,
            ReasonCode = "PayloadHashMismatch",
            Envelope = operation.Payload,
            ObservedAtUtc = operation.TimestampUtc,
        };

    /// <summary>Creates a representative snapshot mutation for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(StreamId streamId, long expectedRevision) =>
        new(streamId, CreatePayload("snapshot"), FormatVersion: 1, expectedRevision);

    /// <summary>Corrupts the persisted snapshot schema version so an envelope cannot be constructed.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadSchemaVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_schema_version = 0
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted authoritative snapshot schema version so its envelope cannot be constructed.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptAuthoritativeSnapshotPayloadSchemaVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshot_authoritative_states
            SET payload_schema_version = 0
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted outbox schema version so an envelope cannot be constructed.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptOutboxPayloadSchemaVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload_schema_version = 0
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot schema version to a TEXT value with an integer prefix.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadSchemaVersionTextWithSuffix(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_schema_version = '1garbage'
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot schema version to a REAL fractional value.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadSchemaVersionRealFraction(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_schema_version = 1.5
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted outbox schema version to a TEXT value with an integer prefix.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptOutboxPayloadSchemaVersionTextWithSuffix(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload_schema_version = '1garbage'
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted outbox schema version to a REAL fractional value.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptOutboxPayloadSchemaVersionRealFraction(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload_schema_version = 1.5
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot schema version to a value that cannot fit in Int32.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadSchemaVersionOutOfRange(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_schema_version = $schemaVersion
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$schemaVersion", long.MaxValue);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot payload column type so raw evidence must use fallbacks.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadColumnTypes(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_contract_id = x'ff',
                payload_schema_version = 'not-an-integer',
                payload_content_type = x'fe',
                payload = 123,
                payload_hash = x'fd'
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the quarantine operation identifier for the representative stream.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptQuarantineOperationIdentifier(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_payload_quarantine
            SET operation_id = 'not-a-guid'
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes inserted quarantine markers before the store rereads them.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void DeleteInsertedQuarantineMarkers(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER delete_inserted_quarantine_marker
            AFTER INSERT ON oc_payload_quarantine
            BEGIN
                DELETE FROM oc_payload_quarantine
                WHERE store_identity = NEW.store_identity AND stream_id = NEW.stream_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Adds the shared stream parameters for raw corruption helpers.</summary>
    /// <param name="command">The SQLite command.</param>
    private static void AddStreamParameters(SqliteCommand command)
    {
        _ = command.Parameters.AddWithValue("$storeIdentity", StoreIdentity);
        _ = command.Parameters.AddWithValue("$streamId", Stream.Value);
    }

    /// <summary>Reads an optional leased operation batch from the adapter.</summary>
    /// <param name="adapter">The local store adapter.</param>
    /// <param name="request">The lease request.</param>
    /// <returns>The leased operation batch, if present.</returns>
    private static async ValueTask<LeasedOperationBatch?> ReadOptionalLeaseAsync(SqliteLocalStoreAdapter adapter, OutboxLeaseRequest request)
    {
        LeasedOperationBatch? leased = null;
        await foreach (var batch in adapter.LeasePendingOperationsAsync(request, CancellationToken.None))
        {
            leased = batch;
        }

        return leased;
    }
}
