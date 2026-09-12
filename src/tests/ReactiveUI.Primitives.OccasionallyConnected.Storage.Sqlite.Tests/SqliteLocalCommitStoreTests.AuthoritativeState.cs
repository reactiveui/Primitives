// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Authoritative snapshot state tests.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The first authoritative payload text.</summary>
    private const string AuthoritativeInitialText = "auth-0";

    /// <summary>The replacement authoritative payload text.</summary>
    private const string AuthoritativeRemoteText = "auth-7";

    /// <summary>The changed authoritative payload text.</summary>
    private const string AuthoritativeChangedText = "auth-changed";

    /// <summary>A mismatched canonical SHA-256 payload hash.</summary>
    private const string TamperedCanonicalPayloadHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    /// <summary>A malformed canonical SHA-256 payload hash.</summary>
    private const string MalformedCanonicalPayloadHash = "sha256-malformed";

    /// <summary>The first optimistic payload text.</summary>
    private const string OptimisticInitialText = "optimistic-21";

    /// <summary>The second optimistic payload text.</summary>
    private const string OptimisticLocalText = "optimistic-22";

    /// <summary>The remote optimistic payload text.</summary>
    private const string OptimisticRemoteText = "optimistic-12";

    /// <summary>The first client sequence.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The second snapshot revision.</summary>
    private const int SecondSnapshotRevision = 2;

    /// <summary>The exact schema-five local commit schema before authoritative sidecars existed.</summary>
    private const string PreAuthoritativeLocalCommitSchemaSql = """
        -- Frozen schema v5 from the operation-state local commit stage.
        -- Keep this fixture independent from current schema construction.

        PRAGMA user_version = 5;

        CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);

        CREATE TABLE oc_subscription_identities (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id));

        CREATE TABLE oc_streams (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            subscription_id TEXT NOT NULL,
            next_client_sequence INTEGER NOT NULL,
            server_cursor TEXT NULL,
            PRIMARY KEY (store_identity, stream_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_subscription_identities (store_identity, stream_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_snapshots (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            format_version INTEGER NOT NULL,
            server_cursor TEXT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            revision INTEGER NOT NULL,
            saved_at_utc TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_outbox (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_sequence INTEGER NOT NULL,
            timestamp_utc TEXT NOT NULL,
            base_version TEXT NULL,
            operation_type INTEGER NOT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            policy_delivery_guarantee INTEGER NOT NULL,
            policy_durability INTEGER NOT NULL,
            policy_priority INTEGER NOT NULL,
            policy_conflict INTEGER NOT NULL,
            snapshot_revision INTEGER NOT NULL,
            committed_at_utc TEXT NOT NULL,
            commit_fingerprint BLOB NOT NULL,
            PRIMARY KEY (store_identity, operation_id),
            UNIQUE (store_identity, stream_id, client_sequence),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_outbox_metadata (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (store_identity, operation_id, key),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_inbox (
            store_identity TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            server_cursor TEXT NOT NULL,
            committed_at_utc TEXT NOT NULL,
            PRIMARY KEY (store_identity, stream_id, event_id),
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_outbox_leases (
            store_identity TEXT NOT NULL,
            lease_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_sequence INTEGER NOT NULL,
            lease_expires_at_utc TEXT NOT NULL,
            lease_member_count INTEGER NOT NULL,
            PRIMARY KEY (store_identity, lease_id, operation_id),
            UNIQUE (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON DELETE CASCADE,
            FOREIGN KEY (store_identity, stream_id)
                REFERENCES oc_streams (store_identity, stream_id)
                ON DELETE CASCADE);

        CREATE TABLE oc_outbox_operation_states (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            operation_state INTEGER NOT NULL,
            attempt_count INTEGER NOT NULL,
            changed_at_utc TEXT NOT NULL,
            reason_code TEXT NULL,
            retry_started_utc TEXT NULL,
            retry_due_utc TEXT NULL,
            retry_previous_delay_ticks INTEGER NULL,
            retry_transient_attempt_count INTEGER NULL,
            retry_authentication_state INTEGER NULL,
            retry_credentials_version TEXT NULL,
            PRIMARY KEY (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id)
                REFERENCES oc_outbox (store_identity, operation_id)
                ON UPDATE CASCADE
                ON DELETE CASCADE);

        INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '5');
        """;

    /// <summary>The fixed operation identifier used by the schema-five legacy fixture.</summary>
    private static readonly OperationId LegacySchemaFiveOperationId = new(new("11111111-1111-1111-1111-111111111111"));

    /// <summary>Verifies a local authoritative checkpoint is durable and remains independent from optimistic state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeStateIsCommittedAndStoreReopens_ThenDistinctSnapshotHalvesRecover()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText);

        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies schema version five migration preserves optimistic state and pending work with unknown authoritative state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaFiveMigrates_ThenOptimisticAndPendingRecoverWithUnknownAuthoritativeState()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        var operation = CreateOperation(FirstClientSequence) with { OperationId = LegacySchemaFiveOperationId };
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0);
        var migratedEvent = CreateRemoteEvent(FirstRemoteCursor);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var transaction = connection.BeginTransaction())
        {
            CreatePreAuthoritativeLocalCommitSchema(connection, transaction);
            InsertPreAuthoritativeLocalCommitRows(connection, transaction, subscriptionId, operation, snapshot, migratedEvent);
            transaction.Commit();
        }

        using (var bound = new SqliteLocalCommitStore(database.Path))
        {
            Action initialize = () => bound.Initialize(new(StoreIdentity, SchemaVersion, false) { ClientId = FirstBindingClientId }, CancellationToken.None);
            await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
            await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SqliteStoreSchema.PreAuthoritativeLocalCommitSchemaVersion);
        }

        using var store = CreateInitializedStore(database.Path);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var duplicate = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        var status = store.GetOperationStatus(operation.OperationId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [migratedEvent.EventId], CancellationToken.None);
        var blockedLease = await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        var changedAuthoritative = new Action(() => store.CommitLocalOperation(
            operation,
            snapshot with { AuthoritativeState = CreatePayload(AuthoritativeChangedText) },
            CancellationToken.None));

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(recovery.Snapshot?.AuthoritativeState).IsNull();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(unapplied.Count).IsEqualTo(0);
        await Assert.That(blockedLease).IsNull();
        await Assert.That(duplicate.SnapshotRevision).IsEqualTo(1);
        await Assert.That(changedAuthoritative).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies recovery rejects a tampered authoritative payload hash.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeSnapshotHashIsTampered_ThenRecoveryRejectsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreateCanonicalPayload(AuthoritativeInitialText),
        };
        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);
        SetAuthoritativeSnapshotHash(database.Path, TamperedCanonicalPayloadHash);

        using var reopened = CreateInitializedStore(database.Path);
        var recover = new Action(() => reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None));

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema version six accepted operations retain replay until receive inclusion is known.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaSixMigrates_ThenAcceptedOperationsRecoverAsReplayVisibleUnknownInclusion()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        var subscriptionId = SubscriptionId.New();
        var snapshot = CreateSnapshotMutation(expectedRevision: 0) with
        {
            AuthoritativeState = CreatePayload(AuthoritativeInitialText),
        };
        await using (var connection = OpenRawConnection(database.Path))
        await using (var transaction = connection.BeginTransaction())
        {
            SchemaSixFixture.Create(connection, transaction);
            _ = SqliteClientIdentityBinding.BindOrValidate(connection, transaction, StoreIdentity, FirstBindingClientId);
            InsertLegacyLocalCommitRows(connection, transaction, subscriptionId, operation, snapshot);
            SqliteLocalCommitSql.InsertInitialOperationState(connection, transaction, StoreIdentity, operation, operation.TimestampUtc);
            SetOperationState(connection, transaction, operation.OperationId, SyncOperationState.Synchronized);
            transaction.Commit();
        }

        using var migrated = new SqliteLocalCommitStore(database.Path);
        migrated.Initialize(new(StoreIdentity, SchemaVersion, false) { ClientId = FirstBindingClientId }, CancellationToken.None);
        var recovery = migrated.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies authoritative mutations with invalid canonical hashes are rejected before commit.</summary>
    /// <param name="payloadHash">The invalid canonical payload hash.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(MalformedCanonicalPayloadHash)]
    [Arguments(TamperedCanonicalPayloadHash)]
    public async Task WhenAuthoritativeMutationHashIsInvalid_ThenCommitRejectsIt(string payloadHash)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreatePayloadWithHash(AuthoritativeInitialText, payloadHash),
        };

        var commit = new Action(() => store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None));

        await Assert.That(commit).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-SHA opaque authoritative hashes are not reinterpreted during recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeOpaqueHashHasCanonicalLength_ThenRecoveryAcceptsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreatePayloadWithHash(AuthoritativeInitialText, new('x', TamperedCanonicalPayloadHash.Length)),
        };
        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies local commits without authoritative state preserve the previously stored authoritative checkpoint.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalPublishOmitsAuthoritativeState_ThenPreviousAuthoritativeStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);

        _ = store.CommitLocalOperation(
            CreateOperation(SecondClientSequence),
            CreateMutation(expectedRevision: 1, OptimisticLocalText, authoritativeText: null),
            CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticLocalText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies remote apply replaces authoritative and optimistic state in one durable transaction.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplySuppliesAuthoritativeState_ThenAtomicSnapshotPairRecovers()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);

        var result = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            CreateMutation(expectedRevision: 1, OptimisticRemoteText, AuthoritativeRemoteText),
            CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(result.SnapshotRevision).IsEqualTo(SecondSnapshotRevision);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticRemoteText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
        await Assert.That(recovery.Snapshot?.ServerCursor).IsEqualTo(FirstRemoteCursor);
    }

    /// <summary>Verifies stale and aborted writes leave both snapshot halves unchanged.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeSnapshotWriteFails_ThenBothSnapshotHalvesRemainUnchanged()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);

        Action stale = () => store.CommitLocalOperation(
            CreateOperation(SecondClientSequence),
            CreateMutation(expectedRevision: 0, "optimistic-stale", "auth-stale"),
            CancellationToken.None);
        Action staleRemote = () => store.ApplyRemoteBatch(
            CreateRemoteBatch("wrong-cursor", FirstRemoteCursor, [remoteEvent]),
            CreateMutation(expectedRevision: 1, "optimistic-remote", "auth-remote"),
            CancellationToken.None);

        await Assert.That(stale).ThrowsExactly<InvalidOperationException>();
        await Assert.That(staleRemote).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate local operation intent includes authoritative mutation presence and content.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationChangesAuthoritativeMutation_ThenOriginalIntentRejectsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText);
        var first = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(FirstRemoteCursor)]),
            CreateMutation(expectedRevision: 1, OptimisticRemoteText, AuthoritativeRemoteText),
            CancellationToken.None);

        var replay = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        Action changedAuthoritative = () => store.CommitLocalOperation(
            operation,
            snapshot with { AuthoritativeState = CreatePayload(AuthoritativeChangedText) },
            CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(changedAuthoritative).ThrowsExactly<InvalidOperationException>();
        Action omittedAuthoritative = () => store.CommitLocalOperation(
            operation,
            snapshot with { AuthoritativeState = null },
            CancellationToken.None);
        Action changedOptimistic = () => store.CommitLocalOperation(
            operation,
            snapshot with { State = CreatePayload(OptimisticLocalText) },
            CancellationToken.None);
        await Assert.That(omittedAuthoritative).ThrowsExactly<InvalidOperationException>();
        await Assert.That(changedOptimistic).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
    }

    /// <summary>Verifies inconsistent historical metadata aborts migration without creating authoritative tables.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaFiveMetadataDisagrees_ThenMigrationPreservesHistoricalDatabase()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        await using (var transaction = connection.BeginTransaction())
        {
            CreatePreAuthoritativeLocalCommitSchema(connection, transaction);
            SetSchemaMetadataVersion(connection, transaction, SchemaVersion);
            transaction.Commit();
        }

        Action initialize = () =>
        {
            using var store = CreateInitializedStore(database.Path);
        };
        await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SqliteStoreSchema.PreAuthoritativeLocalCommitSchemaVersion);
        await using var reopened = OpenRawConnection(database.Path);
        await using var command = reopened.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('oc_snapshot_authoritative_states', 'oc_outbox_authoritative_mutations');";
        await Assert.That(Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture)).IsEqualTo(0);
    }

    /// <summary>Creates a snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="optimisticText">The optimistic payload text.</param>
    /// <param name="authoritativeText">The authoritative payload text.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateMutation(long expectedRevision, string optimisticText, string? authoritativeText)
    {
        var authoritativeState = authoritativeText is null ? null : CreatePayload(authoritativeText);
        return new(Stream, CreatePayload(optimisticText), FormatVersion: 1, expectedRevision) { AuthoritativeState = authoritativeState };
    }

    /// <summary>Creates a canonical SHA-256 payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateCanonicalPayload(string text)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(text);
        var hash = $"sha256-{Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(payload))}";
        return CreatePayloadWithHash(payload, hash);
    }

    /// <summary>Creates a payload envelope with an explicit hash.</summary>
    /// <param name="text">The payload text.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreatePayloadWithHash(string text, string payloadHash) =>
        CreatePayloadWithHash(System.Text.Encoding.UTF8.GetBytes(text), payloadHash);

    /// <summary>Creates a payload envelope with explicit payload bytes and hash.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreatePayloadWithHash(byte[] payload, string payloadHash) =>
        new("reading", 1, "application/json", payload, payloadHash);

    /// <summary>Tampers with the current authoritative snapshot payload hash.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <exception cref="InvalidOperationException">The authoritative snapshot sidecar is missing.</exception>
    private static void SetAuthoritativeSnapshotHash(string path, string payloadHash)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshot_authoritative_states
            SET payload_hash = $payloadHash
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$payloadHash", payloadHash);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The authoritative snapshot sidecar was not found.");
    }

    /// <summary>Creates the schema that existed immediately before authoritative sidecars were introduced.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreatePreAuthoritativeLocalCommitSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = PreAuthoritativeLocalCommitSchemaSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts a pre-authoritative local commit row using a fixed old-format intent fingerprint.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshot">The snapshot mutation.</param>
    /// <param name="remoteEvent">The already applied remote event.</param>
    private static void InsertPreAuthoritativeLocalCommitRows(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        SyncOperation operation,
        SnapshotMutation snapshot,
        RemoteEvent remoteEvent)
    {
        SqliteSubscriptionIdentitySql.InsertSubscriptionIdentityIfMissing(connection, transaction, StoreIdentity, Stream, subscriptionId);
        SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, StoreIdentity, Stream, subscriptionId);
        SqliteLocalCommitSql.InsertOutboxOperation(
            connection,
            transaction,
            StoreIdentity,
            operation,
            snapshot.ExpectedRevision + 1,
            CreateLegacySchemaFiveCommitFingerprint(),
            operation.TimestampUtc);
        SqliteLocalCommitSql.InsertOperationMetadata(connection, transaction, StoreIdentity, operation);
        SqliteLocalCommitSql.InsertInitialOperationState(connection, transaction, StoreIdentity, operation, operation.TimestampUtc);
        SqliteLocalCommitSql.InsertInboxEvent(connection, transaction, StoreIdentity, remoteEvent, remoteEvent.CommittedAtUtc);
        InsertPreAuthoritativeLease(connection, transaction, operation);
        SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, StoreIdentity, snapshot, snapshot.ExpectedRevision + 1, null, operation.TimestampUtc);
        SqliteLocalCommitSql.UpdateNextClientSequence(connection, transaction, StoreIdentity, Stream, operation.ClientSequence + 1);
    }

    /// <summary>Inserts an active pre-authoritative lease row for the migrated operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="operation">The operation.</param>
    private static void InsertPreAuthoritativeLease(SqliteConnection connection, SqliteTransaction transaction, SyncOperation operation)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_outbox_leases
                (store_identity, lease_id, operation_id, stream_id, client_sequence, lease_expires_at_utc, lease_member_count)
            VALUES
                ($storeIdentity, $leaseId, $operationId, $streamId, $clientSequence, $leaseExpiresAtUtc, 1);
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$leaseId", Guid.Parse("22222222-2222-2222-2222-222222222222").ToString("D"));
        _ = command.Parameters.AddWithValue("$operationId", operation.OperationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.Parameters.AddWithValue("$clientSequence", operation.ClientSequence);
        _ = command.Parameters.AddWithValue("$leaseExpiresAtUtc", SqliteLocalCommitSql.FormatDateTimeOffset(new(2100, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Returns the fixed old-format schema-five commit fingerprint for the fixture operation and snapshot.</summary>
    /// <returns>The old-format SHA-256 fingerprint bytes.</returns>
    private static byte[] CreateLegacySchemaFiveCommitFingerprint() =>
        [
            0x91, 0xE4, 0xB4, 0x3B, 0x39, 0x20, 0x79, 0xEA,
            0xC2, 0xDD, 0x08, 0x28, 0x8D, 0x60, 0x8D, 0x68,
            0x38, 0x61, 0x3E, 0x87, 0x67, 0x78, 0x75, 0x17,
            0xCC, 0xBE, 0x13, 0x69, 0xE7, 0x9A, 0x98, 0xE7,
        ];

    /// <summary>Reads payload text for test assertions.</summary>
    /// <param name="payload">The optional payload.</param>
    /// <returns>The decoded payload text.</returns>
    private static string? PayloadText(PayloadEnvelope? payload) =>
        payload is null ? null : System.Text.Encoding.UTF8.GetString(payload.Payload.Span);
}
