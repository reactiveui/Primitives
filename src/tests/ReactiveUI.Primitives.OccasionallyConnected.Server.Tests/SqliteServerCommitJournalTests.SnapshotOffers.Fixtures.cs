// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery offer tests.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Creates the snapshot subscription identity.</summary>
    /// <returns>The trusted identity.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ServerSubscriptionIdentity SnapshotSubscription() =>
        SnapshotSubscription(SnapshotSubscriptionText);

    /// <summary>Creates a snapshot subscription identity from a fixed identifier.</summary>
    /// <param name="subscriptionIdText">The subscription identifier text.</param>
    /// <returns>The trusted identity.</returns>
    private static ServerSubscriptionIdentity SnapshotSubscription(string subscriptionIdText) =>
        new(StreamKey(), Client, new(Guid.Parse(subscriptionIdText)));

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The recovery request.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(SubscriptionId subscriptionId) =>
        SnapshotRequest(subscriptionId, []);

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <returns>The recovery request.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(SubscriptionId subscriptionId, IReadOnlyList<SyncOperation> pendingOperations) =>
        SnapshotRequest(subscriptionId, pendingOperations, []);

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <param name="replayOperations">The owned replay-only operations.</param>
    /// <returns>The recovery request.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(
        SubscriptionId subscriptionId,
        IReadOnlyList<SyncOperation> pendingOperations,
        IReadOnlyList<SyncOperation> replayOperations) =>
        SnapshotRequest(subscriptionId, pendingOperations, replayOperations, ServerReceiveGroupCursor.Create(StreamKey(), 0));

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <param name="expiredCursor">The claimed expired cursor.</param>
    /// <returns>The recovery request.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(
        SubscriptionId subscriptionId,
        IReadOnlyList<SyncOperation> pendingOperations,
        string? expiredCursor) =>
        SnapshotRequest(subscriptionId, pendingOperations, [], expiredCursor);

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <param name="replayOperations">The owned replay-only operations.</param>
    /// <param name="expiredCursor">The claimed expired cursor.</param>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(
        SubscriptionId subscriptionId,
        IReadOnlyList<SyncOperation> pendingOperations,
        IReadOnlyList<SyncOperation> replayOperations,
        string? expiredCursor) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = PayloadContract,
            ClientStateSchemaVersion = SingleEntryCount,
            SnapshotFormatVersion = SingleEntryCount,
            PendingOperations = pendingOperations,
            ReplayOperations = replayOperations,
            MaximumResponseBytes = DefaultMaximumLogicalBytes,
        };

    /// <summary>Creates a pending snapshot recovery operation.</summary>
    /// <param name="key">The server operation key.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The pending operation.</returns>
    private static SyncOperation SnapshotOperation(ServerOperationKey key, long sequence) =>
        new()
        {
            OperationId = key.OperationId,
            StreamId = Stream,
            ClientSequence = sequence,
            TimestampUtc = Start,
            BaseVersion = FirstVersion,
            Type = SyncOperationType.Update,
            Payload = Payload($"pending-{sequence}"),
        };

    /// <summary>Creates a retained ledger entry with the canonical fingerprint for the exact pending operation.</summary>
    /// <param name="key">The server operation key.</param>
    /// <param name="operation">The pending operation used by the recovery request.</param>
    /// <param name="kind">The terminal operation result kind.</param>
    /// <returns>The retained ledger entry.</returns>
    private static ServerLedgerEntry SnapshotEntry(ServerOperationKey key, SyncOperation operation, OperationResultKind kind) =>
        new(
            key,
            new(CanonicalOperationFingerprint.Compute(Tenant, key.ClientId, operation, SnapshotFingerprintBudget)),
            new(key.OperationId, kind, null, FirstVersion),
            [],
            []);

    /// <summary>Creates a non-matching commit fingerprint.</summary>
    /// <returns>The fingerprint.</returns>
    private static ServerCommitFingerprint MismatchedFingerprint() => new(new byte[ServerCommitFingerprint.Length]);

    /// <summary>Creates the client-visible disposition corresponding to a trusted server proof.</summary>
    /// <param name="proof">The server proof captured in the recovery view.</param>
    /// <returns>The client-visible operation disposition.</returns>
    private static SnapshotOperationDisposition CreateClientDisposition(ServerSnapshotOperationDisposition proof) =>
        new() { OperationId = proof.OperationId, Kind = proof.Kind, Result = proof.Result };

    /// <summary>Creates a structurally valid recovered result.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="snapshot">The captured snapshot frontier.</param>
    /// <returns>The recovery result.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryResult SnapshotRecoveryResult(SubscriptionId subscriptionId, ServerCommitSnapshot snapshot) =>
        SnapshotRecoveryResult(subscriptionId, snapshot, []);

    /// <summary>Creates a structurally valid recovered result.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="snapshot">The captured snapshot frontier.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The recovery result.</returns>
    private static RemoteSnapshotRecoveryResult SnapshotRecoveryResult(
        SubscriptionId subscriptionId,
        ServerCommitSnapshot snapshot,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = new()
            {
                StreamId = Stream,
                SubscriptionId = subscriptionId,
                FrontierCursor = snapshot.LastCursor
                    ?? ServerReceiveGroupCursor.Create(snapshot.StreamKey, snapshot.LastGroupSequence),
                ServerVersion = snapshot.State?.Version ?? string.Empty,
                SnapshotFormatVersion = SingleEntryCount,
                ClientState = Payload("snapshot"),
                ObservedAtUtc = Start,
            },
            OperationDispositions = dispositions,
        };

    /// <summary>Creates the retained source view for a snapshot offer.</summary>
    /// <param name="snapshot">The stream snapshot.</param>
    /// <param name="state">The optional subscription state.</param>
    /// <returns>The retained view.</returns>
    private static ServerSnapshotRecoveryView SnapshotView(ServerCommitSnapshot snapshot, ServerSubscriptionState? state) =>
        new()
        {
            Snapshot = snapshot,
            SubscriptionState = state,
            ExpiredCursorOffer = null,
            RequestedExpiredCursor = ServerReceiveGroupCursor.Create(StreamKey(), 0),
            CapturedPendingOperationCount = 0,
            OperationDispositions = [],
            OperationFingerprints = [],
        };

    /// <summary>Creates finite structural snapshot recovery limits for tests.</summary>
    /// <returns>The limits.</returns>
    private static SnapshotRecoveryLimits SnapshotLimits() => new() { MaximumLogicalBytes = DefaultMaximumLogicalBytes };

    /// <summary>Creates a structurally valid snapshot offer request.</summary>
    /// <param name="identity">The trusted subscription identity.</param>
    /// <param name="view">The captured recovery view.</param>
    /// <param name="request">The recovery request bound to the view.</param>
    /// <param name="snapshot">The captured stream snapshot.</param>
    /// <returns>The offer request.</returns>
    private static ServerSnapshotOfferRequest CreateSnapshotOffer(
        ServerSubscriptionIdentity identity,
        ServerSnapshotRecoveryView view,
        RemoteSnapshotRecoveryRequest request,
        ServerCommitSnapshot snapshot) =>
        new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot),
            Limits = SnapshotLimits(),
        };

    /// <summary>Seeds two committed groups and retains a normal offer for the first group.</summary>
    /// <param name="journal">The journal to seed.</param>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The stream snapshot after the second group.</returns>
    /// <exception cref="InvalidOperationException">The first page is missing.</exception>
    private static ServerCommitSnapshot SeedTwoCommitsAndOfferFirstPage(
        SqliteServerCommitJournal journal,
        ServerSubscriptionIdentity identity)
    {
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(
            SingleEntryCount,
            State(SecondVersion),
            Stamp(second),
            Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        return journal.Read(StreamKey(), [first, second]);
    }

    /// <summary>Seeds one committed group and retains a normal offer for that group.</summary>
    /// <param name="journal">The journal to seed.</param>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The stream snapshot after the first group.</returns>
    /// <exception cref="InvalidOperationException">The first page is missing.</exception>
    private static ServerCommitSnapshot SeedOneCommitAndOfferFirstPage(
        SqliteServerCommitJournal journal,
        ServerSubscriptionIdentity identity)
    {
        var key = OperationKey(FirstOperationSeed);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        return journal.Read(StreamKey(), [key]);
    }

    /// <summary>Reads the durable subscription generation high-water metadata value.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The metadata value.</returns>
    /// <exception cref="InvalidOperationException">The metadata value is missing.</exception>
    private static long ReadSubscriptionGenerationHighWater(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM oc_server_journal_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", SubscriptionGenerationHighWaterMetadataKey);
        var value = command.ExecuteScalar();
        return value is string text
            ? long.Parse(text, System.Globalization.CultureInfo.InvariantCulture)
            : throw new InvalidOperationException("The generation metadata value is missing.");
    }

    /// <summary>Writes the durable subscription generation high-water metadata value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="value">The metadata value.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static void WriteSubscriptionGenerationHighWater(string path, long value) =>
        WriteSubscriptionGenerationHighWater(path, value.ToString(System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>Writes the durable subscription generation high-water metadata value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="value">The metadata value.</param>
    private static void WriteSubscriptionGenerationHighWater(string path, string value)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_metadata SET value = $value WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", SubscriptionGenerationHighWaterMetadataKey);
        _ = command.Parameters.AddWithValue("$value", value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes one subscription and the current-schema generation high-water metadata.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="subscriptionId">The deleted subscription id.</param>
    private static void DeleteSubscriptionAndGenerationHighWater(string path, SubscriptionId subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = $$"""
            DELETE FROM oc_server_journal_subscriptions
            WHERE subscription_id = {{RawSubscriptionIdParameterName}};
            DELETE FROM oc_server_journal_metadata
            WHERE key = {{RawGenerationKeyParameterName}};
            """;
        _ = command.Parameters.AddWithValue(RawSubscriptionIdParameterName, subscriptionId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(RawGenerationKeyParameterName, SubscriptionGenerationHighWaterMetadataKey);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Increments a retained subscription generation without rewriting its existing offers.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    private static void IncrementSubscriptionGeneration(string path, SubscriptionId subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = $$"""
            UPDATE oc_server_journal_subscriptions
            SET generation = generation + 1
            WHERE subscription_id = {{RawSubscriptionIdParameterName}};
            UPDATE oc_server_journal_metadata
            SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
            WHERE key = {{RawGenerationKeyParameterName}};
            """;
        _ = command.Parameters.AddWithValue(RawSubscriptionIdParameterName, subscriptionId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue(RawGenerationKeyParameterName, SubscriptionGenerationHighWaterMetadataKey);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Writes a retained subscription revision directly for overflow coverage.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="revision">The revision value.</param>
    private static void WriteSubscriptionRevision(string path, SubscriptionId subscriptionId, long revision)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = $$"""
            UPDATE oc_server_journal_subscriptions
            SET revision = $revision
            WHERE subscription_id = {{RawSubscriptionIdParameterName}};
            """;
        _ = command.Parameters.AddWithValue(RawSubscriptionIdParameterName, subscriptionId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue("$revision", revision);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that deletes a subscription before its revision is updated.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteSubscriptionBeforeRevisionUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_snapshot_delete_before_revision_update
            BEFORE UPDATE OF revision ON oc_server_journal_subscriptions
            BEGIN
                DELETE FROM oc_server_journal_subscriptions WHERE subscription_id = OLD.subscription_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the trigger that deletes a subscription before its revision is updated.</summary>
    /// <param name="path">The database path.</param>
    private static void DropDeleteSubscriptionBeforeRevisionUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_server_snapshot_delete_before_revision_update;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Writes the server journal metadata schema version value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="version">The metadata schema version.</param>
    private static void WriteSchemaVersionMetadata(string path, string version)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_metadata SET value = $version WHERE key = 'schema_version';";
        _ = command.Parameters.AddWithValue("$version", version);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Replaces schema-four metadata storage with a malformed table.</summary>
    /// <param name="path">The database path.</param>
    private static void CorruptSchemaFourMetadataTable(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TABLE oc_server_journal_metadata;
            CREATE TABLE oc_server_journal_metadata (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Rewrites current subscription tables to schema four while preserving retained subscription rows.</summary>
    /// <param name="path">The database path.</param>
    private static void RewriteSubscriptionsAsSchemaFour(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = $$"""
            DROP TABLE oc_server_journal_subscription_offers;
            ALTER TABLE oc_server_journal_subscriptions RENAME TO oc_server_journal_subscriptions_v5;
            CREATE TABLE oc_server_journal_subscriptions (
                subscription_id TEXT NOT NULL PRIMARY KEY, tenant_id TEXT NOT NULL, stream_id TEXT NOT NULL, client_id TEXT NOT NULL,
                initial_position_kind INTEGER NOT NULL, initial_sequence INTEGER NULL, initial_timestamp_utc TEXT NULL,
                initial_cursor TEXT NULL, initial_anchor_cursor TEXT NULL, initial_anchor_group_sequence INTEGER NOT NULL,
                initial_anchor_resolved INTEGER NOT NULL, acknowledged_cursor TEXT NULL, acknowledged_group_sequence INTEGER NOT NULL,
                latest_offered_cursor TEXT NULL, latest_offered_group_sequence INTEGER NOT NULL, acknowledged_at_utc TEXT NULL,
                updated_at_utc TEXT NOT NULL, last_touched_utc TEXT NOT NULL, logical_bytes INTEGER NOT NULL);
            INSERT INTO oc_server_journal_subscriptions
            SELECT subscription_id, tenant_id, stream_id, client_id, initial_position_kind, initial_sequence,
                   initial_timestamp_utc, initial_cursor, initial_anchor_cursor, initial_anchor_group_sequence,
                   initial_anchor_resolved, acknowledged_cursor, acknowledged_group_sequence, latest_offered_cursor,
                   latest_offered_group_sequence, acknowledged_at_utc, updated_at_utc, last_touched_utc, logical_bytes
            FROM oc_server_journal_subscriptions_v5;
            DROP TABLE oc_server_journal_subscriptions_v5;
            CREATE TABLE oc_server_journal_subscription_offers (
                subscription_id TEXT NOT NULL, cursor TEXT NOT NULL, group_sequence INTEGER NOT NULL,
                offered_at_utc TEXT NOT NULL, logical_bytes INTEGER NOT NULL, PRIMARY KEY (subscription_id, cursor),
                FOREIGN KEY (subscription_id) REFERENCES oc_server_journal_subscriptions (subscription_id) ON DELETE CASCADE);
            DELETE FROM oc_server_journal_metadata WHERE key = {{RawGenerationKeyParameterName}};
            UPDATE oc_server_journal_metadata SET value = $schemaVersion WHERE key = 'schema_version';
            PRAGMA user_version = 4;
            """;
        _ = command.Parameters.AddWithValue(RawGenerationKeyParameterName, SubscriptionGenerationHighWaterMetadataKey);
        _ = command.Parameters.AddWithValue("$schemaVersion", SnapshotOfferSchemaFourVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        _ = command.ExecuteNonQuery();
    }
}
