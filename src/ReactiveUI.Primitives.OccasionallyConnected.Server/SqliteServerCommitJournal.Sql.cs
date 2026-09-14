// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides static SQLite creation and write helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Computes the retained cursor byte delta for a commit.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The retained cursor byte delta.</returns>
    private static long GetLastCursorDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.LastCursor is null ? 0 : commit.LastCursorBytes - stream.LastCursorBytes;

    /// <summary>Reads the durable subscription generation high-water value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The current high-water value.</returns>
    /// <exception cref="InvalidOperationException">The stored generation value is invalid.</exception>
    private static long ReadSubscriptionGenerationHighWater(SqliteConnection connection, SqliteTransaction transaction)
    {
        var value = SelectMetadata(connection, transaction, SubscriptionGenerationHighWaterKey);
        return long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var generation) && generation >= 0
            ? generation
            : throw new InvalidOperationException("The SQLite server subscription generation high-water value is invalid.");
    }

    /// <summary>Writes the durable subscription generation high-water value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="generation">The high-water value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteSubscriptionGenerationHighWater(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long generation) =>
        WriteMetadataValue(connection, transaction, SubscriptionGenerationHighWaterKey, generation.ToString(CultureInfo.InvariantCulture));

    /// <summary>Creates the SQLite schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateSchema(SqliteConnection connection, SqliteTransaction transaction)
    {
        SetUserVersion(connection, transaction);
        CreateMetadataTable(connection, transaction);
        CreateStreamsTable(connection, transaction);
        CreateLedgerTable(connection, transaction);
        CreateConflictsTable(connection, transaction);
        CreateEventsTable(connection, transaction);
        CreateEventMetadataTable(connection, transaction);
        CreateSubscriptionsTable(connection, transaction);
        CreateSubscriptionOffersTable(connection, transaction);
        InsertMetadata(connection, transaction, SchemaVersionKey, CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture));
        InsertMetadata(connection, transaction, LatestUtcKey, FormatDateTimeOffset(DateTimeOffset.MinValue));
        InsertMetadata(connection, transaction, SubscriptionGenerationHighWaterKey, "0");
    }

    /// <summary>Validates the current durable schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateExistingSchema(SqliteConnection connection, SqliteTransaction transaction) =>
        ValidateExistingSchema(connection, transaction, GetUserVersion(connection, transaction));

    /// <summary>Validates the current durable schema.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="userVersion">The SQLite user version.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateExistingSchema(SqliteConnection connection, SqliteTransaction transaction, long userVersion)
    {
        if (userVersion != CurrentSchemaVersion)
        {
            throw new InvalidOperationException("The SQLite server journal schema version is not supported.");
        }

        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
                SubscriptionOffersTableName,
                SubscriptionsTableName,
            ]);
        ValidateTableDefinition(connection, transaction, MetadataTableName, MetadataTableSql);
        ValidateTableDefinition(connection, transaction, StreamsTableName, StreamsTableSql);
        ValidateTableDefinition(connection, transaction, LedgerTableName, LedgerTableSql);
        ValidateTableDefinition(connection, transaction, ConflictsTableName, ConflictsTableSql);
        ValidateTableDefinition(connection, transaction, EventsTableName, EventsTableSql);
        ValidateTableDefinition(connection, transaction, EventMetadataTableName, EventMetadataTableSql);
        ValidateTableDefinition(connection, transaction, SubscriptionsTableName, SubscriptionsTableSql);
        ValidateTableDefinition(connection, transaction, SubscriptionOffersTableName, SubscriptionOffersTableSql);
        var metadataSchemaVersion = SelectMetadata(connection, transaction, SchemaVersionKey);
        if (metadataSchemaVersion == CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture))
        {
            _ = ReadSubscriptionGenerationHighWater(connection, transaction);
            return;
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Upserts one stream after admission.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The current stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void UpsertStream(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream,
        ServerCommitValidationResult commit)
    {
        ServerCommitJournalOperations.ApplyState(stream, commit);
        var lastCursor = commit.LastCursor ?? stream.LastCursor;
        var lastCursorBytes = commit.LastCursor is null ? stream.LastCursorBytes : commit.LastCursorBytes;
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_server_journal_streams
            SET revision = $revision,
                state_version = $stateVersion,
                state_payload_contract_id = $statePayloadContractId,
                state_payload_schema_version = $statePayloadSchemaVersion,
                state_payload_content_type = $statePayloadContentType,
                state_payload = $statePayload,
                state_payload_hash = $statePayloadHash,
                write_stamp_committed_at_utc = $writeStampCommittedAtUtc,
                write_stamp_client_id = $writeStampClientId,
                write_stamp_operation_id = $writeStampOperationId,
                last_cursor = $lastCursor,
                last_event_sequence = $lastEventSequence,
                state_bytes = $stateBytes,
                last_cursor_bytes = $lastCursorBytes,
                last_group_sequence = $lastGroupSequence,
                receive_history_incomplete = $receiveHistoryIncomplete
            WHERE tenant_id = $tenantId AND stream_id = $streamId;
            """;
        AddStreamParameters(command, streamKey);
        AddNullableStateParameters(command, stream.State, stream.StateBytes);
        AddNullableWriteStampParameters(command, stream.LastWriteStamp);
        _ = command.Parameters.AddWithValue("$revision", checked(stream.Revision + 1));
        _ = command.Parameters.AddWithValue("$lastCursor", (object?)lastCursor ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$lastEventSequence", checked(stream.LastEventSequence + commit.EventCount));
        _ = command.Parameters.AddWithValue("$lastCursorBytes", lastCursorBytes);
        _ = command.Parameters.AddWithValue("$lastGroupSequence", checked(stream.LastGroupSequence + commit.Entries.Length));
        _ = command.Parameters.AddWithValue("$receiveHistoryIncomplete", Convert.ToInt32(stream.HasReceiveHistoryGap));
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite server journal stream row is missing.");
    }

    /// <summary>Inserts a stream shell.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    private static void InsertStream(SqliteConnection connection, SqliteTransaction transaction, ServerStreamKey streamKey)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_streams
                (tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
                 state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc, write_stamp_client_id,
                 write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes, last_cursor_bytes,
                 last_group_sequence, receive_history_incomplete)
            VALUES
                ($tenantId, $streamId, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0);
            """;
        AddStreamParameters(command, streamKey);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts committed ledger rows and sidecars.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entries">The committed entries.</param>
    /// <param name="entryBytes">The logical bytes per entry.</param>
    private static void InsertLedger(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry[] entries,
        long[] entryBytes)
    {
        var nextEventSequence = ReadLastEventSequence(connection, transaction, streamKey);
        var nextGroupSequence = ReadLastGroupSequence(connection, transaction, streamKey);
        for (var index = 0; index < entries.Length; index++)
        {
            nextGroupSequence = checked(nextGroupSequence + 1);
            InsertLedgerEntry(connection, transaction, streamKey, entries[index], entryBytes[index], nextGroupSequence);
            InsertConflicts(connection, transaction, streamKey, entries[index]);
            nextEventSequence = InsertEvents(connection, transaction, streamKey, entries[index], nextEventSequence);
        }
    }

    /// <summary>Inserts one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="logicalBytes">The retained logical bytes.</param>
    /// <param name="groupSequence">The receive group sequence.</param>
    private static void InsertLedgerEntry(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long logicalBytes,
        long groupSequence)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_ledger
                (tenant_id, stream_id, client_id, operation_id, fingerprint, result_kind, result_reason_code,
                 result_server_version, committed_at_utc, expires_at_utc, logical_bytes, group_sequence)
            VALUES
                ($tenantId, $streamId, $clientId, $operationId, $fingerprint, $resultKind, $resultReasonCode,
                 $resultServerVersion, $committedAtUtc, $expiresAtUtc, $logicalBytes, $groupSequence);
            """;
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, entry.OperationKey);
        AddFingerprintParameter(command, entry.Fingerprint);
        _ = command.Parameters.AddWithValue("$resultKind", (int)entry.Result.Kind);
        _ = command.Parameters.AddWithValue("$resultReasonCode", (object?)entry.Result.ReasonCode ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$resultServerVersion", (object?)entry.Result.ServerVersion ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$committedAtUtc", FormatDateTimeOffset(entry.CommittedAtUtc));
        _ = command.Parameters.AddWithValue("$expiresAtUtc", FormatDateTimeOffset(entry.ExpiresAtUtc));
        _ = command.Parameters.AddWithValue("$logicalBytes", logicalBytes);
        _ = command.Parameters.AddWithValue("$groupSequence", groupSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Migrates schema-one journals without fabricating receive group order.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void MigrateSchemaOneToTwo(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateSchemaOneForMigration(connection, transaction);
        RenameSchemaOneTables(connection, transaction);
        CreateStreamsTable(connection, transaction);
        CreateLedgerTable(connection, transaction);
        CreateConflictsTable(connection, transaction);
        CreateEventsTable(connection, transaction);
        CreateEventMetadataTable(connection, transaction);
        CopySchemaOneRows(connection, transaction);
        DropSchemaOneTables(connection, transaction);
        WriteMetadataValue(connection, transaction, SchemaVersionKey, SchemaVersionTwo.ToString(CultureInfo.InvariantCulture));
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SetSchemaVersionTwoSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Migrates schema-two journals by adding subscription acknowledgement tables.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void MigrateSchemaTwoToThree(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateSchemaTwoForMigration(connection, transaction);
        CreateSchemaThreeSubscriptionsTable(connection, transaction);
        CreateSchemaFourSubscriptionOffersTable(connection, transaction);
        WriteMetadataValue(connection, transaction, SchemaVersionKey, SchemaVersionThree.ToString(CultureInfo.InvariantCulture));
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = SetSchemaVersionThreeSql;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Migrates schema-three journals by adding durable subscription start-position fields.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void MigrateSchemaThreeToFour(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateSchemaThreeForMigration(connection, transaction);
        AddSubscriptionStartPositionColumns(connection, transaction);
        WriteMetadataValue(connection, transaction, SchemaVersionKey, SchemaVersionFour.ToString(CultureInfo.InvariantCulture));
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "PRAGMA user_version = 4;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Migrates schema-four journals by adding durable snapshot-offer proof fields.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void MigrateSchemaFourToFive(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateSchemaFourForMigration(connection, transaction);
        AddSnapshotOfferColumns(connection, transaction);
        WriteMetadataValue(connection, transaction, SchemaVersionKey, CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture));
        SetUserVersion(connection, transaction);
    }

    /// <summary>Validates the schema-four durable table set before migration.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateSchemaFourForMigration(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
                SubscriptionOffersTableName,
                SubscriptionsTableName,
            ]);
        try
        {
            if (SelectMetadata(connection, transaction, SchemaVersionKey) == SchemaVersionFour.ToString(CultureInfo.InvariantCulture))
            {
                ValidateTableDefinition(connection, transaction, SubscriptionsTableName, SchemaFourSubscriptionsTableSql);
                ValidateTableDefinition(connection, transaction, SubscriptionOffersTableName, SchemaFourSubscriptionOffersTableSql);
                return;
            }
        }
        catch (SqliteException exception)
        {
            throw new InvalidOperationException(InvalidSchemaMessage, exception);
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Adds durable snapshot-offer proof columns to schema-four journals.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void AddSnapshotOfferColumns(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            ALTER TABLE oc_server_journal_subscriptions ADD COLUMN generation INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE oc_server_journal_subscriptions ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;
            UPDATE oc_server_journal_subscriptions SET generation = rowid WHERE generation = 0;
            UPDATE oc_server_journal_subscriptions SET logical_bytes = logical_bytes + $migrationLogicalBytes;
            INSERT INTO oc_server_journal_metadata (key, value)
            SELECT $subscriptionGenerationHighWaterKey, CAST(COALESCE(MAX(generation), 0) AS TEXT)
            FROM oc_server_journal_subscriptions;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_stream_revision INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_last_event_sequence INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_subscription_generation INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_originating_subscription_revision INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_issued_subscription_revision INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_format_version INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_client_state_payload_contract_id TEXT NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_client_state_payload_schema_version INTEGER NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_client_state_payload_content_type TEXT NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_client_state_payload BLOB NULL;
            ALTER TABLE oc_server_journal_subscription_offers ADD COLUMN snapshot_client_state_payload_hash TEXT NULL;
            """;
        _ = command.Parameters.AddWithValue("$migrationLogicalBytes", ServerSubscriptionJournalOperations.GetSnapshotOfferMigrationBytes());
        _ = command.Parameters.AddWithValue("$subscriptionGenerationHighWaterKey", SubscriptionGenerationHighWaterKey);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Validates the schema-three durable table set before migration.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateSchemaThreeForMigration(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
                SubscriptionOffersTableName,
                SubscriptionsTableName,
            ]);
        try
        {
            if (SelectMetadata(connection, transaction, SchemaVersionKey) == SchemaVersionThree.ToString(CultureInfo.InvariantCulture))
            {
                ValidateTableDefinition(connection, transaction, SubscriptionsTableName, SchemaThreeSubscriptionsTableSql);
                ValidateTableDefinition(connection, transaction, SubscriptionOffersTableName, SchemaFourSubscriptionOffersTableSql);
                return;
            }
        }
        catch (SqliteException exception)
        {
            throw new InvalidOperationException(InvalidSchemaMessage, exception);
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Rebuilds schema-three subscription rows with schema-four initial position columns.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void AddSubscriptionStartPositionColumns(SqliteConnection connection, SqliteTransaction transaction)
    {
        RenameSubscriptionTablesForSchemaFourMigration(connection, transaction);
        CreateSchemaFourSubscriptionsTable(connection, transaction);
        CreateSchemaFourSubscriptionOffersTable(connection, transaction);
        CopySchemaThreeSubscriptions(connection, transaction);
        CopySchemaThreeSubscriptionOffers(connection, transaction);
        DropSchemaThreeSubscriptionTables(connection, transaction);
    }

    /// <summary>Renames schema-three subscription tables before rebuilding them.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void RenameSubscriptionTablesForSchemaFourMigration(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            ALTER TABLE oc_server_journal_subscription_offers RENAME TO oc_server_journal_subscription_offers_v3;
            ALTER TABLE oc_server_journal_subscriptions RENAME TO oc_server_journal_subscriptions_v3;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Copies schema-three subscription rows into the schema-four table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CopySchemaThreeSubscriptions(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_subscriptions
                (subscription_id, tenant_id, stream_id, client_id, initial_position_kind, initial_sequence, initial_timestamp_utc,
                 initial_cursor, initial_anchor_cursor, initial_anchor_group_sequence, initial_anchor_resolved,
                 acknowledged_cursor, acknowledged_group_sequence, latest_offered_cursor, latest_offered_group_sequence,
                 acknowledged_at_utc, updated_at_utc, last_touched_utc, logical_bytes)
            SELECT subscription_id, tenant_id, stream_id, client_id, 2, 0, NULL, NULL, NULL, 0, 1,
                   acknowledged_cursor, acknowledged_group_sequence, latest_offered_cursor, latest_offered_group_sequence,
                   acknowledged_at_utc, updated_at_utc, last_touched_utc, logical_bytes + $migrationLogicalBytes
            FROM oc_server_journal_subscriptions_v3;
            """;
        _ = command.Parameters.AddWithValue("$migrationLogicalBytes", ServerSubscriptionJournalOperations.GetInitialPositionMigrationBytes());
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Copies schema-three offer rows into the schema-four offer table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CopySchemaThreeSubscriptionOffers(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_subscription_offers (subscription_id, cursor, group_sequence, offered_at_utc, logical_bytes)
            SELECT subscription_id, cursor, group_sequence, offered_at_utc, logical_bytes
            FROM oc_server_journal_subscription_offers_v3;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops schema-three subscription tables after rebuilding them.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void DropSchemaThreeSubscriptionTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DROP TABLE oc_server_journal_subscription_offers_v3;
            DROP TABLE oc_server_journal_subscriptions_v3;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Validates the schema-two durable table set before migration.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateSchemaTwoForMigration(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
            ]);
        try
        {
            if (SelectMetadata(connection, transaction, SchemaVersionKey) == "2")
            {
                return;
            }
        }
        catch (SqliteException exception)
        {
            throw new InvalidOperationException(InvalidSchemaMessage, exception);
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Validates the schema-one durable table set before migration.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <exception cref="InvalidOperationException">Thrown when SQLite data or schema validation fails.</exception>
    private static void ValidateSchemaOneForMigration(SqliteConnection connection, SqliteTransaction transaction)
    {
        ValidateUserTableNames(
            connection,
            transaction,
            [
                ConflictsTableName,
                EventMetadataTableName,
                EventsTableName,
                LedgerTableName,
                MetadataTableName,
                StreamsTableName,
            ]);
        try
        {
            if (SelectMetadata(connection, transaction, SchemaVersionKey) == "1")
            {
                return;
            }
        }
        catch (SqliteException exception)
        {
            throw new InvalidOperationException(InvalidSchemaMessage, exception);
        }

        throw new InvalidOperationException(UnsupportedMetadataSchemaVersionMessage);
    }

    /// <summary>Renames schema-one tables before creating exact schema-two replacements.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void RenameSchemaOneTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            ALTER TABLE oc_server_journal_event_metadata RENAME TO oc_server_journal_event_metadata_v1;
            ALTER TABLE oc_server_journal_events RENAME TO oc_server_journal_events_v1;
            ALTER TABLE oc_server_journal_conflicts RENAME TO oc_server_journal_conflicts_v1;
            ALTER TABLE oc_server_journal_ledger RENAME TO oc_server_journal_ledger_v1;
            ALTER TABLE oc_server_journal_streams RENAME TO oc_server_journal_streams_v1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Copies schema-one rows into schema-two tables without fabricating group sequences.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CopySchemaOneRows(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_streams
                (tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
                 state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc,
                 write_stamp_client_id, write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes,
                 last_cursor_bytes, last_group_sequence, receive_history_incomplete)
            SELECT tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
                   state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc,
                   write_stamp_client_id, write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes,
                   last_cursor_bytes, 0, 1
            FROM oc_server_journal_streams_v1;
            INSERT INTO oc_server_journal_ledger
                (tenant_id, stream_id, client_id, operation_id, fingerprint, result_kind, result_reason_code,
                 result_server_version, committed_at_utc, expires_at_utc, logical_bytes, group_sequence)
            SELECT tenant_id, stream_id, client_id, operation_id, fingerprint, result_kind, result_reason_code,
                   result_server_version, committed_at_utc, expires_at_utc, logical_bytes, NULL
            FROM oc_server_journal_ledger_v1;
            INSERT INTO oc_server_journal_conflicts
                (tenant_id, stream_id, client_id, operation_id, conflict_index, resolution_code,
                 resolved_payload_contract_id, resolved_payload_schema_version, resolved_payload_content_type,
                 resolved_payload, resolved_payload_hash)
            SELECT tenant_id, stream_id, client_id, operation_id, conflict_index, resolution_code,
                   resolved_payload_contract_id, resolved_payload_schema_version, resolved_payload_content_type,
                   resolved_payload, resolved_payload_hash
            FROM oc_server_journal_conflicts_v1;
            INSERT INTO oc_server_journal_events
                (tenant_id, stream_id, event_sequence, client_id, operation_id, event_index, event_id, server_cursor,
                 committed_at_utc, caused_by_operation_id, origin_client_id, origin_operation_id, payload_contract_id,
                 payload_schema_version, payload_content_type, payload, payload_hash)
            SELECT tenant_id, stream_id, event_sequence, client_id, operation_id, event_index, event_id, server_cursor,
                   committed_at_utc, caused_by_operation_id, origin_client_id, origin_operation_id, payload_contract_id,
                   payload_schema_version, payload_content_type, payload, payload_hash
            FROM oc_server_journal_events_v1;
            INSERT INTO oc_server_journal_event_metadata (tenant_id, stream_id, event_sequence, key, value)
            SELECT tenant_id, stream_id, event_sequence, key, value
            FROM oc_server_journal_event_metadata_v1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops schema-one renamed tables after copying rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void DropSchemaOneTables(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            DROP TABLE oc_server_journal_event_metadata_v1;
            DROP TABLE oc_server_journal_events_v1;
            DROP TABLE oc_server_journal_conflicts_v1;
            DROP TABLE oc_server_journal_ledger_v1;
            DROP TABLE oc_server_journal_streams_v1;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts conflict sidecars for one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    private static void InsertConflicts(SqliteConnection connection, SqliteTransaction transaction, ServerStreamKey streamKey, ServerLedgerEntry entry)
    {
        for (var index = 0; index < entry.Conflicts.Count; index++)
        {
            var conflict = entry.Conflicts[index];
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO oc_server_journal_conflicts
                    (tenant_id, stream_id, client_id, operation_id, conflict_index, resolution_code,
                     resolved_payload_contract_id, resolved_payload_schema_version, resolved_payload_content_type,
                     resolved_payload, resolved_payload_hash)
                VALUES
                    ($tenantId, $streamId, $clientId, $operationId, $conflictIndex, $resolutionCode,
                     $resolvedPayloadContractId, $resolvedPayloadSchemaVersion, $resolvedPayloadContentType,
                     $resolvedPayload, $resolvedPayloadHash);
                """;
            AddStreamParameters(command, streamKey);
            AddOperationParameters(command, entry.OperationKey);
            _ = command.Parameters.AddWithValue("$conflictIndex", index);
            _ = command.Parameters.AddWithValue("$resolutionCode", conflict.ResolutionCode);
            AddNullablePayloadParameters(command, "resolved", conflict.ResolvedPayload);
            _ = command.ExecuteNonQuery();
        }
    }

    /// <summary>Inserts event sidecars for one ledger row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="nextEventSequence">The last event sequence.</param>
    /// <returns>The new last event sequence.</returns>
    private static long InsertEvents(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long nextEventSequence)
    {
        for (var index = 0; index < entry.Events.Count; index++)
        {
            nextEventSequence = checked(nextEventSequence + 1);
            var remoteEvent = entry.Events[index];
            InsertEvent(connection, transaction, streamKey, entry.OperationKey, remoteEvent, index, nextEventSequence);
            InsertEventMetadata(connection, transaction, streamKey, nextEventSequence, remoteEvent);
        }

        return nextEventSequence;
    }

    /// <summary>Inserts one event row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="operationKey">The operation key.</param>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="eventIndex">The event index inside the entry.</param>
    /// <param name="eventSequence">The stream event sequence.</param>
    private static void InsertEvent(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        ServerOperationKey operationKey,
        RemoteEvent remoteEvent,
        int eventIndex,
        long eventSequence)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_events
                (tenant_id, stream_id, event_sequence, client_id, operation_id, event_index, event_id, server_cursor,
                 committed_at_utc, caused_by_operation_id, origin_client_id, origin_operation_id, payload_contract_id,
                 payload_schema_version, payload_content_type, payload, payload_hash)
            VALUES
                ($tenantId, $streamId, $eventSequence, $clientId, $operationId, $eventIndex, $eventId, $serverCursor,
                 $committedAtUtc, $causedByOperationId, $originClientId, $originOperationId, $payloadContractId,
                 $payloadSchemaVersion, $payloadContentType, $payload, $payloadHash);
            """;
        AddStreamParameters(command, streamKey);
        AddOperationParameters(command, operationKey);
        _ = command.Parameters.AddWithValue(EventSequenceParameterName, eventSequence);
        _ = command.Parameters.AddWithValue("$eventIndex", eventIndex);
        _ = command.Parameters.AddWithValue("$eventId", remoteEvent.EventId.ToString("D"));
        _ = command.Parameters.AddWithValue("$serverCursor", remoteEvent.ServerCursor);
        _ = command.Parameters.AddWithValue("$committedAtUtc", FormatDateTimeOffset(remoteEvent.CommittedAtUtc));
        _ = command.Parameters.AddWithValue("$causedByOperationId", remoteEvent.CausedByOperationId.HasValue ? remoteEvent.CausedByOperationId.Value.Value.ToString("D") : DBNull.Value);
        _ = command.Parameters.AddWithValue("$originClientId", (object?)remoteEvent.Origin?.ClientId ?? DBNull.Value);
        _ = command.Parameters.AddWithValue("$originOperationId", remoteEvent.Origin is null ? DBNull.Value : remoteEvent.Origin.OperationId.Value.ToString("D"));
        AddPayloadParameters(command, remoteEvent.Payload);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts event metadata rows.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="eventSequence">The event sequence.</param>
    /// <param name="remoteEvent">The event.</param>
    private static void InsertEventMetadata(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerStreamKey streamKey,
        long eventSequence,
        RemoteEvent remoteEvent)
    {
        foreach (var pair in remoteEvent.Metadata)
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO oc_server_journal_event_metadata
                    (tenant_id, stream_id, event_sequence, key, value)
                VALUES
                    ($tenantId, $streamId, $eventSequence, $key, $value);
                """;
            AddStreamParameters(command, streamKey);
            _ = command.Parameters.AddWithValue(EventSequenceParameterName, eventSequence);
            _ = command.Parameters.AddWithValue("$key", pair.Key);
            _ = command.Parameters.AddWithValue(ValueParameterName, pair.Value);
            _ = command.ExecuteNonQuery();
        }
    }
}
