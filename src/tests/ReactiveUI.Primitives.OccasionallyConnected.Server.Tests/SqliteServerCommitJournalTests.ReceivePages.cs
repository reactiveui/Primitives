// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests receive paging for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The legacy schema-one table definitions used to verify migration.</summary>
    private const string SchemaOneTablesSql = """
        PRAGMA user_version = 1;
        CREATE TABLE oc_server_journal_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE oc_server_journal_streams (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            state_version TEXT NULL,
            state_payload_contract_id TEXT NULL,
            state_payload_schema_version INTEGER NULL,
            state_payload_content_type TEXT NULL,
            state_payload BLOB NULL,
            state_payload_hash TEXT NULL,
            write_stamp_committed_at_utc TEXT NULL,
            write_stamp_client_id TEXT NULL,
            write_stamp_operation_id TEXT NULL,
            last_cursor TEXT NULL,
            last_event_sequence INTEGER NOT NULL,
            state_bytes INTEGER NOT NULL,
            last_cursor_bytes INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, stream_id));
        CREATE TABLE oc_server_journal_ledger (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint BLOB NOT NULL,
            result_kind INTEGER NOT NULL,
            result_reason_code TEXT NULL,
            result_server_version TEXT NULL,
            committed_at_utc TEXT NOT NULL,
            expires_at_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, stream_id, client_id, operation_id),
            FOREIGN KEY (tenant_id, stream_id)
                REFERENCES oc_server_journal_streams (tenant_id, stream_id)
                ON DELETE CASCADE);
        CREATE TABLE oc_server_journal_conflicts (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            conflict_index INTEGER NOT NULL,
            resolution_code TEXT NOT NULL,
            resolved_payload_contract_id TEXT NULL,
            resolved_payload_schema_version INTEGER NULL,
            resolved_payload_content_type TEXT NULL,
            resolved_payload BLOB NULL,
            resolved_payload_hash TEXT NULL,
            PRIMARY KEY (tenant_id, stream_id, client_id, operation_id, conflict_index),
            FOREIGN KEY (tenant_id, stream_id, client_id, operation_id)
                REFERENCES oc_server_journal_ledger (tenant_id, stream_id, client_id, operation_id)
                ON DELETE CASCADE);
        CREATE TABLE oc_server_journal_events (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            server_cursor TEXT NOT NULL,
            committed_at_utc TEXT NOT NULL,
            caused_by_operation_id TEXT NULL,
            origin_client_id TEXT NULL,
            origin_operation_id TEXT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (tenant_id, stream_id, event_sequence),
            UNIQUE (tenant_id, stream_id, event_id),
            UNIQUE (tenant_id, stream_id, server_cursor),
            FOREIGN KEY (tenant_id, stream_id, client_id, operation_id)
                REFERENCES oc_server_journal_ledger (tenant_id, stream_id, client_id, operation_id)
                ON DELETE CASCADE);
        CREATE TABLE oc_server_journal_event_metadata (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (tenant_id, stream_id, event_sequence, key),
            FOREIGN KEY (tenant_id, stream_id, event_sequence)
                REFERENCES oc_server_journal_events (tenant_id, stream_id, event_sequence)
                ON DELETE CASCADE);
        """;

    /// <summary>The legacy schema-one seed rows used to verify migration.</summary>
    private const string SchemaOneDataSql = """
        INSERT INTO oc_server_journal_metadata (key, value) VALUES ('schema_version', '1');
        INSERT INTO oc_server_journal_metadata (key, value) VALUES ('latest_utc', $latestUtc);
        INSERT INTO oc_server_journal_streams
            (tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
             state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc,
             write_stamp_client_id, write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes, last_cursor_bytes)
        VALUES
            ($tenantId, $streamId, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
        INSERT INTO oc_server_journal_ledger
            (tenant_id, stream_id, client_id, operation_id, fingerprint, result_kind, result_reason_code,
             result_server_version, committed_at_utc, expires_at_utc, logical_bytes)
        VALUES
            ($tenantId, $streamId, $clientId, $operationId, $fingerprint, $resultKind, NULL,
             $resultServerVersion, $committedAtUtc, $expiresAtUtc, 64);
        """;

    /// <summary>Verifies SQLite receive paging survives reopen and keeps zero-event groups ordered.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesRoundTripCompleteGroupsAndZeroEventCompletions()
    {
        using var database = new TemporaryDatabase();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        string cursor;
        using (var journal = CreateJournal(database.Path))
        {
            var entries = new[]
            {
                Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
                Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: []),
            };
            _ = journal.TryCommit(new(StreamKey(), 0, State(FirstVersion), Stamp(firstKey), entries));
            var firstPage = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            var firstBatch = firstPage.Batch ?? throw new InvalidOperationException("The first page did not return a batch.");
            cursor = firstBatch.NextCursor;
        }

        using var reopened = CreateJournal(database.Path);
        var secondPage = reopened.ReadReceivePage(new(StreamKey(), cursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second page did not return a batch.");
        var end = reopened.ReadReceivePage(new(StreamKey(), secondBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(secondPage.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(secondPage.NextGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(secondPage.LastGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(secondBatch.PreviousCursor).IsEqualTo(cursor);
        await Assert.That(secondBatch.NextCursor).IsNotEqualTo(string.Empty);
        await Assert.That(secondBatch.Events).Count().IsEqualTo(0);
        await Assert.That(secondBatch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(secondBatch.CompletedOperations[0].Origin).IsEqualTo(new(Client, secondKey.OperationId));
        await Assert.That(secondBatch.CompletedOperations[0].EventIds).Count().IsEqualTo(0);
        await Assert.That(end.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
    }

    /// <summary>Verifies SQLite reports a retention gap after the requested group has expired.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesReportRetentionGapAfterCompaction()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();

        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.LastGroupSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies schema-one migration preserves replay but does not fabricate receive group completeness.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesTreatSchemaOneReplayAsHistoryGapAfterMigration()
    {
        using var database = new TemporaryDatabase();
        var firstKey = OperationKey(FirstOperationSeed);
        CreateSchemaOneJournal(database.Path, firstKey);

        using var journal = CreateJournal(database.Path);
        var replay = journal.Read(StreamKey(), [firstKey]);
        var stale = journal.TryCommit(Plan(replay.Revision, null, null, Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [])));
        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(stale.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.Batch).IsNull();
    }

    /// <summary>Verifies schema-one migration rejects unsupported metadata before mutating tables.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectSchemaOneMigrationWithUnsupportedMetadata()
    {
        using var database = new TemporaryDatabase();
        CreateSchemaOneJournal(database.Path, OperationKey(FirstOperationSeed));
        WriteSchemaOneMetadataVersion(database.Path, "9");

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies corrupt durable receive-history markers fail closed during receive paging.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectCorruptReceiveHistoryMarker()
    {
        using var database = new TemporaryDatabase();
        var firstKey = OperationKey(FirstOperationSeed);
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        WriteReceiveHistoryMarker(database.Path, DoubleEntryCount);
        using var reopened = CreateJournal(database.Path);

        await Assert.That(() => reopened.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies receive page dispatch through the durable journal interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesUseCommitJournalInterface()
    {
        using var database = new TemporaryDatabase();
        IServerCommitJournal journal = CreateJournal(database.Path);
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));

        var snapshot = journal.Read(StreamKey(), [firstKey]);
        var page = ((IServerReceiveJournal)journal).ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
    }

    /// <summary>Verifies changing retention cannot make a page silently skip an expired middle group.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected complete prefix page is absent.</exception>
    [Test]
    public async Task ReceivePagesStopBeforeAnExpiredMiddleGroup()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateJournal(database.Path, clock);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var third = OperationKey(Client, ThirdOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        using (var shorterRetention = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount)))
        {
            _ = shorterRetention.TryCommit(Plan(SingleEntryCount, null, null, Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        }

        _ = journal.TryCommit(Plan(DoubleEntryCount, null, null, Entry(third, OperationResultKind.Accepted, ThirdOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        await Assert.That(journal.Compact()).IsEqualTo(SingleEntryCount);

        var page = journal.ReadReceivePage(new(StreamKey(), null, DefaultMaximumLedgerEntries, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The retained complete prefix was not returned.");

        await Assert.That(batch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(batch.NextCursor).IsEqualTo(FirstCursor);
        var gap = journal.ReadReceivePage(new(StreamKey(), batch.NextCursor, DefaultMaximumLedgerEntries, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        await Assert.That(gap.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
    }

    /// <summary>Creates a schema-one database with a dedup receipt whose receive order cannot be reconstructed.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationKey">The operation key.</param>
    private static void CreateSchemaOneJournal(string path, ServerOperationKey operationKey)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = SchemaOneTablesSql + SchemaOneDataSql;
        AddSchemaOneParameters(command, operationKey);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Adds schema-one seed parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="operationKey">The operation key.</param>
    private static void AddSchemaOneParameters(Microsoft.Data.Sqlite.SqliteCommand command, ServerOperationKey operationKey)
    {
        _ = command.Parameters.AddWithValue("$tenantId", Tenant);
        _ = command.Parameters.AddWithValue("$streamId", Stream.Value);
        _ = command.Parameters.AddWithValue("$clientId", operationKey.ClientId);
        _ = command.Parameters.AddWithValue("$operationId", operationKey.OperationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue("$fingerprint", Fingerprint(FirstOperationSeed).ToArray());
        _ = command.Parameters.AddWithValue("$resultKind", (int)OperationResultKind.Accepted);
        _ = command.Parameters.AddWithValue("$resultServerVersion", FirstVersion);
        _ = command.Parameters.AddWithValue("$committedAtUtc", Start.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
        _ = command.Parameters.AddWithValue("$expiresAtUtc", Start.AddMinutes(DefaultRetentionMinutes).ToString("O", System.Globalization.CultureInfo.InvariantCulture));
        _ = command.Parameters.AddWithValue("$latestUtc", Start.ToString("O", System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Writes a legacy metadata schema version.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="version">The schema version text.</param>
    private static void WriteSchemaOneMetadataVersion(string path, string version)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_metadata SET value = $value WHERE key = 'schema_version';";
        _ = command.Parameters.AddWithValue("$value", version);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Writes a raw receive-history marker value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="value">The marker value.</param>
    private static void WriteReceiveHistoryMarker(string path, int value)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_streams SET receive_history_incomplete = $value;";
        _ = command.Parameters.AddWithValue("$value", value);
        _ = command.ExecuteNonQuery();
    }
}
