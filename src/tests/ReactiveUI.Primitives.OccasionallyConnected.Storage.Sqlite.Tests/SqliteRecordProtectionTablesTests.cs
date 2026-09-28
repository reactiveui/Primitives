// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteRecordProtectionTables"/>.</summary>
public sealed class SqliteRecordProtectionTablesTests
{
    /// <summary>The schema version query parameter.</summary>
    private const string PayloadSchemaParameter = "$payloadSchema";

    /// <summary>The stream identity query parameter.</summary>
    private const string StreamIdParameter = "$streamId";

    /// <summary>A valid non-empty test identifier.</summary>
    private const string ValidId = "99999999-9999-9999-9999-999999999999";

    /// <summary>Verifies malformed row identity and context fields cannot be used to rewrite protected values.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task MalformedRowsAreRejectedBeforeProtectedValuesAreRewritten()
    {
        var cases = new (SqliteProtectedTableKind Kind, string Parameter, object Value)[]
        {
            (SqliteProtectedTableKind.Outbox, "$operationId", DBNull.Value),
            (SqliteProtectedTableKind.Outbox, "$payloadContract", DBNull.Value),
            (SqliteProtectedTableKind.OutboxAuthoritativeMutations, PayloadSchemaParameter, "wrong"),
            (SqliteProtectedTableKind.OutboxMetadata, "$key", DBNull.Value),
            (SqliteProtectedTableKind.Snapshots, "$revision", "wrong"),
            (SqliteProtectedTableKind.Snapshots, "$payloadContentType", DBNull.Value),
            (SqliteProtectedTableKind.SnapshotAuthoritativeStates, PayloadSchemaParameter, (long)int.MaxValue + 1),
            (SqliteProtectedTableKind.Streams, StreamIdParameter, DBNull.Value),
            (SqliteProtectedTableKind.Streams, StreamIdParameter, string.Empty),
            (SqliteProtectedTableKind.Inbox, "$eventId", "not-a-guid"),
            (SqliteProtectedTableKind.PayloadQuarantine, "$quarantineId", Guid.Empty.ToString()),
            (SqliteProtectedTableKind.DeadLetters, "$attemptCount", (long)int.MaxValue + 1),
        };

        var connectionString = new SqliteConnectionStringBuilder { DataSource = ":memory:" }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();

        foreach (var testCase in cases)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                SELECT $operationId AS operation_id, $streamId AS stream_id,
                       $clientSequence AS client_sequence, $operationType AS operation_type,
                       $payloadContract AS payload_contract_id, $payloadSchema AS payload_schema_version,
                       $payloadContentType AS payload_content_type, $key AS key,
                       $formatVersion AS format_version, $revision AS revision,
                       $eventId AS event_id, $quarantineId AS quarantine_id,
                       $attemptCount AS attempt_count, $changedAt AS changed_at_utc;
                """;
            _ = command.Parameters.AddWithValue("$operationId", ValidId);
            _ = command.Parameters.AddWithValue(StreamIdParameter, "stream/a");
            _ = command.Parameters.AddWithValue("$clientSequence", 1L);
            _ = command.Parameters.AddWithValue("$operationType", 1L);
            _ = command.Parameters.AddWithValue("$payloadContract", "contract");
            _ = command.Parameters.AddWithValue(PayloadSchemaParameter, 1L);
            _ = command.Parameters.AddWithValue("$payloadContentType", "application/json");
            _ = command.Parameters.AddWithValue("$key", "metadata-key");
            _ = command.Parameters.AddWithValue("$formatVersion", 1L);
            _ = command.Parameters.AddWithValue("$revision", 1L);
            _ = command.Parameters.AddWithValue("$eventId", ValidId);
            _ = command.Parameters.AddWithValue("$quarantineId", ValidId);
            _ = command.Parameters.AddWithValue("$attemptCount", 1L);
            _ = command.Parameters.AddWithValue("$changedAt", "2026-01-01T00:00:00Z");
            command.Parameters[testCase.Parameter].Value = testCase.Value;

            await using var reader = await command.ExecuteReaderAsync();
            _ = await reader.ReadAsync();
            var table = SqliteRecordProtectionTables.All.Single(candidate => candidate.Kind == testCase.Kind);
            var values = new List<SqliteProtectedValue>();

            await Assert.That(table.Describe(reader, values)).IsFalse();
            await Assert.That(values.Count).IsEqualTo(0);
        }
    }
}
