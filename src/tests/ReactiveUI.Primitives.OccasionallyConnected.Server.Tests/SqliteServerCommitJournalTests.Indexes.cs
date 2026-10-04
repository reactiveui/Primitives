// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
/// <content>Verifies indexed selections and compatible counter initialization.</content>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The SQLite explain query-plan detail column.</summary>
    private const int QueryPlanDetailColumn = 3;

    /// <summary>Verifies operation children, receive groups and expiry cleanup use their dedicated indexes.</summary>
    /// <param name="sql">The query plan to inspect.</param>
    /// <param name="indexName">The expected index.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(
        "SELECT * FROM oc_server_journal_events WHERE tenant_id = 'tenant' AND stream_id = 'stream' AND client_id = 'client' AND operation_id = 'operation'",
        "oc_server_events_operation")]
    [Arguments(
        "SELECT client_id FROM oc_server_journal_ledger WHERE tenant_id = 'tenant' AND stream_id = 'stream' AND group_sequence > 10 ORDER BY group_sequence LIMIT 2",
        "oc_server_ledger_group")]
    [Arguments(
        "DELETE FROM oc_server_journal_ledger WHERE expires_at_utc < '2026'",
        "oc_server_ledger_expiry")]
    public async Task JournalQueriesUseTargetedIndexes(string sql, string indexName)
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        using var connection = OpenRawConnection(database.Path);
        using var command = connection.CreateStatement();
        command.SetSql($"EXPLAIN QUERY PLAN {sql};");
        using var reader = command.Query();
        var plans = new List<string>();
        while (reader.Read())
        {
            plans.Add(reader.GetString(QueryPlanDetailColumn));
        }

        await Assert.That(string.Join(" ", plans)).Contains(indexName);
    }

    /// <summary>Verifies journals without counters seed them once while preserving operation proofs.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ExistingJournalSeedsCountersWithoutChangingReplayProofs()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        long bytes;
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            bytes = journal.LogicalBytes;
        }

        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'oc_server_metric_%';");
            var triggers = new List<string>();
            using (var reader = command.Query())
            {
                while (reader.Read())
                {
                    triggers.Add(reader.GetString(0));
                }
            }

            foreach (var trigger in triggers)
            {
                command.SetSql($"DROP TRIGGER {trigger};");
                _ = command.Execute();
            }

            command.SetSql("DELETE FROM oc_server_journal_metadata WHERE key LIKE 'metric_%';");
            _ = command.Execute();
        }

        using var reopened = CreateJournal(database.Path);
        await Assert.That(reopened.LogicalBytes).IsEqualTo(bytes);
        await Assert.That(reopened.EventCount).IsEqualTo(1);
        await Assert.That(reopened.Read(StreamKey(), [key]).Entries[0].Events[0].ServerCursor).IsEqualTo(FirstCursor);
    }
}
