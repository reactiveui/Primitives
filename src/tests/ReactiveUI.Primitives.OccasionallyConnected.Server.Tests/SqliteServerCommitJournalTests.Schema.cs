// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests the first durable schema for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The unsupported user version used by the fail-closed test.</summary>
    private const long UnsupportedExistingSchemaVersion = 2;

    /// <summary>Verifies a fresh journal creates the complete V1 schema.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FreshJournalCreatesCompleteSchemaOne()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            await Assert.That(journal.StreamCount).IsEqualTo(0);
        }

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(1);
        await Assert.That(ReadSchemaMetadataVersion(database.Path)).IsEqualTo("1");
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "PRAGMA table_info(oc_server_journal_streams);";
            await Assert.That(await ContainsColumnAsync(command, "last_group_sequence")).IsTrue();
            command.CommandText = "PRAGMA table_info(oc_server_journal_subscriptions);";
            await Assert.That(await ContainsColumnAsync(command, "generation")).IsTrue();
            command.CommandText = "PRAGMA table_info(oc_server_journal_subscription_offers);";
            await Assert.That(await ContainsColumnAsync(command, "snapshot_format_version")).IsTrue();
        }

        using var reopened = CreateJournal(database.Path);
        await Assert.That(reopened.StreamCount).IsEqualTo(0);
    }

    /// <summary>Verifies an unsupported existing version cannot be upgraded or reset on open.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExistingUnsupportedVersionFailsWithoutMutation()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            var key = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "PRAGMA user_version = 2;";
            _ = await command.ExecuteNonQueryAsync();
        }

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(UnsupportedExistingSchemaVersion);
        await Assert.That(ReadSchemaMetadataVersion(database.Path)).IsEqualTo("1");
        await Assert.That(CountLedgerRows(database.Path)).IsEqualTo(1);
    }

    /// <summary>Reads the durable schema metadata version.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The stored version.</returns>
    private static string? ReadSchemaMetadataVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM oc_server_journal_metadata WHERE key = 'schema_version';";
        return command.ExecuteScalar() as string;
    }

    /// <summary>Checks whether the selected owned table contains one required column.</summary>
    /// <param name="command">The table-info query.</param>
    /// <param name="column">The required column name.</param>
    /// <returns>Whether the column exists.</returns>
    private static async Task<bool> ContainsColumnAsync(Microsoft.Data.Sqlite.SqliteCommand command, string column)
    {
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (string.Equals(reader.GetString(1), column, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Counts retained ledger rows.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The row count.</returns>
    private static long CountLedgerRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_server_journal_ledger;";
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }
}
