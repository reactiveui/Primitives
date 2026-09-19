// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests fail-closed subscription schema migration.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Verifies migration does not silently erase an unsupported subscription schema.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionMigrationRejectsUnexpectedColumnBeforeRebuildingTables()
    {
        const int schemaVersionThree = 3;
        using var database = new TemporaryDatabase();
        using (var created = CreateSubscriptionJournal(database.Path))
        {
            await Assert.That(created.SubscriptionCount).IsEqualTo(0);
        }

        RewriteSubscriptionTablesAsSchemaThree(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "ALTER TABLE oc_server_journal_subscriptions ADD COLUMN unsupported_state TEXT NULL;";
            _ = await command.ExecuteNonQueryAsync();
        }

        await Assert.That(() => CreateSubscriptionJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(schemaVersionThree);
    }
}
