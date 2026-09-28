// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Operation state storage classes and bounded proof reads.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>An oversized proof BLOB is rejected before its envelope is decrypted.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateProofIsOversized_ThenOpenFailsAuthentication()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "UPDATE oc_operation_state_proofs SET proof = zeroblob(4194305) WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);";
            _ = await command.ExecuteNonQueryAsync();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }
}
