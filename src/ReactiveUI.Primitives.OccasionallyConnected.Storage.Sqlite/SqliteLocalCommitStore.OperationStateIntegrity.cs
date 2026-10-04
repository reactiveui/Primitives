// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Validates or initializes protected operation state proofs.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Verifies existing proofs or creates them for a new protected store.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="userVersion">The version before initialization.</param>
    /// <param name="migrated">Whether plaintext rows became encrypted in this transaction.</param>
    private static void EnsureOperationStateIntegrity(SqliteDatabase connection, SqliteTransaction transaction, long userVersion, bool migrated)
    {
        if (userVersion == SqliteStoreSchema.LocalCommitSchemaVersion && !migrated)
        {
            SqliteOperationStateIntegrity.Verify(connection, transaction);
            return;
        }

        SqliteOperationStateIntegrity.Write(connection, transaction);
        SqliteOperationStateIntegrity.Verify(connection, transaction);
    }
}
