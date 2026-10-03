// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Commits initialization and the optional encryption transition.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Commits initialization with an encryption checkpoint when protection was enabled.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <param name="encrypted">Whether plaintext records became encrypted.</param>
    private void CommitInitialization(SqliteDatabase connection, SqliteTransaction transaction, bool encrypted)
    {
        if (encrypted)
        {
            CommitAtCheckpoints(
                transaction,
                SqliteCommitCheckpoint.EncryptionTransitionBeforeCommit,
                SqliteCommitCheckpoint.EncryptionTransitionAfterCommit);
        }
        else
        {
            CommitWithOperationStateIntegrity(transaction);
        }

        if (encrypted)
        {
            SqliteRecordProtectionMaintenance.TruncateWriteAheadLog(connection);
        }
    }
}
