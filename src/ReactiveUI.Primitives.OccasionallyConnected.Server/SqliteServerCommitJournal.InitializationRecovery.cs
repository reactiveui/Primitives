// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Startup recovery for the server SQLite journal after an abrupt process exit.</summary>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Retries only transient WAL truncate failures while creating a startup connection.</summary>
    /// <param name="openConnection">Creates and configures a fresh connection.</param>
    /// <param name="retryDelay">Waits briefly before the next attempt.</param>
    /// <returns>The configured connection.</returns>
    internal static SqliteConnection RetryInitializationConnection(Func<SqliteConnection> openConnection, Action retryDelay)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                return openConnection();
            }
            catch (SqliteException exception) when (exception.SqliteExtendedErrorCode == SqliteIoErrorTruncate && attempt < RecoveryOpenRetries)
            {
                attempt++;
                retryDelay();
            }
        }
    }
}
