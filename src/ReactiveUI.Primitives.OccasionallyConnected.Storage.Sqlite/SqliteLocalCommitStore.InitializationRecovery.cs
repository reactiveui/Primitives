// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Startup recovery for a local SQLite writer after an abrupt process exit.</summary>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Opens and validates startup state while the file system releases a killed writer's WAL handle.</summary>
    /// <param name="openConnection">Creates a fresh store connection.</param>
    /// <param name="validate">Checks ownership before durability settings can change.</param>
    /// <param name="retryDelay">Waits briefly before the next attempt.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The validated connection.</returns>
    internal static SqliteConnection RetryValidatedInitializationConnection(
        Func<SqliteConnection> openConnection,
        Action<SqliteConnection> validate,
        Action retryDelay,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            SqliteConnection? connection = null;
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                connection = openConnection();
                validate(connection);
                return connection;
            }
            catch (SqliteException exception) when (exception.SqliteExtendedErrorCode == SqliteIoErrorTruncate && attempt < RecoveryOpenRetries)
            {
                connection?.Dispose();
                attempt++;
                retryDelay();
            }
            catch
            {
                connection?.Dispose();
                throw;
            }
        }
    }
}
