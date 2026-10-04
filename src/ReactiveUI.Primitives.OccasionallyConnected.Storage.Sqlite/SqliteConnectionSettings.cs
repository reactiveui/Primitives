// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Configures SQLite connection settings used by the identity store.</summary>
internal static class SqliteConnectionSettings
{
    /// <summary>The SQLite integer value for FULL synchronous writes.</summary>
    private const long SqliteFullSynchronous = 2;

    /// <summary>Applies the connection busy timeout.</summary>
    /// <param name="connection">The open connection.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ConfigureBusyTimeout(SqliteDatabase connection) =>
        connection.SetBusyTimeout(SqliteDatabase.DefaultBusyTimeoutMilliseconds);

    /// <summary>Applies per-connection settings required before operational transactions.</summary>
    /// <param name="connection">The open connection.</param>
    /// <exception cref="InvalidOperationException">SQLite did not accept the required operational settings.</exception>
    internal static void ConfigureOperationalConnection(SqliteDatabase connection)
    {
        using (var verifyForeignKeysCommand = connection.CreateStatement())
        {
            verifyForeignKeysCommand.SetSql("SELECT foreign_keys FROM pragma_foreign_keys;");
            var foreignKeys = verifyForeignKeysCommand.Scalar();
            if (foreignKeys is not long enabled || enabled != 1)
            {
                connection.Execute("PRAGMA foreign_keys = ON;");
                foreignKeys = verifyForeignKeysCommand.Scalar();
            }

            VerifyForeignKeys(foreignKeys);
        }

        using var verifySynchronousCommand = connection.CreateStatement();
        verifySynchronousCommand.SetSql("SELECT synchronous FROM pragma_synchronous;");
        var synchronous = verifySynchronousCommand.Scalar();
        if (synchronous is not long mode || mode != SqliteFullSynchronous)
        {
            connection.Execute("PRAGMA synchronous = FULL;");
            synchronous = verifySynchronousCommand.Scalar();
        }

        VerifyFullSynchronous(synchronous);
    }

    /// <summary>Applies durability pragmas after schema validation.</summary>
    /// <param name="connection">The open connection.</param>
    /// <exception cref="InvalidOperationException">SQLite did not accept the required durability settings.</exception>
    internal static void ConfigureDurability(SqliteDatabase connection)
    {
        ConfigureOperationalConnection(connection);

        using var command = connection.CreateStatement();
        command.SetSql("PRAGMA journal_mode = WAL;");
        VerifyWalJournalMode(command.Scalar());
    }

    /// <summary>Verifies SQLite enabled foreign key enforcement for the current connection.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">Foreign key enforcement was not accepted.</exception>
    internal static void VerifyForeignKeys(object? value)
    {
        if (value is long enabled && enabled == 1)
        {
            return;
        }

        throw new InvalidOperationException("SQLite did not enable foreign key enforcement.");
    }

    /// <summary>Verifies SQLite accepted WAL journaling.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">WAL journaling was not accepted.</exception>
    internal static void VerifyWalJournalMode(object? value)
    {
        if (value is string journalMode && string.Equals(journalMode, "wal", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new InvalidOperationException("SQLite did not enable WAL journaling for the identity store.");
    }

    /// <summary>Verifies SQLite accepted FULL synchronous writes.</summary>
    /// <param name="value">The returned PRAGMA value.</param>
    /// <exception cref="InvalidOperationException">FULL synchronous writes were not accepted.</exception>
    internal static void VerifyFullSynchronous(object? value)
    {
        if (value is long synchronous && synchronous == SqliteFullSynchronous)
        {
            return;
        }

        throw new InvalidOperationException("SQLite did not enable FULL synchronous writes for the identity store.");
    }
}
