// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Tracks whether a SQLite database encrypts records at rest, migrates plaintext rows, and rotates keys.</summary>
/// <content>Rewrites protected column values table by table in bounded rowid batches.</content>
internal static partial class SqliteRecordProtectionMaintenance
{
    /// <summary>The number of rows read before their updates are written.</summary>
    private const int RewriteBatchSize = 256;

    /// <summary>The rowid column index in every rewrite query.</summary>
    private const int RowIdIndex = 0;

    /// <summary>The store identity column index in every rewrite query.</summary>
    private const int StoreIdentityIndex = 1;

    /// <summary>Describes how a rewrite changes protected values.</summary>
    private enum RewriteMode
    {
        /// <summary>Encrypts plaintext values of a database that is not yet protected.</summary>
        EncryptPlaintext = 0,

        /// <summary>Re-encrypts values protected by a key other than the current key.</summary>
        ReencryptOtherKeys = 1,
    }

    /// <summary>Rewrites every protected value in every protected table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="protection">The record protection.</param>
    /// <param name="mode">The rewrite mode.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of rewritten values.</returns>
    private static long RewriteProtectedValues(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SqliteRecordProtection protection,
        RewriteMode mode,
        CancellationToken cancellationToken)
    {
        var rewrite = new RewriteContext(protection, mode, protection.GetCurrentKeyId());
        long rewritten = 0;
        foreach (var table in SqliteRecordProtectionTables.All)
        {
            rewritten = checked(rewritten + RewriteTable(connection, transaction, table, rewrite, cancellationToken));
        }

        return rewritten;
    }

    /// <summary>Rewrites the protected values of one table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="table">The table descriptor.</param>
    /// <param name="rewrite">The rewrite state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of rewritten values.</returns>
    private static long RewriteTable(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SqliteProtectedTable table,
        RewriteContext rewrite,
        CancellationToken cancellationToken)
    {
        long rewritten = 0;
        var afterRowId = long.MinValue;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (rowCount, updates, lastRowId) = ReadRewriteBatch(connection, transaction, table, rewrite, afterRowId);
            afterRowId = lastRowId;
            for (var index = 0; index < updates.Count; index++)
            {
                rewritten = checked(rewritten + updates[index].RewrittenValues);
                ApplyUpdate(connection, transaction, table, updates[index]);
            }

            if (rowCount < RewriteBatchSize)
            {
                return rewritten;
            }
        }
    }

    /// <summary>Reads one rowid batch and computes its updates.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="table">The table descriptor.</param>
    /// <param name="rewrite">The rewrite state.</param>
    /// <param name="afterRowId">The last rowid of the previous batch.</param>
    /// <returns>The row count, the pending updates, and the last rowid read.</returns>
    private static (int RowCount, List<SqliteProtectedRowUpdate> Updates, long LastRowId) ReadRewriteBatch(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SqliteProtectedTable table,
        RewriteContext rewrite,
        long afterRowId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        SqliteRecordProtectionTables.SetSelectSql(command, table.Kind);
        _ = command.Parameters.AddWithValue("$afterRowId", afterRowId);
        _ = command.Parameters.AddWithValue("$batchSize", RewriteBatchSize);
        using var reader = command.ExecuteReader();
        List<SqliteProtectedRowUpdate> updates = [];
        List<SqliteProtectedValue> values = [];
        var rowCount = 0;
        var lastRowId = afterRowId;
        while (reader.Read())
        {
            rowCount++;
            lastRowId = reader.GetInt64(RowIdIndex);
            values.Clear();
            if (reader.GetValue(StoreIdentityIndex) is not string storeIdentity || !table.Describe(reader, values))
            {
                continue;
            }

            var update = RewriteRow(reader, lastRowId, rewrite.CreateCipher(storeIdentity), values, rewrite);
            if (update.RewrittenValues > 0)
            {
                updates.Add(update);
            }
        }

        return (rowCount, updates, lastRowId);
    }

    /// <summary>Computes the new stored values of one row.</summary>
    /// <param name="reader">The reader positioned on the row.</param>
    /// <param name="rowId">The rowid.</param>
    /// <param name="cipher">The cipher for the row's store identity.</param>
    /// <param name="values">The protected values of the row.</param>
    /// <param name="rewrite">The rewrite state.</param>
    /// <returns>The row update.</returns>
    private static SqliteProtectedRowUpdate RewriteRow(
        SqliteDataReader reader,
        long rowId,
        SqliteRecordCipher cipher,
        List<SqliteProtectedValue> values,
        RewriteContext rewrite)
    {
        var parameters = new KeyValuePair<string, object>[values.Count];
        var rewritten = 0;
        for (var index = 0; index < values.Count; index++)
        {
            var value = values[index];
            var stored = SqliteRecordProtectionTables.GetValue(reader, value.Column);
            var next = RewriteValue(stored, value, cipher, rewrite);
            if (!ReferenceEquals(next, stored))
            {
                rewritten++;
            }

            parameters[index] = new($"${value.Column}", next);
        }

        return new(rowId, parameters, rewritten);
    }

    /// <summary>Computes the new stored form of one value, or returns the stored value unchanged.</summary>
    /// <param name="stored">The stored value.</param>
    /// <param name="value">The protected value descriptor.</param>
    /// <param name="cipher">The cipher for the row's store identity.</param>
    /// <param name="rewrite">The rewrite state.</param>
    /// <returns>The new stored value, or <paramref name="stored"/> when it stays unchanged.</returns>
    /// <remarks>
    /// A value with an unexpected storage class stays unchanged, so a corrupt plaintext row fails authentication and is
    /// quarantined on its next read. A value that fails authentication during rotation also stays unchanged, so rotation
    /// never re-encrypts, and so launders, tampered data.
    /// </remarks>
    private static object RewriteValue(object stored, in SqliteProtectedValue value, SqliteRecordCipher cipher, RewriteContext rewrite)
    {
        if (rewrite.Mode == RewriteMode.EncryptPlaintext)
        {
            return (value.IsBlob, stored) switch
            {
                (true, byte[] bytes) => cipher.ProtectBytes(bytes, value.Context, value.Column),
                (false, string text) => cipher.ProtectText(text, value.Context, value.Column),
                _ => stored,
            };
        }

        try
        {
            return (value.IsBlob, stored) switch
            {
                (true, byte[] envelope) when !rewrite.IsCurrentKey(SqliteRecordCipher.ReadBytesKeyId(envelope)) =>
                    cipher.ProtectBytes(cipher.UnprotectBytes(envelope, value.Context, value.Column), value.Context, value.Column),
                (false, string envelope) when !rewrite.IsCurrentKey(SqliteRecordCipher.ReadTextKeyId(envelope)) =>
                    cipher.ProtectText(cipher.UnprotectText(envelope, value.Context, value.Column), value.Context, value.Column),
                _ => stored,
            };
        }
        catch (LocalStoreRecordAuthenticationException)
        {
            return stored;
        }
    }

    /// <summary>Writes one row update.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="table">The table descriptor.</param>
    /// <param name="update">The row update.</param>
    /// <exception cref="InvalidOperationException">The row no longer exists.</exception>
    private static void ApplyUpdate(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SqliteProtectedTable table,
        SqliteProtectedRowUpdate update)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        SqliteRecordProtectionTables.SetUpdateSql(command, table.Kind);
        _ = command.Parameters.AddWithValue("$rowId", update.RowId);
        foreach (var parameter in update.Parameters)
        {
            _ = command.Parameters.AddWithValue(parameter.Key, parameter.Value);
        }

        if (command.ExecuteNonQuery() != 1)
        {
            throw new InvalidOperationException("A protected SQLite row changed during record protection maintenance.");
        }
    }

    /// <summary>Holds the state shared by one rewrite pass.</summary>
    private sealed class RewriteContext
    {
        /// <summary>The record protection.</summary>
        private readonly SqliteRecordProtection _protection;

        /// <summary>The current key identifier.</summary>
        private readonly string _currentKeyId;

        /// <summary>Initializes a new instance of the <see cref="RewriteContext"/> class.</summary>
        /// <param name="protection">The record protection.</param>
        /// <param name="mode">The rewrite mode.</param>
        /// <param name="currentKeyId">The current key identifier.</param>
        internal RewriteContext(SqliteRecordProtection protection, RewriteMode mode, string currentKeyId)
        {
            _protection = protection;
            Mode = mode;
            _currentKeyId = currentKeyId;
        }

        /// <summary>Gets the rewrite mode.</summary>
        internal RewriteMode Mode { get; }

        /// <summary>Creates the cipher for a store identity.</summary>
        /// <param name="storeIdentity">The store identity.</param>
        /// <returns>The cipher.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal SqliteRecordCipher CreateCipher(string storeIdentity) => new(_protection, storeIdentity);

        /// <summary>Determines whether a key identifier is the current key.</summary>
        /// <param name="keyId">The key identifier, or null for a malformed envelope.</param>
        /// <returns>Whether the value already uses the current key.</returns>
        internal bool IsCurrentKey(string? keyId) => keyId is null || string.Equals(keyId, _currentKeyId, StringComparison.Ordinal);
    }
}
