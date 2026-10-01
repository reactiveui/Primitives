// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Tracks record encryption, the plaintext configuration transition, and key rotation.</summary>
/// <remarks>
/// A database is either fully plaintext or fully protected. The protection marker and the encrypted key check value live in
/// <c>oc_metadata</c>. The plaintext-to-encrypted transition and key rotation each run inside one write transaction, so a
/// crash leaves the database in its previous consistent state and the next initialization runs the step again.
/// </remarks>
internal static partial class SqliteRecordProtectionMaintenance
{
    /// <summary>The metadata key that marks a protected database.</summary>
    internal const string ProtectionMetadataKey = "rxui.localstore.record_protection";

    /// <summary>The metadata key that holds the encrypted key check value.</summary>
    internal const string KeyCheckMetadataKey = "rxui.localstore.record_protection_check";

    /// <summary>The record protection format recorded in the marker.</summary>
    internal const string ProtectionFormat = "aes-256-gcm/v1";

    /// <summary>The plaintext of the key check value.</summary>
    private const string KeyCheckPlaintext = "ReactiveUI.OccasionallyConnected.Sqlite.KeyCheck.v1";

    /// <summary>The key check value column name used in its associated data.</summary>
    private const string KeyCheckColumn = "value";

    /// <summary>The store identity used for database-wide values.</summary>
    private const string DatabaseScope = "";

    /// <summary>Reports whether the database already carries the encrypted record marker.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether the database is protected.</returns>
    internal static bool IsProtected(SqliteDatabase connection, SqliteTransaction transaction) =>
        TrySelectMetadata(connection, transaction, ProtectionMetadataKey) is not null;

    /// <summary>Validates or establishes the record protection state of a database inside the initialization transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The initialization transaction.</param>
    /// <param name="protection">The configured record protection, or null for a plaintext store.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when plaintext rows were encrypted by this call.</returns>
    /// <exception cref="InvalidOperationException">The database protection state does not match the configuration or the keys.</exception>
    /// <exception cref="NotSupportedException">The database uses an unknown protection format.</exception>
    internal static bool EnsureProtectionState(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        SqliteRecordProtection? protection,
        CancellationToken cancellationToken)
    {
        var marker = TrySelectMetadata(connection, transaction, ProtectionMetadataKey);
        if (marker is null)
        {
            var keyCheck = TrySelectMetadata(connection, transaction, KeyCheckMetadataKey);
            var manifest = TrySelectMetadata(connection, transaction, SqliteOperationStateIntegrity.ManifestKey);
            if (keyCheck is not null || manifest is not null)
            {
                throw new InvalidOperationException("The SQLite local store record protection metadata is inconsistent.");
            }
        }

        if (protection is null)
        {
            if (marker is null)
            {
                return false;
            }

            throw new InvalidOperationException(
                "The SQLite local store encrypts records at rest. Configure the key provider that protects it before opening it.");
        }

        if (marker is null)
        {
            EnableSecureDelete(connection, transaction);
            _ = RewriteProtectedValues(connection, transaction, protection, RewriteMode.EncryptPlaintext, cancellationToken);
            UpsertMetadata(connection, transaction, ProtectionMetadataKey, ProtectionFormat);
            UpsertMetadata(connection, transaction, KeyCheckMetadataKey, CreateKeyCheck(protection));
            return true;
        }

        if (!string.Equals(marker, ProtectionFormat, StringComparison.Ordinal))
        {
            throw new NotSupportedException("The SQLite local store uses an unsupported record protection format.");
        }

        VerifyKeyCheck(connection, transaction, protection);
        return false;
    }

    /// <summary>Re-encrypts every protected value that is not under the current key.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <param name="protection">The record protection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of values re-encrypted.</returns>
    /// <exception cref="InvalidOperationException">The database is not protected or the key check fails.</exception>
    internal static long RotateKeys(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        SqliteRecordProtection protection,
        CancellationToken cancellationToken)
    {
        var marker = TrySelectMetadata(connection, transaction, ProtectionMetadataKey);
        if (!string.Equals(marker, ProtectionFormat, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The SQLite local store does not encrypt records at rest.");
        }

        VerifyKeyCheck(connection, transaction, protection);
        EnableSecureDelete(connection, transaction);
        return RewriteProtectedValues(connection, transaction, protection, RewriteMode.ReencryptOtherKeys, cancellationToken);
    }

    /// <summary>Truncates the write-ahead log so superseded page images leave the WAL file.</summary>
    /// <param name="connection">The connection, outside any transaction.</param>
    internal static void TruncateWriteAheadLog(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        command.SetSql("PRAGMA wal_checkpoint(TRUNCATE);");
        _ = command.Execute();
    }

    /// <summary>Makes SQLite overwrite freed content so replaced plaintext does not stay in free space.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    private static void EnableSecureDelete(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("PRAGMA secure_delete = ON;");
        _ = command.Execute();
    }

    /// <summary>Creates the encrypted key check value under the current key.</summary>
    /// <param name="protection">The record protection.</param>
    /// <returns>The protected key check text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateKeyCheck(SqliteRecordProtection protection) =>
        new SqliteRecordCipher(protection, DatabaseScope).ProtectText(KeyCheckPlaintext, SqliteRecordContext.KeyCheck(), KeyCheckColumn);

    /// <summary>Verifies that the configured keys open the database and moves the key check to the current key.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="protection">The record protection.</param>
    /// <exception cref="InvalidOperationException">The key check is missing or the configured keys do not open it.</exception>
    private static void VerifyKeyCheck(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordProtection protection)
    {
        var stored = TrySelectMetadata(connection, transaction, KeyCheckMetadataKey)
            ?? throw new InvalidOperationException("The SQLite local store record protection key check is missing.");
        var cipher = new SqliteRecordCipher(protection, DatabaseScope);
        string plaintext;
        try
        {
            plaintext = cipher.UnprotectText(stored, SqliteRecordContext.KeyCheck(), KeyCheckColumn);
        }
        catch (LocalStoreRecordAuthenticationException exception)
        {
            throw new InvalidOperationException(
                "The configured key provider cannot open the SQLite local store. Supply the key that protected it.",
                exception);
        }

        if (!string.Equals(plaintext, KeyCheckPlaintext, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The SQLite local store record protection key check is invalid.");
        }

        if (!string.Equals(SqliteRecordCipher.ReadTextKeyId(stored), protection.GetCurrentKeyId(), StringComparison.Ordinal))
        {
            UpsertMetadata(connection, transaction, KeyCheckMetadataKey, CreateKeyCheck(protection));
        }
    }

    /// <summary>Selects one metadata value when present.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The value, or null when absent.</returns>
    private static string? TrySelectMetadata(SqliteDatabase connection, SqliteTransaction transaction, string key)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT value FROM oc_metadata WHERE key = $key;");
        _ = command.Bind("$key", key);
        return command.Scalar() as string;
    }

    /// <summary>Inserts or replaces one metadata value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    private static void UpsertMetadata(SqliteDatabase connection, SqliteTransaction transaction, string key, string value)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_metadata (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value;
            """);
        _ = command.Bind("$key", key);
        _ = command.Bind("$value", value);
        _ = command.Execute();
    }
}
