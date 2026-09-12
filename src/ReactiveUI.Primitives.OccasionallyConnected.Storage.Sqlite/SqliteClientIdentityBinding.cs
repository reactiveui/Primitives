// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Manages client identity bindings stored in SQLite metadata.</summary>
internal static class SqliteClientIdentityBinding
{
    /// <summary>The maximum client identity length in UTF-16 code units.</summary>
    private const int MaximumClientIdLength = 256;

    /// <summary>The hex characters needed for one UTF-16 code unit.</summary>
    private const int HexCharactersPerUtf16CodeUnit = 4;

    /// <summary>The metadata key prefix for per-store client identity bindings.</summary>
    private const string MetadataKeyPrefix = "rxui.localstore.client_id:";

    /// <summary>The strict UTF-8 encoding used for client identity validation.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Validates an optional client identity binding.</summary>
    /// <param name="clientId">The client identity.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <returns>The validated client identity.</returns>
    /// <exception cref="ArgumentException">The client identity is blank, malformed, or too long.</exception>
    internal static string? ValidateClientId(string? clientId, string parameterName)
    {
        if (clientId is null)
        {
            return null;
        }

        if (clientId.Length > MaximumClientIdLength)
        {
            throw new ArgumentException("ClientId must be at most 256 UTF-16 code units.", parameterName);
        }

#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId, parameterName);
#else
        ThrowIfBlankClientId(clientId, parameterName);
#endif

        try
        {
            _ = StrictUtf8.GetByteCount(clientId);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("ClientId must be well-formed Unicode.", parameterName, exception);
        }

        return clientId;
    }

    /// <summary>Validates or writes the client identity binding for one store partition.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The store identity partition.</param>
    /// <param name="clientId">The requested client identity.</param>
    /// <returns>The effective client identity binding.</returns>
    /// <exception cref="InvalidOperationException">The requested binding conflicts with existing state.</exception>
    internal static string? BindOrValidate(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        string? clientId)
    {
        var key = MetadataKeyForStoreIdentity(storeIdentity);
        var existing = SelectBinding(connection, transaction, key);
        if (existing is not null)
        {
            _ = ValidateClientId(existing, nameof(clientId));
            if (clientId is not null && string.Equals(existing, clientId, StringComparison.Ordinal))
            {
                return existing;
            }

            throw new InvalidOperationException("The SQLite local store partition is bound to another client identity.");
        }

        if (clientId is null)
        {
            return null;
        }

        if (HasMutablePartitionState(connection, transaction, storeIdentity))
        {
            throw new InvalidOperationException("An existing unbound SQLite local store partition has durable state and cannot be assigned to a client identity.");
        }

        InsertBinding(connection, transaction, key, clientId);
        return clientId;
    }

#if !NET8_0_OR_GREATER
    /// <summary>Throws when a client identity is blank on target frameworks without built-in argument validation.</summary>
    /// <param name="clientId">The client identity.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The client identity is blank.</exception>
    private static void ThrowIfBlankClientId(string clientId, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(clientId))
        {
            return;
        }

        throw new ArgumentException("ClientId must not be blank.", parameterName);
    }
#endif

    /// <summary>Creates a collision-free metadata key within the versioned local store schema.</summary>
    /// <param name="storeIdentity">The store identity partition.</param>
    /// <returns>The metadata key.</returns>
    private static string MetadataKeyForStoreIdentity(string storeIdentity)
    {
        StringBuilder builder = new(MetadataKeyPrefix.Length + (storeIdentity.Length * HexCharactersPerUtf16CodeUnit));
        _ = builder.Append(MetadataKeyPrefix);
        for (var index = 0; index < storeIdentity.Length; index++)
        {
            _ = builder.Append(((int)storeIdentity[index]).ToString("X4", CultureInfo.InvariantCulture));
        }

        return builder.ToString();
    }

    /// <summary>Selects an existing client identity binding.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <returns>The existing binding, if one exists.</returns>
    /// <exception cref="InvalidOperationException">The stored binding has an invalid SQLite value type.</exception>
    private static string? SelectBinding(SqliteConnection connection, SqliteTransaction transaction, string key)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        return command.ExecuteScalar() switch
        {
            null => null,
            string value => value,
            _ => throw new InvalidOperationException("The SQLite client identity binding is not text."),
        };
    }

    /// <summary>Inserts a client identity binding.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="clientId">The client identity.</param>
    private static void InsertBinding(SqliteConnection connection, SqliteTransaction transaction, string key, string clientId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ($key, $value);";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue("$value", clientId);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Determines whether a partition contains durable state beyond empty subscription mappings.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The store identity partition.</param>
    /// <returns>Whether protected state exists.</returns>
    private static bool HasMutablePartitionState(SqliteConnection connection, SqliteTransaction transaction, string storeIdentity)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT
                (SELECT COUNT(*) FROM oc_snapshots WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_outbox WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_outbox_metadata WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_inbox WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_outbox_leases WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_outbox_operation_states WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_snapshot_authoritative_states WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_outbox_authoritative_mutations WHERE store_identity = $storeIdentity) +
                (SELECT COUNT(*) FROM oc_streams
                    WHERE store_identity = $storeIdentity
                    AND (next_client_sequence <> 1 OR server_cursor IS NOT NULL));
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 0;
    }
}
