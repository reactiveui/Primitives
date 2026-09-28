// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Records and verifies a hash of the schema definitions of a local store.</summary>
/// <remarks>
/// The checksum covers every table, index, trigger and view in <c>sqlite_master</c> except SQLite's own objects. The
/// store records it when it creates the schema, and verifies it before it changes an existing file. A mismatch
/// means something outside the store changed the schema, so opening fails closed. The hash is not keyed: it detects
/// drift and corruption, not a deliberate edit that also rewrites the recorded value.
/// </remarks>
internal static class SqliteSchemaChecksum
{
    /// <summary>The metadata key that holds the schema checksum.</summary>
    internal const string MetadataKey = "schema_checksum";

    /// <summary>The prefix that names the checksum algorithm.</summary>
    internal const string AlgorithmPrefix = "sha256:";

    /// <summary>The column index of the object type.</summary>
    private const int TypeColumn = 0;

    /// <summary>The column index of the object name.</summary>
    private const int NameColumn = 1;

    /// <summary>The column index of the owning table name.</summary>
    private const int TableNameColumn = 2;

    /// <summary>The column index of the object SQL.</summary>
    private const int SqlColumn = 3;

    /// <summary>The hexadecimal digits used to format the hash.</summary>
    private const string HexDigits = "0123456789abcdef";

    /// <summary>The number of bits in one hexadecimal digit.</summary>
    private const int BitsPerHexDigit = 4;

    /// <summary>The mask that selects one hexadecimal digit.</summary>
    private const int HexDigitMask = 0xF;

    /// <summary>The number of hexadecimal digits required for one byte.</summary>
    private const int HexDigitsPerByte = 2;

    /// <summary>Computes the checksum of the current schema definitions.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <returns>The checksum text, prefixed with the algorithm name.</returns>
    internal static string Compute(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT type, name, tbl_name, sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
            ORDER BY type, name;
            """;
        StringBuilder builder = new();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var sql = reader.GetString(SqlColumn);
                _ = builder.Append(reader.GetString(TypeColumn)).Append('\t')
                    .Append(reader.GetString(NameColumn)).Append('\t')
                    .Append(reader.GetString(TableNameColumn)).Append('\t')
                    .Append(sql).Append('\n');
            }
        }

        return AlgorithmPrefix + ToHex(Hash(Encoding.UTF8.GetBytes(builder.ToString())));
    }

    /// <summary>Verifies the required recorded checksum.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <exception cref="InvalidOperationException">The schema does not match its recorded checksum.</exception>
    internal static void Verify(SqliteConnection connection, SqliteTransaction transaction)
    {
        var recorded = TrySelect(connection, transaction);
        if (recorded is not null && FixedTimeEquals(recorded, Compute(connection, transaction)))
        {
            return;
        }

        throw new InvalidOperationException(
            "The SQLite store schema does not match its recorded checksum. Something outside the store changed the schema, so the store will not open it.");
    }

    /// <summary>Records the checksum of the current schema when it differs from the recorded value.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The write transaction.</param>
    /// <returns><see langword="true"/> when a new checksum was written.</returns>
    internal static bool Record(SqliteConnection connection, SqliteTransaction transaction)
    {
        var computed = Compute(connection, transaction);
        var recorded = TrySelect(connection, transaction);
        if (recorded is not null && string.Equals(recorded, computed, StringComparison.Ordinal))
        {
            return false;
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_metadata (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value;
            """;
        _ = command.Parameters.AddWithValue("$key", MetadataKey);
        _ = command.Parameters.AddWithValue("$value", computed);
        _ = command.ExecuteNonQuery();
        return true;
    }

    /// <summary>Reads the recorded checksum.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <returns>The recorded checksum, or null when none is recorded.</returns>
    internal static string? TrySelect(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", MetadataKey);
        return command.ExecuteScalar() as string;
    }

    /// <summary>Hashes the normalized schema text.</summary>
    /// <param name="bytes">The UTF-8 schema text.</param>
    /// <returns>The SHA-256 hash.</returns>
    private static byte[] Hash(byte[] bytes)
    {
#if NET8_0_OR_GREATER
        return SHA256.HashData(bytes);
#else
        using var sha256 = SHA256.Create();
        return sha256.ComputeHash(bytes);
#endif
    }

    /// <summary>Formats bytes as lowercase hexadecimal text.</summary>
    /// <param name="bytes">The bytes.</param>
    /// <returns>The hexadecimal text.</returns>
    private static string ToHex(byte[] bytes)
    {
        var characters = new char[bytes.Length * HexDigitsPerByte];
        for (var index = 0; index < bytes.Length; index++)
        {
            characters[index * HexDigitsPerByte] = HexDigits[bytes[index] >> BitsPerHexDigit];
            characters[(index * HexDigitsPerByte) + 1] = HexDigits[bytes[index] & HexDigitMask];
        }

        return new(characters);
    }

    /// <summary>Compares two checksum texts without a content-dependent early return.</summary>
    /// <param name="left">The first text.</param>
    /// <param name="right">The second text.</param>
    /// <returns>Whether the texts are equal.</returns>
    private static bool FixedTimeEquals(string left, string right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var result = 0;
        for (var index = 0; index < left.Length; index++)
        {
            result |= left[index] ^ right[index];
        }

        return result == 0;
    }
}
