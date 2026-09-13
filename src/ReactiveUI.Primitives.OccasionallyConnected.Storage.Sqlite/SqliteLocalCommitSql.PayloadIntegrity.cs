// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Executes bounded payload integrity reads.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>The invalid SQLite payload hash message.</summary>
    private const string InvalidPayloadHashMessage = "The SQLite payload hash is invalid.";

    /// <summary>Reads the stored payload length from bounded SQL evidence.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The evidence columns.</param>
    /// <returns>The stored payload length.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadPayloadLength(SqliteDataReader reader, SqlitePayloadEvidenceColumns columns) =>
        ReadProjectedNonNegativeLength(reader, columns.PayloadLengthIndex, "The SQLite payload bytes are invalid.");

    /// <summary>Reads the total stored payload metadata length from bounded SQL evidence.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The evidence columns.</param>
    /// <returns>The stored metadata length.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static long ReadPayloadMetadataLength(SqliteDataReader reader, SqlitePayloadEvidenceColumns columns)
    {
        var length = ReadProjectedNonNegativeLength(reader, columns.ContractLengthIndex, "The SQLite payload contract is invalid.");
        length = checked(length + ReadProjectedNonNegativeLength(reader, columns.ContentTypeLengthIndex, "The SQLite payload content type is invalid."));
        return checked(length + ReadProjectedNonNegativeLength(reader, columns.HashLengthIndex, InvalidPayloadHashMessage));
    }

    /// <summary>Reads a projected non-negative length column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The projected length column index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The length.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadProjectedNonNegativeLength(SqliteDataReader reader, int index, string message) =>
        ReadNonNegativeLong(reader, index, message);

    /// <summary>Reads and validates the canonical stored payload hash when bounded SQL evidence proves canonical hash intent.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The evidence columns.</param>
    /// <returns>The canonical payload hash, or null when the stored hash uses the legacy opaque hash contract.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static string? ReadCanonicalPayloadHashPreflight(SqliteDataReader reader, SqlitePayloadEvidenceColumns columns)
    {
        var hashLength = ReadProjectedNonNegativeLength(reader, columns.HashLengthIndex, InvalidPayloadHashMessage);
        if (hashLength == 0)
        {
            throw new InvalidOperationException(InvalidPayloadHashMessage);
        }

        var payloadHash = TryReadTextEvidence(reader, columns.HashStorageTypeIndex, columns.HashBytesIndex)
            ?? throw new InvalidOperationException(InvalidPayloadHashMessage);
        if (!payloadHash.StartsWith(Sha256PayloadHashPrefix, StringComparison.Ordinal))
        {
            return null;
        }

        if (hashLength == Sha256PayloadHashLength && HasCanonicalSha256PayloadHashSuffix(payloadHash))
        {
            return payloadHash;
        }

        throw new InvalidOperationException(InvalidPayloadHashMessage);
    }

    /// <summary>Rejects persisted payloads that cannot fit within the current read budget.</summary>
    /// <param name="payloadLength">The persisted payload length.</param>
    /// <param name="metadataLength">The persisted payload metadata byte length.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <exception cref="QueueCapacityExceededException">The payload cannot fit within the current adapter budget.</exception>
    private static void ThrowIfPayloadExceedsReadBudget(long payloadLength, long metadataLength, long maximumPayloadBytes)
    {
        var retainedLength = checked(payloadLength + metadataLength);
        if (retainedLength <= maximumPayloadBytes && payloadLength <= int.MaxValue)
        {
            return;
        }

        throw new QueueCapacityExceededException("The SQLite command worker has reached its configured capacity.", canFitWhenEmpty: false);
    }

    /// <summary>Validates the persisted payload hash through incremental SQLite BLOB I/O.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="payloadLength">The projected payload length.</param>
    /// <param name="payloadHash">The stored canonical payload hash.</param>
    /// <exception cref="InvalidOperationException">Stored SQLite payload bytes do not match their hash.</exception>
    private static void ValidatePayloadHash(
        SqliteConnection connection,
        SqliteDataReader reader,
        SqlitePayloadColumns columns,
        long payloadLength,
        string payloadHash)
    {
        using var payload = OpenPayloadBlob(connection, reader, columns, payloadLength);
        SqlitePayloadStreamIntegrity.ValidateCanonicalSha256Hash(
            payload,
            payloadHash,
            "The SQLite payload hash does not match the stored payload bytes.");
    }

    /// <summary>Reads persisted payload bytes through incremental SQLite BLOB I/O.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="payloadLength">The projected payload length.</param>
    /// <returns>The payload bytes.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite payload bytes are invalid.</exception>
    private static byte[] ReadPayloadBytes(
        SqliteConnection connection,
        SqliteDataReader reader,
        SqlitePayloadColumns columns,
        long payloadLength)
    {
        using var payload = OpenPayloadBlob(connection, reader, columns, payloadLength);
        return SqlitePayloadStreamIntegrity.ReadExactly(payload, payloadLength, "The SQLite payload bytes are invalid.");
    }

    /// <summary>Opens a read-only SQLite BLOB stream for the projected payload row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="payloadLength">The projected payload length.</param>
    /// <returns>The opened BLOB stream.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite payload bytes are invalid.</exception>
    private static SqliteBlob OpenPayloadBlob(
        SqliteConnection connection,
        SqliteDataReader reader,
        SqlitePayloadColumns columns,
        long payloadLength)
    {
        var rowId = ReadPositiveLong(reader, columns.Source.RowIdIndex, "The SQLite payload rowid is invalid.");
        var payload = new SqliteBlob(
            connection,
            columns.Source.TableName,
            columns.Source.PayloadColumnName,
            rowId,
            readOnly: true);
        return SqlitePayloadStreamIntegrity.AcceptLength(
            payload,
            payloadLength,
            "The SQLite payload length is inconsistent.");
    }

    /// <summary>Determines whether a payload hash has a canonical SHA-256 suffix.</summary>
    /// <param name="payloadHash">The payload hash.</param>
    /// <returns>Whether the hash shape is canonical.</returns>
    private static bool HasCanonicalSha256PayloadHashSuffix(string payloadHash)
    {
        try
        {
            return Convert.FromBase64String(payloadHash.Substring(Sha256PayloadHashPrefix.Length)).Length == Sha256ByteCount;
        }
        catch (FormatException)
        {
            return false;
        }
    }
}
