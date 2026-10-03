// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes SQLite statements for local commit and recovery rows.</summary>
/// <content>Protects and unprotects column values when the store encrypts records at rest.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Gets the stored form of a TEXT value.</summary>
    /// <param name="command">The command whose connection carries the optional cipher.</param>
    /// <param name="value">The plaintext value.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The value to bind.</returns>
    internal static string ProtectText(SqliteStatement command, string value, SqliteRecordContext context, string column) =>
        SqliteRecordCipher.For(command.Database)?.ProtectText(value, context, column) ?? value;

    /// <summary>Gets the stored form of a nullable TEXT value.</summary>
    /// <param name="command">The command whose connection carries the optional cipher.</param>
    /// <param name="value">The plaintext value.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The value to bind.</returns>
    internal static object ProtectNullableText(SqliteStatement command, string? value, SqliteRecordContext context, string column) =>
        value is null ? DBNull.Value : ProtectText(command, value, context, column);

    /// <summary>Gets the stored form of a BLOB value.</summary>
    /// <param name="command">The command whose connection carries the optional cipher.</param>
    /// <param name="value">The plaintext bytes.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The bytes to bind.</returns>
    internal static byte[] ProtectBytes(SqliteStatement command, ReadOnlySpan<byte> value, SqliteRecordContext context, string column) =>
        SqliteRecordCipher.For(command.Database)?.ProtectBytes(value, context, column) ?? value.ToArray();

    /// <summary>Gets the plaintext form of a stored TEXT value.</summary>
    /// <param name="connection">The connection that carries the optional cipher.</param>
    /// <param name="stored">The stored value.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The plaintext value.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The stored value fails authentication.</exception>
    internal static string UnprotectText(SqliteDatabase connection, string stored, SqliteRecordContext context, string column) =>
        SqliteRecordCipher.For(connection)?.UnprotectText(stored, context, column) ?? stored;

    /// <summary>Gets the plaintext form of a nullable stored TEXT value.</summary>
    /// <param name="connection">The connection that carries the optional cipher.</param>
    /// <param name="stored">The stored value.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The plaintext value.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The stored value fails authentication.</exception>
    internal static string? UnprotectNullableText(SqliteDatabase connection, string? stored, SqliteRecordContext context, string column) =>
        stored is null ? null : UnprotectText(connection, stored, context, column);

    /// <summary>Gets the plaintext form of a stored BLOB value.</summary>
    /// <param name="connection">The connection that carries the optional cipher.</param>
    /// <param name="stored">The stored bytes.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The plaintext bytes.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The stored value fails authentication.</exception>
    internal static byte[] UnprotectBytes(SqliteDatabase connection, byte[] stored, SqliteRecordContext context, string column) =>
        SqliteRecordCipher.For(connection)?.UnprotectBytes(stored, context, column) ?? stored;

    /// <summary>Reads and unprotects a nullable TEXT column that belongs to a quarantinable record.</summary>
    /// <param name="connection">The connection that carries the optional cipher.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <param name="context">The record context.</param>
    /// <param name="column">The column name.</param>
    /// <param name="operationId">The affected operation, when known.</param>
    /// <returns>The plaintext value.</returns>
    /// <exception cref="SqlitePayloadQuarantineException">The stored value fails authentication.</exception>
    internal static string? ReadProtectedNullableText(
        SqliteDatabase connection,
        SqliteRows reader,
        int index,
        SqliteRecordContext context,
        string column,
        OperationId? operationId)
    {
        try
        {
            return UnprotectNullableText(connection, ReadNullableString(reader, index), context, column);
        }
        catch (LocalStoreRecordAuthenticationException exception)
        {
            throw SqlitePayloadQuarantineException.ForAuthenticationFailure(operationId, exception);
        }
    }

    /// <summary>Reads the next client sequence of a stream without reading its protected cursor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The next client sequence.</returns>
    /// <exception cref="InvalidOperationException">The stream row is missing or invalid.</exception>
    internal static long ReadNextClientSequence(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT next_client_sequence FROM oc_streams
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """);
        AddStreamParameters(command, storeIdentity, streamId);
        return command.Scalar() is long sequence && sequence > 0
            ? sequence
            : throw new InvalidOperationException("The SQLite stream sequence is invalid.");
    }

    /// <summary>Reads a dead-letter reason code, authenticating it with the dead-letter state when the store is protected.</summary>
    /// <param name="connection">The connection that carries the optional cipher.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="reasonIndex">The reason code column index.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="attemptCount">The stored attempt count.</param>
    /// <param name="changedAtIndex">The state change timestamp column index.</param>
    /// <returns>The reason code.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    /// <exception cref="SqlitePayloadQuarantineException">The protected reason code fails authentication.</exception>
    internal static string? ReadDeadLetterReasonCode(
        SqliteDatabase connection,
        SqliteRows reader,
        int reasonIndex,
        OperationId operationId,
        int attemptCount,
        int changedAtIndex)
    {
        if (SqliteRecordCipher.For(connection) is null)
        {
            return ReadReasonCode(reader, reasonIndex);
        }

        var changedAtUtc = ReadString(reader, changedAtIndex, "The SQLite operation state timestamp is invalid.");
        var reason = ReadProtectedNullableText(
            connection,
            reader,
            reasonIndex,
            SqliteRecordContext.DeadLetter(operationId, attemptCount, changedAtUtc),
            SqliteRecordContext.ReasonCodeColumn,
            operationId);
        return reason is null || reason.Length > 0
            ? reason
            : throw new InvalidOperationException("The SQLite operation reason code is invalid.");
    }

    /// <summary>Reads a payload from a plaintext store, validating canonical hashes through incremental BLOB I/O.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="schemaVersion">The validated schema version.</param>
    /// <param name="payloadLength">The projected payload length.</param>
    /// <returns>The payload.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    private static PayloadEnvelope ReadPlaintextPayload(
        SqliteDatabase connection,
        SqliteRows reader,
        SqlitePayloadColumns columns,
        int schemaVersion,
        long payloadLength)
    {
        var canonicalPayloadHash = ReadCanonicalPayloadHashPreflight(reader, columns.Evidence);
        if (canonicalPayloadHash is not null)
        {
            ValidatePayloadHash(connection, reader, columns, payloadLength, canonicalPayloadHash);
        }

        var payloadHash = ReadString(reader, columns.HashIndex, InvalidPayloadHashMessage);
        return new(
            ReadString(reader, columns.ContractIndex, "The SQLite payload contract is invalid."),
            schemaVersion,
            ReadString(reader, columns.ContentTypeIndex, "The SQLite payload content type is invalid."),
            ReadPayloadBytes(connection, reader, columns, payloadLength),
            payloadHash);
    }

    /// <summary>Reads, authenticates, and decrypts a payload from a protected store.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="cipher">The record cipher.</param>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="schemaVersion">The validated schema version.</param>
    /// <param name="payloadLength">The projected ciphertext length.</param>
    /// <returns>The payload.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid or fails authentication.</exception>
    private static PayloadEnvelope ReadProtectedPayload(
        SqliteDatabase connection,
        SqliteRecordCipher cipher,
        SqliteRows reader,
        SqlitePayloadColumns columns,
        int schemaVersion,
        long payloadLength)
    {
        var contractId = ReadString(reader, columns.ContractIndex, "The SQLite payload contract is invalid.");
        var contentType = ReadString(reader, columns.ContentTypeIndex, "The SQLite payload content type is invalid.");
        var context = columns.Source.Context.WithPayloadMetadata(contractId, schemaVersion, contentType);
        var payloadHash = cipher.UnprotectText(
            ReadString(reader, columns.HashIndex, InvalidPayloadHashMessage),
            context,
            SqliteRecordContext.PayloadHashColumn);
        var payload = cipher.UnprotectBytes(
            ReadPayloadBytes(connection, reader, columns, payloadLength),
            context,
            SqliteRecordContext.PayloadColumn);
        ValidateDecryptedPayloadHash(payloadHash, payload);
        return new(contractId, schemaVersion, contentType, payload, payloadHash);
    }

    /// <summary>Validates a decrypted payload against its canonical SHA-256 hash when the hash uses the canonical form.</summary>
    /// <param name="payloadHash">The decrypted payload hash.</param>
    /// <param name="payload">The decrypted payload bytes.</param>
    /// <exception cref="InvalidOperationException">The hash is malformed or does not match the payload.</exception>
    private static void ValidateDecryptedPayloadHash(string payloadHash, byte[] payload)
    {
        if (!payloadHash.StartsWith(Sha256PayloadHashPrefix, StringComparison.Ordinal))
        {
            return;
        }

        if (payloadHash.Length != Sha256PayloadHashLength || !HasCanonicalSha256PayloadHashSuffix(payloadHash))
        {
            throw new InvalidOperationException(InvalidPayloadHashMessage);
        }

        using var stream = new MemoryStream(payload, writable: false);
        SqlitePayloadStreamIntegrity.ValidateCanonicalSha256Hash(
            stream,
            payloadHash,
            "The SQLite payload hash does not match the stored payload bytes.");
    }

    /// <summary>Compares and replaces a protected stream cursor.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="expectedCursor">The expected current server cursor.</param>
    /// <param name="nextCursor">The next server cursor.</param>
    /// <exception cref="InvalidOperationException">The stream row is missing, the cursor is stale, or it fails authentication.</exception>
    private static void UpdateProtectedServerCursor(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        string? expectedCursor,
        string nextCursor)
    {
        if (!TryReadStreamState(connection, transaction, storeIdentity, streamId, out var stream)
            || !string.Equals(stream.ServerCursor, expectedCursor, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The SQLite stream cursor does not match the expected cursor.");
        }

        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            UPDATE oc_streams
            SET server_cursor = $nextCursor
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """);
        AddStreamParameters(command, storeIdentity, streamId);
        _ = command.Bind(
            "$nextCursor",
            ProtectText(command, nextCursor, SqliteRecordContext.Stream(streamId), SqliteRecordContext.ServerCursorColumn));
        if (command.Execute() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The SQLite stream cursor does not match the expected cursor.");
    }
}
