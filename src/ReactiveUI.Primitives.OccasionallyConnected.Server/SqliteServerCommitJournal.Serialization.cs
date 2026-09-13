// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides SQLite serialization helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Reads an optional state row segment.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="columns">The state columns.</param>
    /// <returns>The state or null.</returns>
    private static ServerState? ReadNullableState(SqliteDataReader reader, StreamId streamId, StateColumns columns)
    {
        const string Message = "The SQLite server journal state is invalid.";
        if (reader.IsDBNull(columns.VersionIndex))
        {
            EnsurePayloadColumnsNull(reader, columns.Payload, Message);
            return null;
        }

        return new(
            streamId,
            ReadValidatedText(reader, columns.VersionIndex, "The SQLite server journal state version is invalid."),
            ReadPayload(reader, columns.Payload));
    }

    /// <summary>Reads an optional payload row segment.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <returns>The payload or null.</returns>
    private static PayloadEnvelope? ReadNullablePayload(SqliteDataReader reader, PayloadColumns columns)
    {
        if (!reader.IsDBNull(columns.ContractIndex))
        {
            return ReadPayload(reader, columns);
        }

        EnsurePayloadColumnsNull(reader, columns, "The SQLite server journal payload is invalid.");
        return null;
    }

    /// <summary>Reads a payload row segment.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <returns>The payload.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static PayloadEnvelope ReadPayload(SqliteDataReader reader, PayloadColumns columns) =>
        new(
            ReadString(reader, columns.ContractIndex, "The SQLite server journal payload contract is invalid."),
            ReadPositiveInt(reader, columns.SchemaIndex, "The SQLite server journal payload schema is invalid."),
            ReadString(reader, columns.ContentTypeIndex, "The SQLite server journal payload content type is invalid."),
            ReadBytes(reader, columns.PayloadIndex, "The SQLite server journal payload bytes are invalid."),
            ReadString(reader, columns.HashIndex, "The SQLite server journal payload hash is invalid."));

    /// <summary>Reads an optional write stamp.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="committedAtIndex">The timestamp index.</param>
    /// <param name="clientIndex">The client index.</param>
    /// <param name="operationIndex">The operation index.</param>
    /// <returns>The write stamp or null.</returns>
    private static ServerWriteStamp? ReadNullableWriteStamp(SqliteDataReader reader, int committedAtIndex, int clientIndex, int operationIndex)
    {
        const string Message = "The SQLite server journal write stamp is invalid.";
        if (reader.IsDBNull(committedAtIndex))
        {
            EnsureColumnsNull(reader, Message, clientIndex, operationIndex);
            return null;
        }

        return new(
            ReadDateTimeOffset(reader, committedAtIndex, "The SQLite server journal write stamp timestamp is invalid."),
            ReadValidatedText(reader, clientIndex, "The SQLite server journal write stamp client is invalid."),
            new(ReadGuid(reader, operationIndex, "The SQLite server journal write stamp operation is invalid.")));
    }

    /// <summary>Reads an optional remote event origin.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="clientIndex">The client index.</param>
    /// <param name="operationIndex">The operation index.</param>
    /// <returns>The origin or null.</returns>
    private static RemoteEventOrigin? ReadNullableOrigin(SqliteDataReader reader, int clientIndex, int operationIndex)
    {
        const string Message = "The SQLite server journal origin is invalid.";
        if (reader.IsDBNull(clientIndex))
        {
            EnsureColumnsNull(reader, Message, operationIndex);
            return null;
        }

        return new(
            ReadValidatedText(reader, clientIndex, "The SQLite server journal origin client is invalid."),
            new(ReadGuid(reader, operationIndex, "The SQLite server journal origin operation is invalid.")));
    }

    /// <summary>Reads an optional operation id.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation id or null.</returns>
    private static OperationId? ReadNullableOperationId(SqliteDataReader reader, int index) =>
        reader.IsDBNull(index) ? null : new(ReadGuid(reader, index, "The SQLite server journal operation id is invalid."));

    /// <summary>Reads an operation key.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="clientIndex">The client index.</param>
    /// <param name="operationIndex">The operation index.</param>
    /// <returns>The operation key.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static ServerOperationKey ReadOperationKey(SqliteDataReader reader, int clientIndex, int operationIndex) =>
        new(
            ReadValidatedText(reader, clientIndex, "The SQLite server journal client is invalid."),
            new(ReadGuid(reader, operationIndex, "The SQLite server journal operation id is invalid.")));

    /// <summary>Reads an operation result kind.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The column index.</param>
    /// <returns>The operation result kind.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static OperationResultKind ReadResultKind(SqliteDataReader reader, int index)
    {
        var kind = (OperationResultKind)ReadInt(reader, index, "The SQLite server journal result kind is invalid.");
        return kind is OperationResultKind.Accepted or OperationResultKind.Conflict or OperationResultKind.Rejected
            ? kind
            : throw new InvalidOperationException("The SQLite server journal result kind is invalid.");
    }

    /// <summary>Adds nullable state parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="state">The state.</param>
    /// <param name="stateBytes">The state bytes.</param>
    private static void AddNullableStateParameters(SqliteCommand command, ServerState? state, long stateBytes)
    {
        if (state is null)
        {
            _ = command.Parameters.AddWithValue("$stateVersion", DBNull.Value);
            AddNullablePayloadParameters(command, "state", null);
            _ = command.Parameters.AddWithValue("$stateBytes", 0);
            return;
        }

        _ = command.Parameters.AddWithValue("$stateVersion", state.Version);
        AddPayloadParameters(command, "state", state.State);
        _ = command.Parameters.AddWithValue("$stateBytes", stateBytes);
    }

    /// <summary>Adds nullable write stamp parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="writeStamp">The stamp.</param>
    private static void AddNullableWriteStampParameters(SqliteCommand command, ServerWriteStamp? writeStamp)
    {
        _ = command.Parameters.AddWithValue("$writeStampCommittedAtUtc", writeStamp.HasValue ? FormatDateTimeOffset(writeStamp.Value.CommittedAtUtc) : DBNull.Value);
        _ = command.Parameters.AddWithValue("$writeStampClientId", writeStamp.HasValue ? writeStamp.Value.ClientId : DBNull.Value);
        _ = command.Parameters.AddWithValue("$writeStampOperationId", writeStamp.HasValue ? writeStamp.Value.OperationId.Value.ToString("D") : DBNull.Value);
    }

    /// <summary>Adds stream parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="streamKey">The stream key.</param>
    private static void AddStreamParameters(SqliteCommand command, ServerStreamKey streamKey)
    {
        _ = command.Parameters.AddWithValue("$tenantId", streamKey.TenantId);
        _ = command.Parameters.AddWithValue("$streamId", streamKey.StreamId.Value);
    }

    /// <summary>Adds operation parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="operationKey">The operation key.</param>
    private static void AddOperationParameters(SqliteCommand command, ServerOperationKey operationKey)
    {
        _ = command.Parameters.AddWithValue("$clientId", operationKey.ClientId);
        _ = command.Parameters.AddWithValue("$operationId", operationKey.OperationId.Value.ToString("D"));
    }

    /// <summary>Adds fingerprint parameter.</summary>
    /// <param name="command">The command.</param>
    /// <param name="fingerprint">The fingerprint.</param>
    private static void AddFingerprintParameter(SqliteCommand command, ServerCommitFingerprint fingerprint)
    {
        _ = command.Parameters.Add("$fingerprint", SqliteType.Blob);
        command.Parameters["$fingerprint"].Value = fingerprint.ToArray();
    }

    /// <summary>Adds payload parameters.</summary>
    /// <param name="command">The command.</param>
    /// <param name="payload">The payload.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AddPayloadParameters(SqliteCommand command, PayloadEnvelope payload) => AddPayloadParameters(command, string.Empty, payload);

    /// <summary>Adds payload parameters with a name prefix.</summary>
    /// <param name="command">The command.</param>
    /// <param name="prefix">The parameter prefix.</param>
    /// <param name="payload">The payload.</param>
    private static void AddPayloadParameters(SqliteCommand command, string prefix, PayloadEnvelope payload)
    {
        var name = GetPayloadParameterName(prefix, PayloadSuffix);
        _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadContractIdSuffix), payload.ContractId);
        _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadSchemaVersionSuffix), payload.SchemaVersion);
        _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadContentTypeSuffix), payload.ContentType);
        _ = command.Parameters.Add(name, SqliteType.Blob);
        command.Parameters[name].Value = payload.Payload.ToArray();
        _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadHashSuffix), payload.PayloadHash);
    }

    /// <summary>Adds nullable payload parameters with a name prefix.</summary>
    /// <param name="command">The command.</param>
    /// <param name="prefix">The parameter prefix.</param>
    /// <param name="payload">The payload.</param>
    private static void AddNullablePayloadParameters(SqliteCommand command, string prefix, PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadContractIdSuffix), DBNull.Value);
            _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadSchemaVersionSuffix), DBNull.Value);
            _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadContentTypeSuffix), DBNull.Value);
            _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadSuffix), DBNull.Value);
            _ = command.Parameters.AddWithValue(GetPayloadParameterName(prefix, PayloadHashSuffix), DBNull.Value);
            return;
        }

        AddPayloadParameters(command, prefix, payload);
    }

    /// <summary>Gets a payload parameter name.</summary>
    /// <param name="prefix">The parameter prefix.</param>
    /// <param name="suffix">The parameter suffix.</param>
    /// <returns>The parameter name.</returns>
    private static string GetPayloadParameterName(string prefix, string suffix) =>
        prefix.Length == 0 ? $"${char.ToLowerInvariant(suffix[0])}{suffix.Remove(0, 1)}" : $"${prefix}{suffix}";

    /// <summary>Reads a string column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The string.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ReadString(SqliteDataReader reader, int index, string message) =>
        ReadStorage<string>(reader.GetValue(index), message);

    /// <summary>Reads and validates a stored server journal identifier-like text column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The validated text.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static string ReadValidatedText(SqliteDataReader reader, int index, string message)
    {
        var text = ReadString(reader, index, message);
        try
        {
            ServerCommitJournalGuard.ValidateText(text, nameof(text));
            return text;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException(message, exception);
        }
    }

    /// <summary>Reads and validates a stored cursor column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The validated cursor.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static string ReadCursor(SqliteDataReader reader, int index, string message)
    {
        var cursor = ReadString(reader, index, message);
        try
        {
            ServerCommitJournalGuard.ValidateCursor(cursor);
            return cursor;
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException(message, exception);
        }
    }

    /// <summary>Reads a nullable string column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The string or null.</returns>
    private static string? ReadNullableString(SqliteDataReader reader, int index, string message) =>
        reader.IsDBNull(index) ? null : ReadString(reader, index, message);

    /// <summary>Reads and validates a nullable stored cursor column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The cursor or null.</returns>
    private static string? ReadNullableCursor(SqliteDataReader reader, int index, string message) =>
        reader.IsDBNull(index) ? null : ReadCursor(reader, index, message);

    /// <summary>Reads a byte array column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static byte[] ReadBytes(SqliteDataReader reader, int index, string message) =>
        !reader.IsDBNull(index) && reader.GetValue(index) is byte[] bytes
            ? bytes
            : throw new InvalidOperationException(message);

    /// <summary>Reads a fingerprint column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <returns>The fingerprint bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static byte[] ReadFingerprint(SqliteDataReader reader, int index)
    {
        var bytes = ReadBytes(reader, index, "The SQLite server journal fingerprint is invalid.");
        return bytes.Length == ServerCommitFingerprint.Length
            ? bytes
            : throw new InvalidOperationException("The SQLite server journal fingerprint is invalid.");
    }

    /// <summary>Reads an integer column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The integer.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static int ReadInt(SqliteDataReader reader, int index, string message)
    {
        var value = ReadLong(reader, index, message);
        ThrowIfFalse(value >= int.MinValue, message);
        ThrowIfFalse(value <= int.MaxValue, message);
        return (int)value;
    }

    /// <summary>Reads a positive integer column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The integer.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static int ReadPositiveInt(SqliteDataReader reader, int index, string message)
    {
        var value = ReadInt(reader, index, message);
        return value > 0 ? value : throw new InvalidOperationException(message);
    }

    /// <summary>Reads a non-negative long column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadNonNegativeLong(SqliteDataReader reader, int index, string message) =>
        ReadNonNegativeLong(ReadLong(reader, index, message), message);

    /// <summary>Reads a non-negative long scalar.</summary>
    /// <param name="value">The scalar.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static long ReadNonNegativeLong(object? value, string message)
    {
        var number = ReadStorage<long>(value, message);
        ThrowIfFalse(number >= 0, message);
        return number;
    }

    /// <summary>Reads an optional non-negative integer-storage column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value or null.</returns>
    private static long? ReadNullableNonNegativeLong(SqliteDataReader reader, int index, string message) =>
        reader.IsDBNull(index) ? null : ReadNonNegativeLong(reader, index, message);

    /// <summary>Reads a stored boolean column encoded as 0 or 1.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The boolean value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static bool ReadBoolean(SqliteDataReader reader, int index, string message)
    {
        var value = ReadLong(reader, index, message);
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw new InvalidOperationException(message),
        };
    }

    /// <summary>Reads an integer-storage column without SQLite type coercion.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The long value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadLong(SqliteDataReader reader, int index, string message) =>
        ReadStorage<long>(reader.GetValue(index), message);

    /// <summary>Reads a non-empty GUID column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The GUID value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static Guid ReadGuid(SqliteDataReader reader, int index, string message)
    {
        var text = ReadString(reader, index, message);
        return Guid.TryParse(text, out var value) && value != Guid.Empty ? value : throw new InvalidOperationException(message);
    }

    /// <summary>Reads and validates a stream identifier.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The stream identifier.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static StreamId ReadStreamId(SqliteDataReader reader, int index, string message)
    {
        var text = ReadString(reader, index, message);
        try
        {
            return new(text);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException(message, exception);
        }
    }

    /// <summary>Reads a date-time offset column.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="index">The index.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The date-time offset.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset ReadDateTimeOffset(SqliteDataReader reader, int index, string message) =>
        ParseDateTimeOffset(ReadString(reader, index, message), message);

    /// <summary>Parses a stored date-time offset.</summary>
    /// <param name="value">The value.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The date-time offset.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static DateTimeOffset ParseDateTimeOffset(string value, string message) =>
        DateTimeOffset.TryParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var timestamp)
            ? timestamp
            : throw new InvalidOperationException(message);

    /// <summary>Formats a date-time offset for storage.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The formatted value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatDateTimeOffset(DateTimeOffset value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    /// <summary>Reads an integer count scalar.</summary>
    /// <param name="value">The scalar.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The count.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static int ReadCount(object? value, string message)
    {
        var count = ReadStorage<long>(value, message);
        ThrowIfFalse(count >= 0, message);
        ThrowIfFalse(count <= int.MaxValue, message);
        return (int)count;
    }

    /// <summary>Reads a raw SQLite provider value without SQLite type coercion.</summary>
    /// <typeparam name="T">The required CLR storage type.</typeparam>
    /// <param name="value">The provider value.</param>
    /// <param name="message">The failure message.</param>
    /// <returns>The typed value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static T ReadStorage<T>(object? value, string message) =>
        value is T typed ? typed : throw new InvalidOperationException(message);

    /// <summary>Throws when a provider invariant is false.</summary>
    /// <param name="condition">Whether the invariant holds.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static void ThrowIfFalse(bool condition, string message) =>
        _ = condition ? true : throw new InvalidOperationException(message);

    /// <summary>Verifies that unused optional payload columns are also null.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="columns">The payload columns.</param>
    /// <param name="message">The failure message.</param>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EnsurePayloadColumnsNull(SqliteDataReader reader, PayloadColumns columns, string message) =>
        EnsureColumnsNull(reader, message, columns.ContractIndex, columns.SchemaIndex, columns.ContentTypeIndex, columns.PayloadIndex, columns.HashIndex);

    /// <summary>Verifies that unused optional columns are null.</summary>
    /// <param name="reader">The reader.</param>
    /// <param name="message">The failure message.</param>
    /// <param name="indexes">The column indexes.</param>
    /// <exception cref="InvalidOperationException">Thrown when stored SQLite data is invalid.</exception>
    private static void EnsureColumnsNull(SqliteDataReader reader, string message, params int[] indexes)
    {
        for (var index = 0; index < indexes.Length; index++)
        {
            if (!reader.IsDBNull(indexes[index]))
            {
                throw new InvalidOperationException(message);
            }
        }
    }

    /// <summary>Identifies the payload column positions in a row.</summary>
    /// <param name="ContractIndex">The contract id column.</param>
    /// <param name="SchemaIndex">The schema version column.</param>
    /// <param name="ContentTypeIndex">The content type column.</param>
    /// <param name="PayloadIndex">The payload bytes column.</param>
    /// <param name="HashIndex">The payload hash column.</param>
    private readonly record struct PayloadColumns(int ContractIndex, int SchemaIndex, int ContentTypeIndex, int PayloadIndex, int HashIndex);

    /// <summary>Identifies the state column positions in a row.</summary>
    /// <param name="VersionIndex">The state version column.</param>
    /// <param name="Payload">The payload columns.</param>
    private readonly record struct StateColumns(int VersionIndex, PayloadColumns Payload);
}
