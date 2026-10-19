// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>The query keys emitted by the client subscribe path.</summary>
    private static readonly HashSet<string> KnownQueryKeys =
    [
        StreamIdPropertyName,
        SubscriptionIdPropertyName,
        CursorPropertyName,
        PositionKindPropertyName,
        TimestampPropertyName,
        SequencePropertyName,
        InitialCursorPropertyName,
    ];

    /// <summary>Adds one decoded key/value pair after rejecting missing separators, unknown keys, and duplicates.</summary>
    /// <param name="pair">The encoded query pair.</param>
    /// <param name="values">The collected query values.</param>
    /// <exception cref="HttpRemoteTransportException">The pair is malformed, duplicated, or unknown.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AddQueryPair(ReadOnlySpan<char> pair, Dictionary<string, string> values)
    {
        var equals = pair.IndexOf('=');
        if (equals <= 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        var key = DecodeQueryComponent(pair.Slice(0, equals));
        var value = DecodeQueryComponent(pair.Slice(equals + 1));
        if (IsKnownQueryKey(key) && !values.ContainsKey(key))
        {
            values.Add(key, value);
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Decodes a query component using strict UTF-8 for both raw Unicode and percent-encoded bytes.</summary>
    /// <param name="value">The encoded query component.</param>
    /// <returns>The decoded component text.</returns>
    /// <exception cref="HttpRemoteTransportException">The component contains malformed Unicode or percent escapes.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string DecodeQueryComponent(ReadOnlySpan<char> value)
    {
        using MemoryStream buffer = new();
        var index = 0;
        while (index < value.Length)
        {
            if (value[index] == '%')
            {
                WritePercentEncodedByte(value, ref index, buffer);
                continue;
            }

            var start = index;
            while (index < value.Length && value[index] != '%')
            {
                index++;
            }

            var text = value.Slice(start, index - start).ToString();
            var bytes = StrictUtf8.GetBytes(text);
            buffer.Write(bytes, 0, bytes.Length);
        }

        return StrictUtf8.GetString(buffer.ToArray());
    }

    /// <summary>Writes one percent-encoded query byte into the destination buffer.</summary>
    /// <param name="value">The full encoded value.</param>
    /// <param name="index">The current percent marker index.</param>
    /// <param name="buffer">The destination byte buffer.</param>
    /// <exception cref="HttpRemoteTransportException">The escape sequence is malformed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WritePercentEncodedByte(ReadOnlySpan<char> value, ref int index, MemoryStream buffer)
    {
        if (index + PercentEncodedByteHexDigits >= value.Length
            || !TryReadHex(value[index + 1], out var high)
            || !TryReadHex(value[index + PercentEncodedByteHexDigits], out var low))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        buffer.WriteByte((byte)((high << 4) | low));
        index += PercentEncodedByteHexDigits + 1;
    }

    /// <summary>Reads a single hexadecimal character from a percent escape.</summary>
    /// <param name="character">The encoded character.</param>
    /// <param name="value">The numeric nibble value.</param>
    /// <returns><see langword="true"/> when the character is hexadecimal; otherwise <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryReadHex(char character, out int value)
    {
        if (character is >= '0' and <= '9')
        {
            value = character - '0';
            return true;
        }

        if (character is >= 'A' and <= 'F')
        {
            value = character - 'A' + DecimalRadix;
            return true;
        }

        if (character is >= 'a' and <= 'f')
        {
            value = character - 'a' + DecimalRadix;
            return true;
        }

        value = 0;
        return false;
    }

    /// <summary>Checks whether a decoded query key is emitted by the client subscribe path.</summary>
    /// <param name="key">The decoded query key.</param>
    /// <returns><see langword="true"/> when the key is part of the subscribe protocol.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsKnownQueryKey(string key) => KnownQueryKeys.Contains(key);

    /// <summary>Requires the mandatory subscribe query fields before constructing the domain request.</summary>
    /// <param name="values">The decoded query values.</param>
    /// <param name="keys">The required keys.</param>
    /// <exception cref="HttpRemoteTransportException">A required key is missing.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RequireKeys(Dictionary<string, string> values, params string[] keys)
    {
        for (var index = 0; index < keys.Length; index++)
        {
            if (!values.ContainsKey(keys[index]))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Creates canonical replay query fields from the exact decoded query values.</summary>
    /// <param name="values">The decoded query values.</param>
    /// <returns>The decoded query fields.</returns>
    private static KeyValuePair<string, string>[] CreateQueryFields(Dictionary<string, string> values)
    {
        var fields = new KeyValuePair<string, string>[values.Count];
        var index = 0;
        foreach (var value in values)
        {
            fields[index] = value;
            index++;
        }

        return fields;
    }

    /// <summary>Reads an optional subscribe query value while preserving absence versus an invalid empty value.</summary>
    /// <param name="values">The decoded query values.</param>
    /// <param name="key">The optional key.</param>
    /// <returns>The decoded value when present; otherwise <see langword="null"/>.</returns>
    /// <exception cref="HttpRemoteTransportException">The optional value is present but empty.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string? GetOptionalQueryValue(Dictionary<string, string> values, string key)
    {
        if (!values.TryGetValue(key, out var value))
        {
            return null;
        }

        if (value.Length is 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        return value;
    }

    /// <summary>Creates the immutable subscribe start position from its mutually exclusive query fields.</summary>
    /// <param name="kind">The decoded start position kind.</param>
    /// <param name="values">The decoded query values.</param>
    /// <returns>The validated start position.</returns>
    /// <exception cref="HttpRemoteTransportException">The position fields are missing, conflicting, or malformed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StartPosition CreateStartPosition(StartPositionKind kind, Dictionary<string, string> values)
    {
        switch (kind)
        {
            case StartPositionKind.Latest:
            {
                RejectKeys(values, TimestampPropertyName, SequencePropertyName, InitialCursorPropertyName);
                return StartPosition.Latest;
            }

            case StartPositionKind.FromTimestamp:
            {
                RejectKeys(values, SequencePropertyName, InitialCursorPropertyName);
                if (!values.TryGetValue(TimestampPropertyName, out var value)
                    || !DateTimeOffset.TryParseExact(value, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var timestamp))
                {
                    throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
                }

                return StartPosition.FromTimestamp(timestamp);
            }

            case StartPositionKind.FromSequence:
            {
                RejectKeys(values, TimestampPropertyName, InitialCursorPropertyName);
                if (!values.TryGetValue(SequencePropertyName, out var value)
                    || !long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var sequence))
                {
                    throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
                }

                return StartPosition.FromSequence(sequence);
            }

            case StartPositionKind.FromCursor:
            {
                RejectKeys(values, TimestampPropertyName, SequencePropertyName);
                var initialCursor = GetOptionalQueryValue(values, InitialCursorPropertyName)
                    ?? throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
                return StartPosition.FromCursor(initialCursor);
            }

            default:
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Rejects query fields that conflict with the selected start position kind.</summary>
    /// <param name="values">The decoded query values.</param>
    /// <param name="keys">The keys that must be absent.</param>
    /// <exception cref="HttpRemoteTransportException">A conflicting key is present.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RejectKeys(Dictionary<string, string> values, params string[] keys)
    {
        for (var index = 0; index < keys.Length; index++)
        {
            if (values.ContainsKey(keys[index]))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Parses a query integer using invariant protocol formatting.</summary>
    /// <param name="value">The decoded integer text.</param>
    /// <returns>The parsed integer.</returns>
    /// <exception cref="HttpRemoteTransportException">The integer text is malformed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ParseInt32(string value)
    {
        if (int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            return result;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Converts a wire feature bit field after rejecting unknown capability bits.</summary>
    /// <param name="value">The encoded capability flags.</param>
    /// <returns>The declared transport capabilities.</returns>
    /// <exception cref="HttpRemoteTransportException">The bit field contains unknown capabilities.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteTransportCapabilities ToCapabilities(int value)
    {
        var capabilities = (RemoteTransportCapabilities)value;
        if ((capabilities & ~KnownFeatures) == 0)
        {
            return capabilities;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Converts a wire delivery guarantee after rejecting undefined enum values.</summary>
    /// <param name="value">The encoded enum value.</param>
    /// <returns>The declared delivery guarantee.</returns>
    /// <exception cref="HttpRemoteTransportException">The value is not a declared delivery guarantee.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DeliveryGuarantee ToDeliveryGuarantee(int value) => value switch
    {
        (int)DeliveryGuarantee.AtMostOnce => DeliveryGuarantee.AtMostOnce,
        (int)DeliveryGuarantee.AtLeastOnce => DeliveryGuarantee.AtLeastOnce,
        (int)DeliveryGuarantee.ExactlyOnce => DeliveryGuarantee.ExactlyOnce,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts a wire operation durability after rejecting undefined enum values.</summary>
    /// <param name="value">The encoded enum value.</param>
    /// <returns>The declared durability.</returns>
    /// <exception cref="HttpRemoteTransportException">The value is not a declared durability.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OperationDurability ToDurability(int value) => value switch
    {
        (int)OperationDurability.Durable => OperationDurability.Durable,
        (int)OperationDurability.Volatile => OperationDurability.Volatile,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts a wire conflict policy after rejecting undefined enum values.</summary>
    /// <param name="value">The encoded enum value.</param>
    /// <returns>The declared conflict policy.</returns>
    /// <exception cref="HttpRemoteTransportException">The value is not a declared conflict policy.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ConflictPolicy ToConflictPolicy(int value) => value switch
    {
        (int)ConflictPolicy.LastWriterWins => ConflictPolicy.LastWriterWins,
        (int)ConflictPolicy.Merge => ConflictPolicy.Merge,
        (int)ConflictPolicy.Custom => ConflictPolicy.Custom,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts a wire operation type after rejecting undefined enum values.</summary>
    /// <param name="value">The encoded enum value.</param>
    /// <returns>The declared operation type.</returns>
    /// <exception cref="HttpRemoteTransportException">The value is not a declared operation type.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperationType ToOperationType(int value) => value switch
    {
        (int)SyncOperationType.Append => SyncOperationType.Append,
        (int)SyncOperationType.Update => SyncOperationType.Update,
        (int)SyncOperationType.Delete => SyncOperationType.Delete,
        (int)SyncOperationType.Custom => SyncOperationType.Custom,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts a wire operation result kind after rejecting undefined enum values.</summary>
    /// <param name="value">The encoded enum value.</param>
    /// <returns>The declared result kind.</returns>
    /// <exception cref="HttpRemoteTransportException">The value is not a declared result kind.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OperationResultKind ToOperationResultKind(int value) => value switch
    {
        (int)OperationResultKind.Accepted => OperationResultKind.Accepted,
        (int)OperationResultKind.Conflict => OperationResultKind.Conflict,
        (int)OperationResultKind.Rejected => OperationResultKind.Rejected,
        (int)OperationResultKind.Retryable => OperationResultKind.Retryable,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Creates a validated subscribe request from decoded query values.</summary>
    /// <param name="values">The decoded query values.</param>
    /// <returns>The subscribe request.</returns>
    /// <exception cref="HttpRemoteTransportException">The query does not describe a valid subscribe request.</exception>
    private RemoteSubscribeRequest CreateSubscribeRequest(Dictionary<string, string> values)
    {
        RequireKeys(values, StreamIdPropertyName, SubscriptionIdPropertyName, PositionKindPropertyName);
        var streamId = new StreamId(values[StreamIdPropertyName]);
        var subscriptionId = new SubscriptionId(Guid.Parse(values[SubscriptionIdPropertyName]));
        var cursor = GetOptionalQueryValue(values, CursorPropertyName);
        var kind = (StartPositionKind)ParseInt32(values[PositionKindPropertyName]);
        var position = CreateStartPosition(kind, values);
        var request = new RemoteSubscribeRequest(streamId, subscriptionId, cursor, position);
        ValidateSubscribeRequest(request);
        return request;
    }

    /// <summary>Parses the bounded subscribe query into decoded values without accepting unknown fields.</summary>
    /// <param name="query">The encoded query string, with or without a leading question mark.</param>
    /// <returns>The decoded query values.</returns>
    /// <exception cref="HttpRemoteTransportException">The query is empty, malformed, duplicated, unknown, or too large.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Dictionary<string, string> ParseQuery(string query)
    {
        ValidateProtocolString(query, _limits.MaximumQueryBytes);
        var span = query.AsSpan();
        if (!span.IsEmpty && span[0] == '?')
        {
            span = span.Slice(1);
        }

        if (span.IsEmpty)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        Dictionary<string, string> values = [with(comparer: StringComparer.Ordinal)];
        while (!span.IsEmpty)
        {
            if (values.Count >= _limits.MaximumQueryKeys)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            var separator = span.IndexOf('&');
            var pair = separator < 0 ? span : span.Slice(0, separator);
            if (pair.IsEmpty)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            AddQueryPair(pair, values);
            if (separator < 0)
            {
                break;
            }

            span = span.Slice(separator + 1);
            if (span.IsEmpty)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }

        return values;
    }
}
