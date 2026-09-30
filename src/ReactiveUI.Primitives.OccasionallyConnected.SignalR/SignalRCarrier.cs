// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http.Headers;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Bounds the RPC carrier independently of the enclosed protocol.</summary>
internal static class SignalRCarrier
{
    /// <summary>The maximum encoded carrier size.</summary>
    internal const int MaximumBytes = 4 * 1024 * 1024;

    /// <summary>The maximum body size before base64 carrier encoding.</summary>
    internal const int MaximumBodyBytes = 2 * 1024 * 1024;

    /// <summary>The maximum combined header and path characters.</summary>
    private const int MaximumHeaderCharacters = 32 * 1024;

    /// <summary>Encodes a bounded carrier.</summary>
    /// <param name="message">The message.</param>
    /// <returns>The encoded bytes.</returns>
    internal static byte[] Encode(SignalRCarrierMessage message)
    {
        Validate(message);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(message, SignalRCarrierJsonContext.Default.SignalRCarrierMessage);
        ValidateSize(bytes.Length, MaximumBytes);
        return bytes;
    }

    /// <summary>Decodes a bounded carrier.</summary>
    /// <param name="bytes">The bytes.</param>
    /// <returns>The decoded message.</returns>
    /// <exception cref="HttpRemoteTransportException">The carrier is invalid or too large.</exception>
    internal static SignalRCarrierMessage Decode(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        ValidateSize(bytes.Length, MaximumBytes);
        try
        {
            var message = JsonSerializer.Deserialize(bytes, SignalRCarrierJsonContext.Default.SignalRCarrierMessage)
                ?? throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            Validate(message);
            return message;
        }
        catch (JsonException)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }
    }

    /// <summary>Copies the bounded HTTP headers without changing signed replay values.</summary>
    /// <param name="source">The source headers.</param>
    /// <param name="content">The source content.</param>
    /// <returns>The owned headers.</returns>
    internal static Dictionary<string, string[]> GetHeaders(HttpHeaders source, HttpContent? content)
    {
        var headers = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var header in source)
        {
            headers.Add(header.Key, new List<string>(header.Value).ToArray());
        }

        if (content is not null)
        {
            foreach (var header in content.Headers)
            {
                headers.Add(header.Key, new List<string>(header.Value).ToArray());
            }
        }

        return headers;
    }

    /// <summary>Copies headers to the corresponding request or response collection.</summary>
    /// <param name="headers">The headers.</param>
    /// <param name="target">The target headers.</param>
    /// <param name="content">The target content.</param>
    /// <exception cref="HttpRemoteTransportException">A content header is supplied without content.</exception>
    internal static void SetHeaders(Dictionary<string, string[]> headers, HttpHeaders target, HttpContent? content)
    {
        foreach (var header in headers)
        {
            if (!target.TryAddWithoutValidation(header.Key, header.Value)
                && (content is null || !content.Headers.TryAddWithoutValidation(header.Key, header.Value)))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Rejects unbounded wire allocations.</summary>
    /// <param name="size">The size.</param>
    /// <param name="maximum">The bound.</param>
    /// <exception cref="HttpRemoteTransportException">The size exceeds the limit.</exception>
    private static void ValidateSize(int size, int maximum)
    {
        if (size > maximum)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }
    }

    /// <summary>Checks carrier fields before use.</summary>
    /// <param name="message">The decoded carrier.</param>
    /// <exception cref="HttpRemoteTransportException">The carrier is invalid or too large.</exception>
    /// <exception cref="OverflowException">Header size arithmetic overflowed.</exception>
    private static void Validate(SignalRCarrierMessage message)
    {
        if (message.Body is null || message.Headers is null || message.Headers.Count > 32)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateSize(message.Body.Length, MaximumBodyBytes);
        var characters = message.PathAndQuery?.Length ?? 0;
        foreach (var header in message.Headers)
        {
            if (string.IsNullOrWhiteSpace(header.Key) || header.Value is null || header.Value.Length > 8)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            characters = checked(characters + header.Key.Length + CountCharacters(header.Value));
        }

        ValidateSize(characters, MaximumHeaderCharacters);
    }

    /// <summary>Counts bounded header values.</summary>
    /// <param name="values">The values.</param>
    /// <returns>The character count.</returns>
    /// <exception cref="HttpRemoteTransportException">A value is null.</exception>
    private static int CountCharacters(string[] values)
    {
        var count = 0;
        foreach (var value in values)
        {
            if (value is null)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }

            count = checked(count + value.Length);
        }

        return count;
    }
}
