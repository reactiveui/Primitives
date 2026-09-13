// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Ensures a received batch and its events belong to the stream requested by the caller.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="expectedStreamId">The expected stream, when the caller bound the response to one stream.</param>
    /// <exception cref="HttpRemoteTransportException">The batch or an event belongs to another stream.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateStream(RemoteEventBatch batch, StreamId? expectedStreamId)
    {
        if (expectedStreamId is null)
        {
            return;
        }

        var expectedValue = expectedStreamId.Value.Value;
        if (!StringComparer.Ordinal.Equals(batch.StreamId.Value, expectedValue))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        for (var index = 0; index < batch.Events.Count; index++)
        {
            if (!StringComparer.Ordinal.Equals(batch.Events[index].StreamId.Value, expectedValue))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
            }
        }
    }

    /// <summary>Verifies each received batch advances from the caller's current cursor.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="currentCursor">The durable cursor known by the caller.</param>
    /// <exception cref="HttpRemoteTransportException">The batch cursor chain is discontinuous.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateCursorContinuity(RemoteEventBatch batch, ref string? currentCursor)
    {
        if (currentCursor is null)
        {
            currentCursor = batch.NextCursor;
            return;
        }

        if (StringComparer.Ordinal.Equals(batch.NextCursor, currentCursor))
        {
            return;
        }

        if (!StringComparer.Ordinal.Equals(batch.PreviousCursor, currentCursor))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        currentCursor = batch.NextCursor;
    }

    /// <summary>Serializes a generated protocol DTO into a bounded UTF-8 JSON buffer.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="dto">The DTO to serialize.</param>
    /// <param name="typeInfo">The generated JSON metadata.</param>
    /// <param name="maximumBytes">The encoded body byte limit.</param>
    /// <returns>The serialized body bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The serialized body exceeds the configured limit.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Serialize<T>(T dto, JsonTypeInfo<T> typeInfo, int maximumBytes)
    {
        HttpBoundedBufferWriter bufferWriter = new(maximumBytes);
        using Utf8JsonWriter jsonWriter = new(bufferWriter);
        JsonSerializer.Serialize(jsonWriter, dto, typeInfo);
        jsonWriter.Flush();

        return bufferWriter.ToArray();
    }

    /// <summary>Rejects encoded peer bodies before JSON parsing can allocate DTO graphs.</summary>
    /// <param name="bytes">The encoded body bytes.</param>
    /// <param name="maximumBytes">The encoded body byte limit.</param>
    /// <exception cref="HttpRemoteTransportException">The body exceeds the configured limit.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateEncodedLength(byte[] bytes, int maximumBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(bytes);
        if (bytes.Length <= maximumBytes)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Preflights and deserializes a generated protocol DTO from bounded UTF-8 JSON.</summary>
    /// <typeparam name="T">The DTO type.</typeparam>
    /// <param name="bytes">The encoded body bytes.</param>
    /// <param name="typeInfo">The generated JSON metadata.</param>
    /// <param name="maximumBytes">The encoded body byte limit.</param>
    /// <param name="preflight">The schema and collection-bound preflight action.</param>
    /// <returns>The deserialized DTO.</returns>
    /// <exception cref="HttpRemoteTransportException">The body is malformed or violates configured limits.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private T Deserialize<T>(byte[] bytes, JsonTypeInfo<T> typeInfo, int maximumBytes, Action<JsonElement> preflight)
    {
        ValidateEncodedLength(bytes, maximumBytes);
        ValidateJsonPreflight(bytes, preflight);
        try
        {
            var dto = JsonSerializer.Deserialize(bytes, typeInfo);
            ArgumentExceptionHelper.ThrowIfNull(dto);
            return dto;
        }
        catch (JsonException exception)
        {
            throw CreateProtocolViolation(exception);
        }
    }

    /// <summary>Checks JSON depth, closed schema, and collection bounds before DTO deserialization.</summary>
    /// <param name="bytes">The encoded body bytes.</param>
    /// <param name="preflight">The schema-specific preflight action.</param>
    /// <exception cref="HttpRemoteTransportException">The JSON is malformed or violates the protocol schema.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateJsonPreflight(byte[] bytes, Action<JsonElement> preflight)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes, new JsonDocumentOptions { MaxDepth = _limits.MaximumJsonDepth });
            preflight(document.RootElement);
        }
        catch (JsonException exception)
        {
            throw CreateProtocolViolation(exception);
        }
    }
}
