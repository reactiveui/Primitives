// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

/// <summary>Serializes and validates WebSocket protocol frames.</summary>
internal static class WebSocketProtocol
{
    /// <summary>The shared JSON settings for the WebSocket wire protocol.</summary>
    internal static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true,
        MaxDepth = 32,
    };

    /// <summary>Gets source-generated JSON metadata for the wire protocol.</summary>
    internal static WebSocketJsonSerializerContext JsonContext { get; } = new(JsonOptions);

    /// <summary>Serializes a protocol frame and its registered body.</summary>
    /// <typeparam name="TBody">The protocol body type.</typeparam>
    /// <param name="messageType">The frame type.</param>
    /// <param name="messageId">The frame identifier.</param>
    /// <param name="correlationId">The optional request identifier.</param>
    /// <param name="body">The serialized body.</param>
    /// <returns>The UTF-8 encoded frame.</returns>
    /// <exception cref="NotSupportedException">The body type is not registered for source generation.</exception>
    internal static byte[] Serialize<TBody>(string messageType, Guid messageId, Guid? correlationId, TBody body)
    {
        var frame = new Frame(
            messageType,
            messageId,
            correlationId,
            JsonSerializer.SerializeToElement(body, GetTypeInfo<TBody>()));
        return JsonSerializer.SerializeToUtf8Bytes(frame, JsonContext.Frame);
    }

    /// <summary>Parses and validates a protocol frame.</summary>
    /// <param name="bytes">The UTF-8 encoded frame.</param>
    /// <param name="maximumBytes">The largest accepted frame size.</param>
    /// <returns>The parsed frame.</returns>
    /// <exception cref="WebSocketRemoteTransportException">The frame exceeds the limit or has invalid JSON.</exception>
    internal static Frame Parse(ReadOnlySpan<byte> bytes, int maximumBytes)
    {
        if (bytes.Length > maximumBytes)
        {
            throw new WebSocketRemoteTransportException("message-too-large", "The WebSocket message exceeds the configured limit.");
        }

        try
        {
            var frame = JsonSerializer.Deserialize(bytes, JsonContext.Frame);
            return frame is null || string.IsNullOrWhiteSpace(frame.MessageType)
                ? throw new WebSocketRemoteTransportException("protocol-error", "The WebSocket message is not a valid protocol frame.")
                : frame;
        }
        catch (JsonException exception)
        {
            throw new WebSocketRemoteTransportException("protocol-error", exception.Message);
        }
    }

    /// <summary>Gets source-generated metadata for a registered protocol body.</summary>
    /// <typeparam name="T">The protocol body type.</typeparam>
    /// <returns>The generated JSON metadata.</returns>
    /// <exception cref="NotSupportedException">The body type is not registered for source generation.</exception>
    internal static JsonTypeInfo<T> GetTypeInfo<T>()
    {
        if (JsonContext.GetTypeInfo(typeof(T)) is JsonTypeInfo<T> typeInfo)
        {
            return typeInfo;
        }

        throw new NotSupportedException($"The WebSocket protocol does not support the body type '{typeof(T)}'.");
    }

    /// <summary>A validated protocol frame.</summary>
    /// <param name="MessageType">The frame type.</param>
    /// <param name="MessageId">The frame identifier.</param>
    /// <param name="CorrelationId">The optional request identifier.</param>
    /// <param name="Body">The JSON body.</param>
    internal sealed record Frame(
        string MessageType,
        Guid MessageId,
        Guid? CorrelationId,
        JsonElement Body);
}
