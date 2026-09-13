// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Provides stateless HTTP protocol codec helpers.</summary>
internal static class HttpProtocolCodecHelper
{
    /// <summary>The JSON string quote byte count.</summary>
    private const long JsonStringQuoteBytes = 2;

    /// <summary>The conservative JSON text escape expansion factor.</summary>
    private const long JsonTextEscapeExpansion = 6;

    /// <summary>The JSON null token byte count.</summary>
    private const long JsonNullTokenBytes = 4;

    /// <summary>Converts milliseconds to a time span.</summary>
    /// <param name="milliseconds">The optional milliseconds.</param>
    /// <returns>The time span.</returns>
    /// <exception cref="HttpRemoteTransportException">The duration is negative.</exception>
    internal static TimeSpan? ToTimeSpan(long? milliseconds)
    {
        if (!milliseconds.HasValue)
        {
            return null;
        }

        if (milliseconds.Value < 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        return TimeSpan.FromMilliseconds(milliseconds.Value);
    }

    /// <summary>Converts an operation result DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The operation result.</returns>
    internal static OperationSyncResult ToOperationResult(HttpProtocolJsonContext.OperationSyncResultWire dto) =>
        new(new(dto.OperationId), (OperationResultKind)dto.Kind, dto.ReasonCode, dto.ServerVersion);

    /// <summary>Converts a completion DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The completion.</returns>
    internal static RemoteOperationCompletion ToCompletion(HttpProtocolJsonContext.RemoteOperationCompletionWire dto) =>
        new(ToOrigin(dto.Origin), dto.EventIds);

    /// <summary>Converts an origin DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The origin.</returns>
    internal static RemoteEventOrigin ToOrigin(HttpProtocolJsonContext.RemoteEventOriginWire dto) =>
        new(dto.ClientId, new(dto.OperationId));

    /// <summary>Copies a metadata dictionary.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <returns>The copied dictionary.</returns>
    internal static Dictionary<string, string> ToDictionary(IReadOnlyDictionary<string, string> metadata)
    {
        Dictionary<string, string> copy = [with(capacity: metadata.Count, comparer: StringComparer.Ordinal)];
        foreach (var pair in metadata)
        {
            copy.Add(pair.Key, pair.Value);
        }

        return copy;
    }

    /// <summary>Estimates the escaped UTF-8 byte count for one JSON string value.</summary>
    /// <param name="value">The string value.</param>
    /// <returns>The conservative encoded byte estimate.</returns>
    internal static long EstimateJsonStringBytes(string value) => JsonStringQuoteBytes + (Encoding.UTF8.GetByteCount(value) * JsonTextEscapeExpansion);

    /// <summary>Estimates the escaped UTF-8 byte count for an optional JSON string value.</summary>
    /// <param name="value">The optional string value.</param>
    /// <returns>The conservative encoded byte estimate.</returns>
    internal static long EstimateOptionalJsonStringBytes(string? value) =>
        value is null ? JsonNullTokenBytes : EstimateJsonStringBytes(value);
}
