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

    /// <summary>The largest protocol millisecond duration representable by <see cref="TimeSpan"/>.</summary>
    private static readonly long MaximumTimeSpanMilliseconds = TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerMillisecond;

    /// <summary>Converts a protocol retention duration into a CLR time span.</summary>
    /// <param name="milliseconds">Retention duration encoded in milliseconds, or <see langword="null"/> when absent.</param>
    /// <returns>The decoded retention window, or <see langword="null"/> when absent.</returns>
    /// <exception cref="HttpRemoteTransportException">The encoded duration is negative or too large for <see cref="TimeSpan"/>.</exception>
    internal static TimeSpan? ToTimeSpan(long? milliseconds)
    {
        if (!milliseconds.HasValue)
        {
            return null;
        }

        if (milliseconds.Value < 0 || milliseconds.Value > MaximumTimeSpanMilliseconds)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        return TimeSpan.FromTicks(checked(milliseconds.Value * TimeSpan.TicksPerMillisecond));
    }

    /// <summary>Converts an operation result DTO.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>Domain result for one pushed operation.</returns>
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

    /// <summary>Estimates the escaped UTF-8 byte count for one protocol JSON string.</summary>
    /// <param name="value">Protocol text written as a JSON string.</param>
    /// <returns>The conservative encoded byte estimate.</returns>
    internal static long EstimateJsonStringBytes(string value) => JsonStringQuoteBytes + (Encoding.UTF8.GetByteCount(value) * JsonTextEscapeExpansion);

    /// <summary>Estimates the escaped UTF-8 byte count for an optional protocol JSON string.</summary>
    /// <param name="value">Protocol text written as a JSON string when present.</param>
    /// <returns>The conservative encoded byte estimate.</returns>
    internal static long EstimateOptionalJsonStringBytes(string? value) =>
        value is null ? JsonNullTokenBytes : EstimateJsonStringBytes(value);
}
