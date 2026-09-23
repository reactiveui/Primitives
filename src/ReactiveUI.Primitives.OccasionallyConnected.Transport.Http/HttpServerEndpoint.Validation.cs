// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Validates the immutable option set.</summary>
    /// <param name="options">The endpoint options.</param>
    private static void ValidateOptions(HttpServerEndpointOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options.Hub);
        ArgumentExceptionHelper.ThrowIfNull(options.DeclaredCapabilities);
        ArgumentExceptionHelper.ThrowIfNull(options.ReplayAuthorizer);
        ArgumentExceptionHelper.ThrowIfNull(options.ReplayProtection);
        ArgumentExceptionHelper.ThrowIfNull(options.SnapshotRecoveryLimits);
        ArgumentExceptionHelper.ThrowIfNull(options.TimeProvider);
        ValidatePositive(options.MaximumConcurrentRequests, nameof(options.MaximumConcurrentRequests));
        ValidatePositive(options.MaximumConcurrentAcknowledgements, nameof(options.MaximumConcurrentAcknowledgements));
        ValidatePositive(options.MaximumConcurrentSubscriptions, nameof(options.MaximumConcurrentSubscriptions));
        ValidatePositive(options.MaximumRequestBytes, nameof(options.MaximumRequestBytes));
        ValidatePositive(options.MaximumResponseBytes, nameof(options.MaximumResponseBytes));
        ValidatePositive(options.MaximumPayloadBytes, nameof(options.MaximumPayloadBytes));
        ValidatePositive(options.MaximumMetadataEntries, nameof(options.MaximumMetadataEntries));
        ValidatePositive(options.MaximumMetadataKeyBytes, nameof(options.MaximumMetadataKeyBytes));
        ValidatePositive(options.MaximumMetadataValueBytes, nameof(options.MaximumMetadataValueBytes));
        ValidatePositive(options.MaximumBatchOperations, nameof(options.MaximumBatchOperations));
        ValidatePositive(options.MaximumEventsPerBatch, nameof(options.MaximumEventsPerBatch));
        ValidatePositive(options.MaximumCompletedOperationsPerBatch, nameof(options.MaximumCompletedOperationsPerBatch));
        ValidatePositive(options.MaximumJsonDepth, nameof(options.MaximumJsonDepth));
        ValidatePositive(options.MaximumQueryBytes, nameof(options.MaximumQueryBytes));
        ValidatePositive(options.MaximumQueryKeys, nameof(options.MaximumQueryKeys));
        ValidatePositive(options.MaximumProtocolStringBytes, nameof(options.MaximumProtocolStringBytes));
        ValidateTimeout(options.LongPollTimeout);
        options.ReplayProtection.Validate();
        options.SnapshotRecoveryLimits.Validate();
        ValidateCapabilities(options.DeclaredCapabilities, options.SnapshotRecoveryHub is not null);
        ValidateRoutes(options);
    }

    /// <summary>Validates endpoint capabilities against the portable HTTP guarantees.</summary>
    /// <param name="capabilities">The declared capabilities.</param>
    /// <param name="hasSnapshotRecoveryHub">Whether the endpoint has a recovery hub for the optional feature.</param>
    /// <exception cref="ArgumentException">The declared capabilities exceed the endpoint guarantees.</exception>
    private static void ValidateCapabilities(NegotiatedCapabilities capabilities, bool hasSnapshotRecoveryHub)
    {
        if ((capabilities.Features & ~SupportedCapabilities) != 0
            || capabilities.MaximumBatchOperations <= 0
            || capabilities.MaximumBatchBytes <= 0
            || capabilities.ProtocolVersion.Major != SupportedProtocolMajor)
        {
            throw new ArgumentException("The HTTP server endpoint capabilities are not supported.", nameof(capabilities));
        }

        if ((capabilities.Features & RemoteTransportCapabilities.SnapshotRecovery) != 0 && !hasSnapshotRecoveryHub)
        {
            throw new ArgumentException("Snapshot recovery capabilities require a snapshot recovery hub.", nameof(capabilities));
        }

        if (!capabilities.EffectiveExactlyOnceWindow.HasValue)
        {
            return;
        }

        if ((capabilities.Features & RemoteTransportCapabilities.AtomicApplyAndAcknowledge) != 0)
        {
            return;
        }

        throw new ArgumentException("Exactly-once effect requires atomic apply and acknowledge support.", nameof(capabilities));
    }

    /// <summary>Validates all configured routes are relative and distinct after normalization.</summary>
    /// <param name="options">The endpoint options.</param>
    private static void ValidateRoutes(HttpServerEndpointOptions options)
    {
        _ = NormalizePathBase(options.PathBase);
        var routes = new HashSet<string>(StringComparer.Ordinal);
        AddRoute(routes, options.ConnectPath, nameof(options.ConnectPath));
        AddRoute(routes, options.PushPath, nameof(options.PushPath));
        AddRoute(routes, options.SubscribePath, nameof(options.SubscribePath));
        AddRoute(routes, options.AcknowledgePath, nameof(options.AcknowledgePath));
        AddRoute(routes, options.SnapshotRecoveryPath, nameof(options.SnapshotRecoveryPath));
    }

    /// <summary>Adds a normalized route and rejects duplicates.</summary>
    /// <param name="routes">The route set.</param>
    /// <param name="path">The configured path.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The route is invalid or duplicates another route.</exception>
    private static void AddRoute(HashSet<string> routes, string path, string parameterName)
    {
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(path, parameterName);
        if (routes.Add(NormalizeRelativePath(path)))
        {
            return;
        }

        throw new ArgumentException("HTTP server endpoint routes must be distinct.", parameterName);
    }

    /// <summary>Creates the endpoint protocol codec.</summary>
    /// <param name="options">The endpoint options.</param>
    /// <returns>The configured protocol codec.</returns>
    private static HttpProtocolCodec CreateCodec(HttpServerEndpointOptions options) =>
        new(
            new HttpProtocolLimits
            {
                MaximumRequestBytes = options.MaximumRequestBytes,
                MaximumResponseBytes = options.MaximumResponseBytes,
                MaximumPayloadBytes = options.MaximumPayloadBytes,
                MaximumMetadataEntries = options.MaximumMetadataEntries,
                MaximumMetadataKeyBytes = options.MaximumMetadataKeyBytes,
                MaximumMetadataValueBytes = options.MaximumMetadataValueBytes,
                MaximumBatchOperations = options.MaximumBatchOperations,
                MaximumEventsPerBatch = options.MaximumEventsPerBatch,
                MaximumCompletedOperationsPerBatch = options.MaximumCompletedOperationsPerBatch,
                MaximumJsonDepth = options.MaximumJsonDepth,
                MaximumQueryBytes = options.MaximumQueryBytes,
                MaximumQueryKeys = options.MaximumQueryKeys,
                MaximumProtocolStringBytes = options.MaximumProtocolStringBytes,
            },
            options.SnapshotRecoveryLimits);

    /// <summary>Creates the conservative capability response advertised to clients.</summary>
    /// <param name="options">The endpoint options.</param>
    /// <returns>The advertised capabilities.</returns>
    private static NegotiatedCapabilities CreateAdvertisedCapabilities(HttpServerEndpointOptions options)
    {
        var maximumBatchOperations = Math.Min(options.DeclaredCapabilities.MaximumBatchOperations, options.MaximumBatchOperations);
        var maximumBatchBytes = Math.Min(options.DeclaredCapabilities.MaximumBatchBytes, options.MaximumRequestBytes);
        var features = options.DeclaredCapabilities.Features;
        if (options.SnapshotRecoveryHub is null)
        {
            features &= ~RemoteTransportCapabilities.SnapshotRecovery;
        }

        return options.DeclaredCapabilities with { Features = features, MaximumBatchOperations = maximumBatchOperations, MaximumBatchBytes = maximumBatchBytes };
    }

    /// <summary>Validates a positive integer option.</summary>
    /// <param name="value">The option value.</param>
    /// <param name="parameterName">The parameter name.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidatePositive(int value, string parameterName) =>
        HttpRemoteTransportOptionsValidation.ValidatePositive(value, parameterName);

    /// <summary>Validates the finite long-poll timeout.</summary>
    /// <param name="timeout">The configured timeout.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is not finite and positive.</exception>
    private static void ValidateTimeout(TimeSpan timeout)
    {
        if (timeout > TimeSpan.Zero && timeout != TimeSpan.MaxValue)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "HTTP server endpoint long-poll timeout must be finite and positive.");
    }

    /// <summary>Combines a normalized path base and relative route.</summary>
    /// <param name="pathBase">The normalized path base.</param>
    /// <param name="path">The relative route.</param>
    /// <returns>The normalized route path.</returns>
    private static string Combine(string pathBase, string path)
    {
        var relative = NormalizeRelativePath(path);
        return pathBase.Length == 0 ? relative : $"{pathBase}/{relative}";
    }

    /// <summary>Normalizes an optional route base.</summary>
    /// <param name="pathBase">The configured route base.</param>
    /// <returns>The normalized path base.</returns>
    /// <exception cref="ArgumentException"><paramref name="pathBase"/> is an absolute URI.</exception>
    private static string NormalizePathBase(string pathBase)
    {
        if (string.IsNullOrWhiteSpace(pathBase))
        {
            return string.Empty;
        }

        var relative = NormalizeRelativePath(pathBase);
        if (Uri.TryCreate(relative, UriKind.Absolute, out _))
        {
            throw new ArgumentException("HTTP server endpoint path base must not be absolute.", nameof(pathBase));
        }

        return relative;
    }

    /// <summary>Normalizes a route by trimming separators.</summary>
    /// <param name="path">The configured path.</param>
    /// <returns>The normalized relative path.</returns>
    /// <exception cref="ArgumentException"><paramref name="path"/> is not a valid relative path.</exception>
    private static string NormalizeRelativePath(string path) =>
        TryNormalizeRequestPath(path, out var route)
            ? route
            : throw new ArgumentException("HTTP server endpoint routes must be valid percent-encoded relative paths.", nameof(path));

    /// <summary>Gets the route status for an actual and expected HTTP method.</summary>
    /// <param name="actual">The request method.</param>
    /// <param name="expected">The expected method.</param>
    /// <returns>The route response status.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpStatusCode GetMethodStatus(HttpMethod actual, HttpMethod expected) =>
        actual == expected ? HttpStatusCode.OK : HttpStatusCode.MethodNotAllowed;

    /// <summary>Checks whether the host-authenticated principal is usable by the endpoint.</summary>
    /// <param name="client">The authenticated principal.</param>
    /// <returns><see langword="true"/> when both trusted identifiers are present.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasTrustedPrincipal(ServerAuthenticatedClient client) =>
        !string.IsNullOrWhiteSpace(client.TenantId) && !string.IsNullOrWhiteSpace(client.ClientId);

    /// <summary>Gets a request route from the URI path.</summary>
    /// <param name="uri">The request URI.</param>
    /// <returns>The normalized route, or <see langword="null"/> when the route is malformed.</returns>
    private static string? TryGetRoute(Uri uri)
    {
        var rawPath = GetRawPath(uri);
        return TryNormalizeRequestPath(rawPath, out var route) ? route : null;
    }

    /// <summary>Gets the raw escaped path from a request URI.</summary>
    /// <param name="uri">The request URI.</param>
    /// <returns>The escaped path without query or fragment text.</returns>
    private static string GetRawPath(Uri uri)
    {
        if (uri.IsAbsoluteUri)
        {
            return uri.GetComponents(UriComponents.Path, UriFormat.UriEscaped);
        }

        var text = uri.OriginalString;
        var queryIndex = text.IndexOf('?');
        var fragmentIndex = text.IndexOf('#');
        var end = GetPathEnd(queryIndex, fragmentIndex, text.Length);
        return end == text.Length ? text : text.Remove(end);
    }

    /// <summary>Gets the end index for a relative request path.</summary>
    /// <param name="queryIndex">The query separator index.</param>
    /// <param name="fragmentIndex">The fragment separator index.</param>
    /// <param name="length">The original text length.</param>
    /// <returns>The exclusive path end index.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetPathEnd(int queryIndex, int fragmentIndex, int length)
    {
        if (queryIndex < 0)
        {
            return fragmentIndex < 0 ? length : fragmentIndex;
        }

        return fragmentIndex < 0 ? queryIndex : Math.Min(queryIndex, fragmentIndex);
    }

    /// <summary>Normalizes and validates the escaped request path.</summary>
    /// <param name="rawPath">The raw escaped path.</param>
    /// <param name="route">The normalized route.</param>
    /// <returns><see langword="true"/> when the path is valid.</returns>
    private static bool TryNormalizeRequestPath(string rawPath, out string route)
    {
        var segments = new List<string>(InitialRouteSegmentCapacity);
        var path = rawPath.Trim('/');
        route = string.Empty;
        if (path.Length == 0)
        {
            return true;
        }

        foreach (var rawSegment in path.Split('/'))
        {
            if (!TryDecodeRouteSegment(rawSegment, out var segment))
            {
                return false;
            }

            segments.Add(segment);
        }

        route = string.Join("/", segments);
        return true;
    }

    /// <summary>Decodes and validates one escaped route segment.</summary>
    /// <param name="rawSegment">The raw escaped segment.</param>
    /// <param name="segment">The decoded segment.</param>
    /// <returns><see langword="true"/> when the segment is route-safe.</returns>
    private static bool TryDecodeRouteSegment(string rawSegment, out string segment)
    {
        segment = string.Empty;
        if (!TryDecodePercentEncodedText(rawSegment, out var decoded))
        {
            return false;
        }

        if (decoded.Length == 0 || decoded is "." or ".." || ContainsRouteSeparator(decoded))
        {
            return false;
        }

        segment = decoded;
        return true;
    }

    /// <summary>Determines whether a decoded route segment contains a route separator.</summary>
    /// <param name="value">The decoded route segment.</param>
    /// <returns><see langword="true"/> when the route segment contains a separator.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ContainsRouteSeparator(string value)
    {
#if NET8_0_OR_GREATER
        return value.Contains('/') || value.Contains('\\');
#else
        return value.Contains("/") || value.Contains("\\");
#endif
    }

    /// <summary>Decodes percent-encoded route text using strict UTF-8.</summary>
    /// <param name="value">The escaped text.</param>
    /// <param name="decoded">The decoded text.</param>
    /// <returns><see langword="true"/> when decoding succeeds.</returns>
    private static bool TryDecodePercentEncodedText(string value, out string decoded)
    {
        decoded = string.Empty;
        using MemoryStream buffer = new();
        var index = 0;
        try
        {
            while (index < value.Length)
            {
                if (value[index] == '%')
                {
                    if (!TryWritePercentByte(value, ref index, buffer))
                    {
                        return false;
                    }

                    continue;
                }

                var start = index;
                while (index < value.Length && value[index] != '%')
                {
                    index++;
                }

                var bytes = StrictUtf8.GetBytes(value.Substring(start, index - start));
                buffer.Write(bytes, 0, bytes.Length);
            }

            decoded = StrictUtf8.GetString(buffer.ToArray());
            return true;
        }
        catch (EncoderFallbackException)
        {
            return false;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }

    /// <summary>Writes one percent-encoded byte to a buffer.</summary>
    /// <param name="value">The escaped text.</param>
    /// <param name="index">The percent index.</param>
    /// <param name="buffer">The destination buffer.</param>
    /// <returns><see langword="true"/> when a byte was written.</returns>
    private static bool TryWritePercentByte(string value, ref int index, Stream buffer)
    {
        if (index + PercentEncodedLowOffset >= value.Length)
        {
            return false;
        }

        var high = value[index + PercentEncodedHighOffset];
        var low = value[index + PercentEncodedLowOffset];
        if (!Uri.IsHexDigit(high) || !Uri.IsHexDigit(low))
        {
            return false;
        }

        buffer.WriteByte((byte)((Uri.FromHex(high) << HexHighNibbleShift) + Uri.FromHex(low)));
        index += PercentEncodedWidth;
        return true;
    }
}
