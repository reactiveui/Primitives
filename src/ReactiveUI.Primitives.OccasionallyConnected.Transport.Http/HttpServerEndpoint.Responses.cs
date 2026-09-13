// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Creates a successful protocol response with a JSON body.</summary>
    /// <param name="body">The protocol body bytes.</param>
    /// <returns>The caller-owned response.</returns>
    private static HttpResponseMessage CreateProtocolResponse(byte[] body)
    {
        var response = CreateResponse(HttpStatusCode.OK);
        response.Content = new ByteArrayContent(body);
        response.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(HttpProtocolContent.MediaType);
        return response;
    }

    /// <summary>Creates a bodyless response.</summary>
    /// <param name="statusCode">The status code.</param>
    /// <returns>The caller-owned response.</returns>
    private static HttpResponseMessage CreateResponse(HttpStatusCode statusCode) => new(statusCode);

    /// <summary>Creates an error response from a transport failure.</summary>
    /// <param name="exception">The transport failure.</param>
    /// <param name="effectsPossible">Whether hub-side effects may already have happened.</param>
    /// <returns>The caller-owned error response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateErrorResponse(HttpRemoteTransportException exception, bool effectsPossible) =>
        CreateResponse(MapTransportStatus(exception.Kind, effectsPossible));

    /// <summary>Maps a transport failure to an HTTP status code.</summary>
    /// <param name="kind">The transport failure kind.</param>
    /// <param name="effectsPossible">Whether hub-side effects may already have happened.</param>
    /// <returns>The HTTP status code.</returns>
    private static HttpStatusCode MapTransportStatus(HttpTransportFailureKind kind, bool effectsPossible) => kind switch
    {
        HttpTransportFailureKind.Authentication => HttpStatusCode.Unauthorized,
        HttpTransportFailureKind.AuthorizationDenied => HttpStatusCode.Forbidden,
        HttpTransportFailureKind.PayloadTooLarge when !effectsPossible => PayloadTooLarge,
        HttpTransportFailureKind.SchemaIncompatible when !effectsPossible => HttpStatusCode.UnsupportedMediaType,
        HttpTransportFailureKind.ProtocolViolation when !effectsPossible => HttpStatusCode.BadRequest,
        HttpTransportFailureKind.ValidationRejected when !effectsPossible => HttpStatusCode.BadRequest,
        HttpTransportFailureKind.Transient when !effectsPossible => TooManyRequests,
        HttpTransportFailureKind.Transient => ServiceUnavailable,
        HttpTransportFailureKind.AmbiguousTransportOutcome => HttpStatusCode.InternalServerError,
        _ => effectsPossible ? HttpStatusCode.InternalServerError : HttpStatusCode.BadRequest,
    };

    /// <summary>Creates a retryable error response after hub invocation.</summary>
    /// <returns>The caller-owned error response.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpResponseMessage CreateAmbiguousResponse() => CreateResponse(HttpStatusCode.InternalServerError);

    /// <summary>Checks whether a request has a body.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <returns><see langword="true"/> when a body is attached.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasBody(HttpRequestMessage request) => request.Content is not null;

    /// <summary>Validates protocol request content headers.</summary>
    /// <param name="content">The request content.</param>
    /// <exception cref="HttpRemoteTransportException">The content media type is missing, incompatible, compressed, or oversized.</exception>
    private static void ValidateProtocolContentHeaders(HttpContent content)
    {
        if (content.Headers.ContentLength.GetValueOrDefault() > int.MaxValue)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        if (content.Headers.ContentEncoding.Count > 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible);
        }

        ValidateMediaType(content.Headers.ContentType);
    }

    /// <summary>Validates the protocol media type.</summary>
    /// <param name="contentType">The parsed media type.</param>
    /// <exception cref="HttpRemoteTransportException">The media type is missing or incompatible.</exception>
    private static void ValidateMediaType(MediaTypeHeaderValue? contentType)
    {
        if (contentType is null
            || !string.Equals(contentType.MediaType, ProtocolMediaType, StringComparison.OrdinalIgnoreCase))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible);
        }

        var versionCount = 0;
        string? version = null;
        foreach (var parameter in contentType.Parameters)
        {
            if (!string.Equals(parameter.Name, ProtocolVersionParameterName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            versionCount++;
            version = parameter.Value?.Trim('"');
        }

        if (versionCount == SupportedProtocolMajor && string.Equals(version, ProtocolVersionParameterValue, StringComparison.Ordinal))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Reads from a stream with cancellation.</summary>
    /// <param name="stream">The source stream.</param>
    /// <param name="buffer">The destination buffer.</param>
    /// <param name="offset">The buffer offset.</param>
    /// <param name="count">The requested count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The number of bytes read.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<int> ReadAsync(Stream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        stream.ReadAsync(buffer, offset, count, cancellationToken);

    /// <summary>Gets the query from a relative request URI.</summary>
    /// <param name="requestTarget">The relative request target.</param>
    /// <returns>The query including the leading separator, or an empty string.</returns>
    private static string GetRelativeQuery(string requestTarget)
    {
        var queryIndex = requestTarget.IndexOf('?');
        if (queryIndex < 0)
        {
            return string.Empty;
        }

        var fragmentIndex = requestTarget.IndexOf('#', queryIndex);
        return fragmentIndex < 0
            ? requestTarget.Remove(0, queryIndex)
            : requestTarget.Remove(fragmentIndex).Remove(0, queryIndex);
    }

    /// <summary>Preserves the first cleanup failure while still attempting all cleanup work.</summary>
    /// <param name="current">The currently preserved failure.</param>
    /// <param name="next">The next cleanup failure.</param>
    /// <returns>The preserved failure.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Exception? PreserveFailure(Exception? current, Exception? next) => current ?? next;
}
