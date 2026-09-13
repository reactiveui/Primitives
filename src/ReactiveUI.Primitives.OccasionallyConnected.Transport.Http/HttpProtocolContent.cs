// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Reads and writes bounded HTTP protocol content.</summary>
internal static class HttpProtocolContent
{
    /// <summary>The HTTP protocol media type.</summary>
    internal const string MediaType = "application/vnd.reactiveui.occasionally-connected+json;v=1";

    /// <summary>The stream read buffer size.</summary>
    private const int ReadBufferSize = 8192;

    /// <summary>The protocol media type without parameters.</summary>
    private const string ProtocolMediaType = "application/vnd.reactiveui.occasionally-connected+json";

    /// <summary>Reads HTTP response content up to the configured byte limit.</summary>
    /// <param name="response">The HTTP response.</param>
    /// <param name="options">The adapter options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The response bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The response exceeds configured bounds.</exception>
    internal static async ValueTask<byte[]> ReadBoundedBytesAsync(
        HttpResponseMessage response,
        HttpRemoteTransportOptions options,
        CancellationToken cancellationToken)
    {
        ValidateMediaType(response);
        var length = response.Content.Headers.ContentLength;
        if (length.GetValueOrDefault() > options.MaximumResponseBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, response.StatusCode);
        }

#if NET5_0_OR_GREATER
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
#else
        using var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
#endif
#if NET5_0_OR_GREATER
        await using MemoryStream buffer = new();
#else
        using MemoryStream buffer = new();
#endif
        var bytes = new byte[ReadBufferSize];
        while (true)
        {
            var remaining = options.MaximumResponseBytes - checked((int)buffer.Length);
            if (remaining <= 0)
            {
                var probe = await ReadAsync(stream, bytes, 0, 1, cancellationToken).ConfigureAwait(false);
                if (probe == 0)
                {
                    return buffer.ToArray();
                }

                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, response.StatusCode);
            }

            var readSize = Math.Min(bytes.Length, remaining);
            var read = await ReadAsync(stream, bytes, 0, readSize, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                return buffer.ToArray();
            }

            buffer.Write(bytes, 0, read);
        }
    }

    /// <summary>Validates the protocol media type on a successful response with a body.</summary>
    /// <param name="response">The HTTP response.</param>
    /// <exception cref="HttpRemoteTransportException">The response media type is missing or incompatible.</exception>
    private static void ValidateMediaType(HttpResponseMessage response)
    {
        var contentType = response.Content.Headers.ContentType;
        if (contentType is null
            || !string.Equals(contentType.MediaType, ProtocolMediaType, StringComparison.OrdinalIgnoreCase))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible, response.StatusCode);
        }

        var versionCount = 0;
        string? version = null;
        foreach (var parameter in contentType.Parameters)
        {
            if (!string.Equals(parameter.Name, "v", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            versionCount++;
            version = parameter.Value?.Trim('"');
        }

        if (versionCount == 1 && string.Equals(version, "1", StringComparison.Ordinal))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible, response.StatusCode);
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
}
