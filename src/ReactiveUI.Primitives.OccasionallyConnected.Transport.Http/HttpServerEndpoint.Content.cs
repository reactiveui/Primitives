// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Reads required bounded request body bytes.</summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="cancellationToken">The request cancellation token.</param>
    /// <returns>The bounded request bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The body is missing, malformed, compressed, or oversized.</exception>
    private async ValueTask<byte[]> ReadRequiredBodyAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.Content is null)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }

        ValidateProtocolContentHeaders(request.Content);
        var length = request.Content.Headers.ContentLength;
        if (length.GetValueOrDefault() > _maximumRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

#if NET5_0_OR_GREATER
        await using var stream = await request.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
#else
        using var stream = await request.Content.ReadAsStreamAsync().ConfigureAwait(false);
#endif
#if NET5_0_OR_GREATER
        await using MemoryStream buffer = new();
#else
        using MemoryStream buffer = new();
#endif
        var bytes = new byte[ReadBufferSize];
        while (true)
        {
            var remaining = _maximumRequestBytes - checked((int)buffer.Length);
            if (remaining <= 0)
            {
                var probe = await ReadAsync(stream, bytes, 0, SupportedProtocolMajor, cancellationToken).ConfigureAwait(false);
                if (probe == 0)
                {
                    return buffer.ToArray();
                }

                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            var readSize = Math.Min(bytes.Length, remaining);
            var read = await ReadAsync(stream, bytes, 0, readSize, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                return buffer.ToArray();
            }

#if NET5_0_OR_GREATER
            await buffer.WriteAsync(bytes.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
#else
            await buffer.WriteAsync(bytes, 0, read, cancellationToken).ConfigureAwait(false);
#endif
        }
    }
}
