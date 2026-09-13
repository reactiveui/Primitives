// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Parses and validates a subscribe query from a request URI.</summary>
    /// <param name="uri">The request URI.</param>
    /// <returns>The subscribe request.</returns>
    /// <exception cref="HttpRemoteTransportException">The query is malformed or oversized.</exception>
    private RemoteSubscribeRequest ParseSubscribeRequest(Uri uri)
    {
        var query = uri.IsAbsoluteUri ? uri.Query : GetRelativeQuery(uri.OriginalString);
        if (StrictUtf8.GetByteCount(query) > _maximumQueryBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        return _codec.ParseSubscribeRequest(query);
    }

    /// <summary>Reads at most one complete batch from the borrowed hub.</summary>
    /// <param name="request">The subscription request.</param>
    /// <param name="authenticatedClient">The host-authenticated client principal.</param>
    /// <param name="cancellationToken">The endpoint-owned poll token.</param>
    /// <returns>The batch list to encode.</returns>
    private async ValueTask<IReadOnlyList<RemoteEventBatch>> ReadOneBatchAsync(
        RemoteSubscribeRequest request,
        ServerAuthenticatedClient authenticatedClient,
        CancellationToken cancellationToken)
    {
        var batches = _hub.SubscribeStreamAsync(request, authenticatedClient, cancellationToken);
        await using var enumerator = batches.GetAsyncEnumerator(cancellationToken);
        return await enumerator.MoveNextAsync().ConfigureAwait(false)
            ? [enumerator.Current]
            : [];
    }
}
