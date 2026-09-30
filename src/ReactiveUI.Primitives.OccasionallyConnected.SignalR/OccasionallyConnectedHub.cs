// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Hosts the synchronization protocol over SignalR with invocation cancellation.</summary>
[System.Diagnostics.DebuggerDisplay("{Context.ConnectionId}")]
public sealed class OccasionallyConnectedHub : Hub
{
    /// <summary>The borrowed endpoint shared across transient hub instances.</summary>
    private readonly SignalRServerEndpoint _endpoint;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedHub"/> class.</summary>
    /// <param name="endpoint">The server endpoint registered by the host.</param>
    public OccasionallyConnectedHub(SignalRServerEndpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        _endpoint = endpoint;
    }

    /// <summary>Returns one bounded protocol response. Streaming invocation lets SignalR propagate caller cancellation.</summary>
    /// <param name="request">The source-generated protocol carrier.</param>
    /// <param name="cancellationToken">The invocation cancellation token.</param>
    /// <returns>The single response.</returns>
    public async IAsyncEnumerable<byte[]> Exchange(
        byte[] request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Context.ConnectionAborted);
        yield return await _endpoint.ExchangeAsync(request, Context.User, linked.Token).ConfigureAwait(false);
    }
}
