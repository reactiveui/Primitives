// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Drops one acknowledgement after the real server protocol and journal have committed it.</summary>
public sealed class LostResponseHub : Hub
{
    /// <summary>The real protocol endpoint.</summary>
    private readonly SignalRServerEndpoint _endpoint;

    /// <summary>The fault state shared across connections.</summary>
    private readonly LostResponseState _state;

    /// <summary>Initializes a new instance of the <see cref="LostResponseHub"/> class.</summary>
    /// <param name="endpoint">The real endpoint.</param>
    /// <param name="services">The host services.</param>
    public LostResponseHub(SignalRServerEndpoint endpoint, IServiceProvider services)
    {
        _endpoint = endpoint;
        _state = (LostResponseState)services.GetService(typeof(LostResponseState))!;
    }

    /// <summary>Runs real protocol work then disconnects instead of sending one committed push ACK.</summary>
    /// <param name="request">The carrier.</param>
    /// <param name="cancellationToken">The canceled invocation.</param>
    /// <returns>The response, unless deliberately lost.</returns>
    /// <exception cref="HubException">The test deliberately injects a peer failure.</exception>
    public async IAsyncEnumerable<byte[]> Exchange(byte[] request, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (_state.PeerFailure == SignalRPeerFailure.EmptyResponse)
        {
            yield break;
        }

        if (_state.PeerFailure == SignalRPeerFailure.HubFailure)
        {
            throw new HubException("peer failure");
        }

        var response = await _endpoint.ExchangeAsync(request, Context.User, cancellationToken);
        if (_state.DropPushResponse && SignalRCarrier.Decode(request).PathAndQuery == "/push")
        {
            _state.DropPushResponse = false;
            Context.Abort();
            yield break;
        }

        yield return _state.PeerFailure switch
        {
            SignalRPeerFailure.MalformedBody => "{"u8.ToArray(),
            SignalRPeerFailure.InvalidStatus => SignalRCarrier.Encode(new(null, null, 0, [], [])),
            SignalRPeerFailure.RequestAsResponse => request,
            _ => response,
        };
    }
}
