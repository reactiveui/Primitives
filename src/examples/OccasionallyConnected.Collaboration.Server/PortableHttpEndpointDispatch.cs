// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Dispatches a portable HTTP request to an occasionally-connected endpoint.</summary>
/// <param name="request">The portable request.</param>
/// <param name="authenticatedClient">The host-authenticated identity.</param>
/// <param name="cancellationToken">The request cancellation token.</param>
/// <returns>The portable response.</returns>
public delegate ValueTask<HttpResponseMessage> PortableHttpEndpointDispatch(
    HttpRequestMessage request,
    ServerAuthenticatedClient authenticatedClient,
    CancellationToken cancellationToken);
