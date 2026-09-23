// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Authorizes HTTP replay admission after the transport host has authenticated the request principal.</summary>
public interface IHttpReplayAuthorizer
{
    /// <summary>Authorizes a fresh or duplicate replay request before endpoint effects or cache replay.</summary>
    /// <param name="context">The replay authorization context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when replay admission is allowed; otherwise, <see langword="false"/>.</returns>
    ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken);
}
