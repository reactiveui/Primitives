// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Materializes canonical server state and event proposals after conflict resolution.</summary>
public interface IServerDomainHandler
{
    /// <summary>Applies an accepted conflict decision to the domain state.</summary>
    /// <param name="context">The domain apply context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The canonical state and event proposals.</returns>
    ValueTask<ServerDomainApplyResult> ApplyAsync(
        ServerDomainApplyContext context,
        CancellationToken cancellationToken);
}
