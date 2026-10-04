// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates the initial canonical state for an empty server stream.</summary>
public interface IServerInitialStateFactory
{
    /// <summary>Creates the initial canonical state.</summary>
    /// <param name="streamId">The stream that needs initial state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The initial server state.</returns>
    ValueTask<ServerState> CreateInitialStateAsync(
        StreamId streamId,
        CancellationToken cancellationToken);
}
