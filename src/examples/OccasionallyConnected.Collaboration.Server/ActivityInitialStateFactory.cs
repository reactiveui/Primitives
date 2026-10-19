// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Creates the initial state for the custom activity stream.</summary>
[System.Diagnostics.DebuggerDisplay("Activity initial state")]
public sealed class ActivityInitialStateFactory : IServerInitialStateFactory
{
    /// <summary>The initial activity stream version.</summary>
    private const string InitialVersion = "activity-v0";

    /// <inheritdoc/>
    public ValueTask<ServerState> CreateInitialStateAsync(StreamId streamId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new ServerState(streamId, InitialVersion, ActivityPayloads.CreateInitialState()));
    }
}
