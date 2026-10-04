// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Materializes an allowlisted client snapshot from a captured canonical server state.</summary>
public interface IServerSnapshotMaterializer
{
    /// <summary>Builds the requested bounded client state from trusted snapshot recovery context.</summary>
    /// <param name="context">The trusted captured recovery context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The materialization result.</returns>
    ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
        ServerSnapshotMaterializationContext context,
        CancellationToken cancellationToken);
}
