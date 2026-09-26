// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Materializes the captured CRDT server state as the client snapshot state.</summary>
[System.Diagnostics.DebuggerDisplay("CRDT snapshot materializer")]
internal sealed class ResilienceLabSnapshotMaterializer : IServerSnapshotMaterializer
{
    /// <inheritdoc/>
    public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
        ServerSnapshotMaterializationContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        var state = CrdtCodec.DecodeState(context.CapturedServerState.State.Payload, ResilienceLabLoopback.Bounds);
        return ValueTask.FromResult(new ServerSnapshotMaterializationResult
        {
            Status = ServerSnapshotMaterializationStatus.Materialized,
            ClientState = CrdtServerPayloads.CreateState(state, ResilienceLabLoopback.Bounds),
        });
    }
}
