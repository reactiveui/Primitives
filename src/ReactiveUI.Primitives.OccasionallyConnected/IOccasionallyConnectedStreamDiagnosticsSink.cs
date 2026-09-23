// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Receives stream-specific synchronization diagnostics from the owning engine.</summary>
internal interface IOccasionallyConnectedStreamDiagnosticsSink
{
    /// <summary>Publishes one engine state with the stream's own pending queue aggregate.</summary>
    /// <param name="state">The stream state.</param>
    /// <param name="revision">The engine diagnostic revision used to reject stale concurrent notifications.</param>
    void PublishSyncState(SyncState state, long revision);
}
