// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides internal snapshot-recovery views and durable snapshot cursor offers.</summary>
internal interface IServerSnapshotRecoveryJournal
{
    /// <summary>Reads a trusted bounded view used to evaluate a snapshot recovery request.</summary>
    /// <param name="request">The read request.</param>
    /// <returns>The retained snapshot-recovery view.</returns>
    ServerSnapshotRecoveryView ReadSnapshotRecoveryView(ServerSnapshotRecoveryReadRequest request);

    /// <summary>Durably offers a recovered snapshot cursor for later authenticated acknowledgement.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>The offer result.</returns>
    ServerSnapshotOfferResult TryOfferSnapshot(ServerSnapshotOfferRequest request);
}
