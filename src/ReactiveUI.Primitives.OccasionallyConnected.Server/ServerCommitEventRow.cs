// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores one retained sidecar event row.</summary>
/// <param name="Ledger">The containing terminal ledger row.</param>
/// <param name="RemoteEvent">The remote event.</param>
/// <param name="Sequence">The increasing sidecar sequence.</param>
internal sealed record ServerCommitEventRow(ServerCommitLedgerRow Ledger, RemoteEvent RemoteEvent, long Sequence);
