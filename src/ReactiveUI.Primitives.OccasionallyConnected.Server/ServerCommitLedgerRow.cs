// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores one retained terminal ledger row.</summary>
/// <param name="StreamKey">The containing stream key.</param>
/// <param name="Entry">The terminal ledger entry.</param>
/// <param name="LogicalBytes">The retained logical bytes.</param>
internal sealed record ServerCommitLedgerRow(ServerStreamKey StreamKey, ServerLedgerEntry Entry, long LogicalBytes);
