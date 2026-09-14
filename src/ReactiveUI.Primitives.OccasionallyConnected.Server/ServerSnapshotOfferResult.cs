// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Reports the durable result of offering a recovered snapshot cursor.</summary>
internal sealed record ServerSnapshotOfferResult
{
    /// <summary>Gets the durable offer status.</summary>
    internal required ServerSnapshotOfferStatus Status { get; init; }

    /// <summary>Gets the retained subscription state, or null when the binding is expired or missing.</summary>
    internal required ServerSubscriptionState? SubscriptionState { get; init; }

    /// <summary>Gets the recovered snapshot cursor when an offer was issued or replayed.</summary>
    internal required string? Cursor { get; init; }
}
