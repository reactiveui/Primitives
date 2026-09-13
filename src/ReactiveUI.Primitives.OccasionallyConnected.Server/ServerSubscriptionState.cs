// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Reports persisted subscription acknowledgement state.</summary>
/// <param name="Identity">The trusted subscription identity.</param>
/// <param name="LatestOfferedCursor">The latest offered complete cursor.</param>
/// <param name="LatestOfferedGroupSequence">The latest offered complete group sequence.</param>
/// <param name="AcknowledgedCursor">The acknowledged cursor.</param>
/// <param name="AcknowledgedGroupSequence">The acknowledged complete group sequence.</param>
/// <param name="OfferCount">The retained offered cursor count.</param>
internal sealed record ServerSubscriptionState(
    ServerSubscriptionIdentity Identity,
    string? LatestOfferedCursor,
    long LatestOfferedGroupSequence,
    string? AcknowledgedCursor,
    long AcknowledgedGroupSequence,
    int OfferCount);
