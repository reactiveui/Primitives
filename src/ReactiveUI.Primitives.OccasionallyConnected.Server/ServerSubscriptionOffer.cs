// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores one offered complete receive position for a subscription.</summary>
/// <param name="Cursor">The offered cursor.</param>
/// <param name="GroupSequence">The complete group sequence.</param>
/// <param name="OfferedAtUtc">The monotonic server time when the cursor was offered.</param>
/// <param name="LogicalBytes">The retained logical byte count.</param>
internal sealed record ServerSubscriptionOffer(
    string Cursor,
    long GroupSequence,
    DateTimeOffset OfferedAtUtc,
    long LogicalBytes);
