// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores the durable read anchor resolved for a subscription's initial position.</summary>
/// <param name="Cursor">The cursor to use internally before the first client cursor.</param>
/// <param name="GroupSequence">The complete group sequence represented by <paramref name="Cursor"/>.</param>
/// <param name="IsResolved">Whether the initial position has a resolved retained anchor.</param>
internal readonly record struct ServerSubscriptionInitialAnchor(string? Cursor, long GroupSequence, bool IsResolved);
