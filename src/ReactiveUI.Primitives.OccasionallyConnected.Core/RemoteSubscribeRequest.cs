// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a remote subscription request.</summary>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="SubscriptionId">The durable subscription identifier.</param>
/// <param name="Cursor">The optional resume cursor.</param>
/// <param name="InitialPosition">The initial position used when no durable cursor exists.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed record RemoteSubscribeRequest(
    StreamId StreamId,
    SubscriptionId SubscriptionId,
    string? Cursor,
    StartPosition InitialPosition);
