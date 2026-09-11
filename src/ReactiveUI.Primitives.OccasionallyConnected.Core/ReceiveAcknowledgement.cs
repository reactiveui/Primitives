// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes acknowledgement of a durably applied receive cursor.</summary>
/// <param name="SubscriptionId">The durable subscription identifier.</param>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="Cursor">The durably applied cursor.</param>
[System.Diagnostics.DebuggerDisplay("{SubscriptionId,nq} {Cursor,nq}")]
public sealed record ReceiveAcknowledgement(
    SubscriptionId SubscriptionId,
    StreamId StreamId,
    string Cursor);
