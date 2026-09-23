// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes durable receive metadata prepared by a stream participant.</summary>
/// <param name="StreamId">The subscribed stream identity.</param>
/// <param name="SubscriptionId">The durable subscription identity.</param>
/// <param name="Cursor">The durable server cursor to resume from, if any.</param>
/// <param name="InitialPosition">The initial start position used when no durable cursor exists.</param>
/// <param name="DeliveryGuarantee">The durable delivery guarantee required by the stream.</param>
internal sealed record ReceiveStreamSubscription(
    StreamId StreamId,
    SubscriptionId SubscriptionId,
    string? Cursor,
    StartPosition InitialPosition,
    DeliveryGuarantee DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce);
