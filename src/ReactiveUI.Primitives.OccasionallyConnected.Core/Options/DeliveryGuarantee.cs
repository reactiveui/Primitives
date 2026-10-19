// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the delivery and retry contract for remote work.</summary>
public enum DeliveryGuarantee
{
    /// <summary>Sends once and treats an ambiguous outcome as terminal.</summary>
    AtMostOnce = 0,

    /// <summary>Retries retained work until a terminal acknowledgement is recorded.</summary>
    AtLeastOnce = 1,

    /// <summary>Requires capability-gated exactly-once effect within the negotiated retention window.</summary>
    ExactlyOnce = 2,
}
