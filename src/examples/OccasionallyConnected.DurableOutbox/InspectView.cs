// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>The durable state view printed by the inspect command.</summary>
internal enum InspectView
{
    /// <summary>Prints subscription, pending work, and snapshot state.</summary>
    Status = 0,

    /// <summary>Prints pending work only.</summary>
    Pending = 1,

    /// <summary>Prints snapshot state only.</summary>
    Snapshot = 2,

    /// <summary>Prints the durable subscription identity only.</summary>
    Subscription = 3,
}
