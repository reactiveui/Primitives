// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a logical remote subscription that can resume across reconnects and restarts.</summary>
/// <param name="Value">The stable subscription identifier value.</param>
[DebuggerDisplay("{Value,nq}")]
public readonly record struct SubscriptionId(Guid Value)
{
    /// <summary>Creates a new non-empty subscription identifier.</summary>
    /// <returns>A new subscription identifier.</returns>
    public static SubscriptionId New() => new(Guid.NewGuid());
}
