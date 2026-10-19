// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how exactly-once work behaves when the negotiated deduplication window expires.</summary>
public enum ExactlyOnceExpiryBehavior
{
    /// <summary>Stops the operation and reports guarantee expiration.</summary>
    StopAndReport = 0,

    /// <summary>Allows explicit fallback to at-least-once processing.</summary>
    FallbackToAtLeastOnce = 1,
}
