// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the current admission state of a circuit breaker.</summary>
public enum CircuitBreakerState
{
    /// <summary>Requests are admitted normally.</summary>
    Closed = 0,

    /// <summary>Requests are rejected until the configured delay expires.</summary>
    Open = 1,

    /// <summary>One recovery probe is in progress and all other requests are rejected.</summary>
    HalfOpen = 2,
}
