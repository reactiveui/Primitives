// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides an immutable view of one endpoint circuit breaker.</summary>
/// <param name="Endpoint">The endpoint represented by the breaker.</param>
/// <param name="State">The breaker admission state.</param>
/// <param name="ConsecutiveTransientFailures">The currently recorded consecutive transient failures.</param>
/// <param name="RetryAfterUtc">The earliest time an open breaker may admit a recovery probe, if applicable.</param>
[DebuggerDisplay("{Endpoint,nq}; {State,nq}; Failures={ConsecutiveTransientFailures,nq}")]
public sealed record CircuitBreakerSnapshot(
    string Endpoint,
    CircuitBreakerState State,
    int ConsecutiveTransientFailures,
    DateTimeOffset? RetryAfterUtc);
