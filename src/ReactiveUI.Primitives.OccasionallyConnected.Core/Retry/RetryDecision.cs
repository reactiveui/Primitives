// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a retry policy decision and the next state to persist.</summary>
/// <param name="Kind">The decision kind.</param>
/// <param name="StopReason">The terminal stop reason when <paramref name="Kind"/> is <see cref="RetryDecisionKind.Stop"/>.</param>
/// <param name="Delay">The delay before retrying.</param>
/// <param name="DueUtc">The UTC time at which the retry becomes due.</param>
/// <param name="NextState">The next durable retry state.</param>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}: {StopReason,nq}")]
public sealed record RetryDecision(
    RetryDecisionKind Kind,
    RetryStopReason StopReason,
    TimeSpan? Delay,
    DateTimeOffset? DueUtc,
    RetryState NextState);
