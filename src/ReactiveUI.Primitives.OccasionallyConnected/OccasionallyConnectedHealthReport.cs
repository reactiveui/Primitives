// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Summarizes context health using counts, ages, and stable reason codes only.</summary>
/// <param name="Status">The health status.</param>
/// <param name="LifecycleStatus">The synchronization lifecycle status the report was derived from.</param>
/// <param name="ReasonCode">The stable reason code, or <see langword="null"/> when the context is healthy.</param>
/// <param name="PendingOperations">The number of operations waiting for remote synchronization.</param>
/// <param name="PendingBytes">The encoded size of operations waiting for remote synchronization.</param>
/// <param name="RetryAfter">The delay before the next connection attempt, when known.</param>
/// <param name="StateAge">The time elapsed since the lifecycle state changed.</param>
[System.Diagnostics.DebuggerDisplay("{Status,nq}: {ReasonCode,nq}")]
public sealed record OccasionallyConnectedHealthReport(
    OccasionallyConnectedHealthStatus Status,
    SyncLifecycleStatus LifecycleStatus,
    string? ReasonCode,
    int PendingOperations,
    long PendingBytes,
    TimeSpan? RetryAfter,
    TimeSpan StateAge);
