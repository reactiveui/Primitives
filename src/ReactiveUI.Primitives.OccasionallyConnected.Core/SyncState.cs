// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the current synchronization state.</summary>
/// <param name="Status">The lifecycle status.</param>
/// <param name="NetworkAvailable">A value indicating whether a network path is currently available.</param>
/// <param name="PendingOperations">The number of pending operations.</param>
/// <param name="PendingBytes">The pending operation payload bytes.</param>
/// <param name="ChangedAtUtc">The time this state was recorded.</param>
/// <param name="LastSuccessfulSyncUtc">The most recent successful synchronization time.</param>
/// <param name="RetryAfter">The optional retry delay requested by policy or the server.</param>
/// <param name="ReasonCode">The optional stable reason code.</param>
[System.Diagnostics.DebuggerDisplay("{Status,nq}")]
public sealed record SyncState(
    SyncLifecycleStatus Status,
    bool NetworkAvailable,
    int PendingOperations,
    long PendingBytes,
    DateTimeOffset ChangedAtUtc,
    DateTimeOffset? LastSuccessfulSyncUtc,
    TimeSpan? RetryAfter,
    string? ReasonCode);
