// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>The immutable facts read from one durable client store.</summary>
/// <param name="SubscriptionId">The subscription identity.</param>
/// <param name="ServerCursor">The persisted server cursor.</param>
/// <param name="SnapshotCounter">The restored counter snapshot.</param>
/// <param name="PendingOperationId">The pending operation identity.</param>
/// <param name="PendingClientSequence">The pending client sequence.</param>
/// <param name="PendingCount">The count of pending operations.</param>
/// <param name="OperationStatus">The persisted operation status.</param>
/// <param name="RetryState">The persisted retry state.</param>
/// <param name="InboxCount">The durable inbox entry count.</param>
internal sealed record ClientStoreProof(
    SubscriptionId SubscriptionId,
    string? ServerCursor,
    long? SnapshotCounter,
    OperationId? PendingOperationId,
    long? PendingClientSequence,
    int PendingCount,
    SyncOperationStatus? OperationStatus,
    RetryState? RetryState,
    int InboxCount);
