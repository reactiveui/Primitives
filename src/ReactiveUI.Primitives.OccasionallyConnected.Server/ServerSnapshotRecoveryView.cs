// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Captures the durable stream and subscription state used to decide a snapshot recovery response.</summary>
internal sealed record ServerSnapshotRecoveryView
{
    /// <summary>Gets the atomic stream snapshot.</summary>
    internal required ServerCommitSnapshot Snapshot { get; init; }

    /// <summary>Gets the retained subscription state, or null when the binding is expired or missing.</summary>
    internal required ServerSubscriptionState? SubscriptionState { get; init; }

    /// <summary>Gets the retained proof that the expired cursor was previously offered to this subscription.</summary>
    internal required ServerSubscriptionOffer? ExpiredCursorOffer { get; init; }

    /// <summary>Gets the expired cursor value that was present on the recovery request that produced this view.</summary>
    internal required string? RequestedExpiredCursor { get; init; }

    /// <summary>Gets the number of pending operations captured before replay-only operations in this view.</summary>
    internal required int CapturedPendingOperationCount { get; init; }

    /// <summary>Gets dispositions from trusted retained ledger entries for the requested operations.</summary>
    internal required IReadOnlyList<ServerSnapshotOperationDisposition> OperationDispositions
    {
        get;
        init => field = ServerSnapshotRecoveryCollectionCopy.List(value, nameof(OperationDispositions));
    }

    /// <summary>Gets canonical fingerprints for the ordered pending plus replay-only operation union.</summary>
    internal required IReadOnlyList<ServerCommitFingerprint> OperationFingerprints
    {
        get;
        init => field = ServerSnapshotRecoveryCollectionCopy.List(value, nameof(OperationFingerprints));
    }
}
