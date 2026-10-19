// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the durable state of a synchronization operation.</summary>
public enum SyncOperationState
{
    /// <summary>The operation has committed to the local store.</summary>
    SavedLocally = 0,

    /// <summary>The operation is queued for upload.</summary>
    QueuedForUpload = 1,

    /// <summary>The operation is being uploaded.</summary>
    Uploading = 2,

    /// <summary>The operation conflicted with server state.</summary>
    Conflict = 3,

    /// <summary>The operation synchronized successfully.</summary>
    Synchronized = 4,

    /// <summary>The operation was rejected by the server.</summary>
    Rejected = 5,

    /// <summary>The operation has been moved to the dead-letter store.</summary>
    DeadLettered = 6,

    /// <summary>The operation has an ambiguous transport outcome.</summary>
    Ambiguous = 7,

    /// <summary>The negotiated guarantee window expired before a terminal outcome.</summary>
    GuaranteeExpired = 8,
}
