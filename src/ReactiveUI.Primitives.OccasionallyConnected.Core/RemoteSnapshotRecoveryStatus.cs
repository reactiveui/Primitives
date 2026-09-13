// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the outcome of a remote snapshot recovery request.</summary>
public enum RemoteSnapshotRecoveryStatus
{
    /// <summary>A coherent snapshot checkpoint and exact pending-operation dispositions are present.</summary>
    Recovered = 0,

    /// <summary>The server cannot materialize the requested allowlisted client state projection.</summary>
    UnsupportedProjection = 1,

    /// <summary>The server no longer retains enough provenance to recover safely.</summary>
    RetentionExpired = 2,

    /// <summary>A previously attempted pending operation cannot be resolved without violating its guarantee.</summary>
    AmbiguousPendingOperation = 3,

    /// <summary>The request failed validation.</summary>
    ValidationRejected = 4,

    /// <summary>The request or response cannot fit bounded snapshot recovery limits.</summary>
    CapacityExceeded = 5,

    /// <summary>The coherent server view changed before it could be offered durably.</summary>
    RetryableConcurrentChange = 6,
}
