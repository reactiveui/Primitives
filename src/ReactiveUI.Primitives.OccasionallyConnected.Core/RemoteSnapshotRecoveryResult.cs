// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the remote answer to a bounded snapshot recovery request.</summary>
[System.Diagnostics.DebuggerDisplay("{Status,nq}")]
public sealed record RemoteSnapshotRecoveryResult
{
    /// <summary>Gets the remote recovery status.</summary>
    public required RemoteSnapshotRecoveryStatus Status { get; init; }

    /// <summary>Gets the coherent checkpoint when <see cref="Status"/> is <see cref="RemoteSnapshotRecoveryStatus.Recovered"/>.</summary>
    public RemoteSnapshotCheckpoint? Checkpoint { get; init; }

    /// <summary>Gets exact requested-operation dispositions for a recovered checkpoint.</summary>
    public IReadOnlyList<SnapshotOperationDisposition> OperationDispositions
    {
        get;
        init => field = SnapshotRecoveryCollectionCopy.List(value, nameof(OperationDispositions));
    }
    = [];

    /// <summary>Gets an optional bounded stable reason code.</summary>
    public string? ReasonCode { get; init; }
}
