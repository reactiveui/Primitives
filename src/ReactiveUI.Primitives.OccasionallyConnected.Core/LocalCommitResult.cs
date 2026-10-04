// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the result of committing a local operation.</summary>
/// <param name="OperationId">The committed operation identifier.</param>
/// <param name="ClientSequence">The committed client sequence.</param>
/// <param name="SnapshotRevision">The snapshot revision after the commit.</param>
/// <param name="CommittedAtUtc">The commit timestamp.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq}")]
public sealed record LocalCommitResult(
    OperationId OperationId,
    long ClientSequence,
    long SnapshotRevision,
    DateTimeOffset CommittedAtUtc)
{
    /// <summary>Initializes a new instance of the <see cref="LocalCommitResult"/> class.</summary>
    /// <param name="operationId">The committed operation identifier.</param>
    /// <param name="clientSequence">The committed client sequence.</param>
    /// <param name="committedAtUtc">The commit timestamp.</param>
    public LocalCommitResult(OperationId operationId, long clientSequence, DateTimeOffset committedAtUtc)
        : this(operationId, clientSequence, SnapshotRevision: 0, committedAtUtc)
    {
    }
}
