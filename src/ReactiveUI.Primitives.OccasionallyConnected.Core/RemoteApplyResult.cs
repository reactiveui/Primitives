// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the durable result of applying a remote batch locally.</summary>
/// <param name="NextCursor">The cursor after the applied batch.</param>
/// <param name="AppliedCount">The number of events applied.</param>
/// <param name="DuplicateCount">The number of duplicate events ignored.</param>
/// <param name="SnapshotRevision">The snapshot revision after the remote batch transaction.</param>
[System.Diagnostics.DebuggerDisplay("{NextCursor,nq}")]
public sealed record RemoteApplyResult(
    string NextCursor,
    int AppliedCount,
    int DuplicateCount,
    long SnapshotRevision)
{
    /// <summary>Initializes a new instance of the <see cref="RemoteApplyResult"/> class.</summary>
    /// <param name="nextCursor">The cursor after the applied batch.</param>
    /// <param name="appliedCount">The number of events applied.</param>
    /// <param name="duplicateCount">The number of duplicate events ignored.</param>
    public RemoteApplyResult(string nextCursor, int appliedCount, int duplicateCount)
        : this(nextCursor, appliedCount, duplicateCount, SnapshotRevision: 0)
    {
    }
}
