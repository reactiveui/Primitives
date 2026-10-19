// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Groups data needed to build result reconciliation snapshots.</summary>
internal sealed class SqliteResultReconciliationPlan
{
    /// <summary>Initializes a new instance of the <see cref="SqliteResultReconciliationPlan"/> class.</summary>
    /// <param name="result">The validated result.</param>
    /// <param name="snapshotMutations">The replacement mutations.</param>
    /// <param name="savedAtUtc">The snapshot save timestamp.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    internal SqliteResultReconciliationPlan(
        RemoteSyncResult result,
        IReadOnlyList<SnapshotMutation> snapshotMutations,
        DateTimeOffset savedAtUtc,
        long maximumPayloadBytes)
    {
        Result = result;
        SnapshotMutations = snapshotMutations;
        SavedAtUtc = savedAtUtc;
        MaximumPayloadBytes = maximumPayloadBytes;
    }

    /// <summary>Gets the validated result.</summary>
    internal RemoteSyncResult Result { get; }

    /// <summary>Gets the replacement mutations.</summary>
    internal IReadOnlyList<SnapshotMutation> SnapshotMutations { get; }

    /// <summary>Gets the snapshot save timestamp.</summary>
    internal DateTimeOffset SavedAtUtc { get; }

    /// <summary>Gets the maximum payload bytes this adapter can materialize.</summary>
    internal long MaximumPayloadBytes { get; }
}
