// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Groups repeated commit data needed to read an existing result.</summary>
internal sealed class SqliteCommittedResultQuery
{
    /// <summary>Initializes a new instance of the <see cref="SqliteCommittedResultQuery"/> class.</summary>
    /// <param name="operation">The repeated operation.</param>
    /// <param name="snapshotMutation">The original snapshot mutation.</param>
    /// <param name="fingerprint">The canonical commit intent fingerprint.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    internal SqliteCommittedResultQuery(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        byte[] fingerprint,
        long maximumPayloadBytes)
    {
        Operation = operation;
        SnapshotMutation = snapshotMutation;
        Fingerprint = fingerprint;
        MaximumPayloadBytes = maximumPayloadBytes;
    }

    /// <summary>Gets the repeated operation.</summary>
    internal SyncOperation Operation { get; }

    /// <summary>Gets the original snapshot mutation.</summary>
    internal SnapshotMutation SnapshotMutation { get; }

    /// <summary>Gets the canonical commit intent fingerprint.</summary>
    internal byte[] Fingerprint { get; }

    /// <summary>Gets the maximum payload bytes this adapter can materialize.</summary>
    internal long MaximumPayloadBytes { get; }
}
