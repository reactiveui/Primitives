// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Stores the payload-bearing rows read during SQLite recovery.</summary>
/// <param name="Snapshot">The durable snapshot, when present.</param>
/// <param name="Pending">The upload-pending operations.</param>
/// <param name="Replay">The replay-visible operations.</param>
/// <param name="DeadLetters">The retained dead-letter records.</param>
internal readonly record struct SqliteRecoveredPayloadRows(
    LocalSnapshot? Snapshot,
    List<SyncOperation> Pending,
    List<SyncOperation> Replay,
    List<DeadLetterRecord> DeadLetters)
{
    /// <summary>Gets a value indicating whether any payload-bearing row was recovered.</summary>
    internal bool HasRows => Snapshot is not null || Pending.Count != 0 || Replay.Count != 0 || DeadLetters.Count != 0;
}
