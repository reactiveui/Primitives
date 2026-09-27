// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Names the durable SQLite write checkpoints reported to an <see cref="ISqliteCommitFaultPoint"/>.</summary>
/// <remarks>
/// A <c>BeforeCommit</c> checkpoint runs inside the open write transaction after every row change and before
/// <c>COMMIT</c>. An <c>AfterCommit</c> checkpoint runs after <c>COMMIT</c> returns and before the caller receives the result.
/// </remarks>
internal enum SqliteCommitCheckpoint
{
    /// <summary>The local operation, snapshot, and next sequence rows are written but not committed.</summary>
    LocalCommitBeforeCommit = 0,

    /// <summary>The local operation transaction has committed.</summary>
    LocalCommitAfterCommit = 1,

    /// <summary>The pre-send attempt barrier is written but not committed.</summary>
    AttemptBarrierBeforeCommit = 2,

    /// <summary>The pre-send attempt barrier transaction has committed.</summary>
    AttemptBarrierAfterCommit = 3,

    /// <summary>The remote inbox, cursor, and snapshot rows are written but not committed.</summary>
    RemoteApplyBeforeCommit = 4,

    /// <summary>The remote apply transaction has committed.</summary>
    RemoteApplyAfterCommit = 5,

    /// <summary>The upload result, lease release, and snapshot rows are written but not committed.</summary>
    SyncResultBeforeCommit = 6,

    /// <summary>The upload result transaction has committed.</summary>
    SyncResultAfterCommit = 7,

    /// <summary>The dead-letter, lease, and snapshot rows are written but not committed.</summary>
    DeadLetterBeforeCommit = 8,

    /// <summary>The dead-letter transaction has committed.</summary>
    DeadLetterAfterCommit = 9,

    /// <summary>The compaction deletes are written but not committed.</summary>
    CompactionBeforeCommit = 10,

    /// <summary>The compaction transaction has committed.</summary>
    CompactionAfterCommit = 11,

    /// <summary>The plaintext rows are encrypted and the protection marker is written but not committed.</summary>
    EncryptionMigrationBeforeCommit = 12,

    /// <summary>The plaintext-to-encrypted migration transaction has committed.</summary>
    EncryptionMigrationAfterCommit = 13,

    /// <summary>The values under older keys are re-encrypted but not committed.</summary>
    KeyRotationBeforeCommit = 14,

    /// <summary>The key rotation transaction has committed.</summary>
    KeyRotationAfterCommit = 15,
}
