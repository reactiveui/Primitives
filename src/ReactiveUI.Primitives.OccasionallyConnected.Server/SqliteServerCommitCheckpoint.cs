// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Names the durable SQLite server journal checkpoints reported to an <see cref="ISqliteServerCommitFaultPoint"/>.</summary>
/// <remarks>
/// A <c>BeforeCommit</c> checkpoint runs inside the open write transaction after every row change and before
/// <c>COMMIT</c>. An <c>AfterCommit</c> checkpoint runs after <c>COMMIT</c> returns and before the caller receives the result.
/// </remarks>
internal enum SqliteServerCommitCheckpoint
{
    /// <summary>The server effect, ledger, and events are written but not committed.</summary>
    TryCommitBeforeCommit = 0,

    /// <summary>The server effect, ledger, and events have committed.</summary>
    TryCommitAfterCommit = 1,
}
