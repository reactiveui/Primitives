// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies a table with protected columns.</summary>
internal enum SqliteProtectedTableKind
{
    /// <summary>The outbox table.</summary>
    Outbox = 0,

    /// <summary>The original authoritative outbox mutation table.</summary>
    OutboxAuthoritativeMutations = 1,

    /// <summary>The outbox metadata table.</summary>
    OutboxMetadata = 2,

    /// <summary>The snapshot table.</summary>
    Snapshots = 3,

    /// <summary>The current authoritative snapshot table.</summary>
    SnapshotAuthoritativeStates = 4,

    /// <summary>The stream table.</summary>
    Streams = 5,

    /// <summary>The remote inbox table.</summary>
    Inbox = 6,

    /// <summary>The payload quarantine table.</summary>
    PayloadQuarantine = 7,

    /// <summary>The dead-lettered rows of the operation state table.</summary>
    DeadLetters = 8,
}
