// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

/// <summary>Identifies durable journal append and compaction boundaries.</summary>
internal enum FileSystemJournalCheckpoint
{
    /// <summary>A journal header was appended before its payload is written.</summary>
    AfterAppendHeader = 0,

    /// <summary>The compacted journal has been flushed and is ready to replace the active journal.</summary>
    BeforeCompactionJournalReplace = 1,

    /// <summary>The compacted journal has replaced the active journal.</summary>
    AfterCompactionJournalReplace = 2,
}
