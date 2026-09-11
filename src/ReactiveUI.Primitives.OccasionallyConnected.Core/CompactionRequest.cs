// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a compaction request.</summary>
/// <param name="StreamId">The optional stream filter.</param>
/// <param name="RetainTerminalRecordsAfter">The terminal record retention cutoff.</param>
/// <param name="TargetBytes">The target byte budget after compaction.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq}")]
public sealed record CompactionRequest(
    StreamId? StreamId,
    DateTimeOffset RetainTerminalRecordsAfter,
    long TargetBytes);
