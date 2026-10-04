// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a compaction result.</summary>
/// <param name="RecordsRemoved">The number of records removed.</param>
/// <param name="BytesReclaimed">The number of bytes reclaimed.</param>
[System.Diagnostics.DebuggerDisplay("{RecordsRemoved,nq} records")]
public sealed record CompactionResult(
    long RecordsRemoved,
    long BytesReclaimed);
