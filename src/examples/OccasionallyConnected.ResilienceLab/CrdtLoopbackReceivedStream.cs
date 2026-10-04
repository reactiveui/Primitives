// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Holds a received stream page and cursor frontier.</summary>
/// <param name="States">The decoded authoritative states.</param>
/// <param name="PreviousCursor">The cursor preceding the page.</param>
/// <param name="Cursor">The next frontier cursor.</param>
/// <param name="EventIds">The received server event identifiers.</param>
/// <param name="EventCount">The received event count.</param>
/// <param name="CompletedOperationCount">The completed operation count in the page.</param>
[DebuggerDisplay("{Cursor,nq}; Events={EventCount,nq}; Completed={CompletedOperationCount,nq}")]
internal sealed record CrdtLoopbackReceivedStream(
    IReadOnlyList<CrdtState> States,
    string? PreviousCursor,
    string Cursor,
    IReadOnlyList<Guid> EventIds,
    int EventCount,
    int CompletedOperationCount);
