// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes pending synchronization work.</summary>
/// <param name="OperationCount">The pending operation count.</param>
/// <param name="Bytes">The estimated retained bytes of pending operations.</param>
/// <param name="OldestOperationUtc">The oldest pending operation timestamp.</param>
[System.Diagnostics.DebuggerDisplay("{OperationCount,nq} operations")]
public sealed record PendingSyncSummary(
    int OperationCount,
    long Bytes,
    DateTimeOffset? OldestOperationUtc);
