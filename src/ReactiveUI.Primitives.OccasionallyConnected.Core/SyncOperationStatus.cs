// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the current state of a synchronization operation.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="State">The operation state.</param>
/// <param name="Attempt">The current upload attempt count.</param>
/// <param name="ChangedAtUtc">The time this state was recorded.</param>
/// <param name="ReasonCode">The optional stable reason code.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {State,nq}")]
public sealed record SyncOperationStatus(
    OperationId OperationId,
    StreamId StreamId,
    SyncOperationState State,
    int Attempt,
    DateTimeOffset ChangedAtUtc,
    string? ReasonCode);
