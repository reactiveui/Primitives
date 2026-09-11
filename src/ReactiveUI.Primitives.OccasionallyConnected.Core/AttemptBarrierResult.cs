// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes whether a remote attempt may proceed after the durable attempt barrier is recorded.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="Attempt">The attempt number covered by the barrier decision.</param>
/// <param name="MaySend">Whether the engine may perform network I/O for this attempt.</param>
/// <param name="ReasonCode">The optional stable reason code when sending is denied.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} Attempt {Attempt,nq} MaySend={MaySend,nq}")]
public sealed record AttemptBarrierResult(
    OperationId OperationId,
    int Attempt,
    bool MaySend,
    string? ReasonCode);
