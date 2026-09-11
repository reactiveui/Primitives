// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes an operation rejected by the server.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="ReasonCode">The stable reason code.</param>
/// <param name="MayResubmit">Whether the client may submit replacement work.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {ReasonCode,nq}")]
public sealed record RejectedOperation(
    OperationId OperationId,
    string ReasonCode,
    bool MayResubmit);
