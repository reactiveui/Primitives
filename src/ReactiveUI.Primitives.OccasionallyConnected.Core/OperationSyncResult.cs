// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the remote result for one operation.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="Kind">The result kind.</param>
/// <param name="ReasonCode">The optional stable reason code.</param>
/// <param name="ServerVersion">The optional server version after the decision.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {Kind,nq}")]
public sealed record OperationSyncResult(
    OperationId OperationId,
    OperationResultKind Kind,
    string? ReasonCode,
    string? ServerVersion);
