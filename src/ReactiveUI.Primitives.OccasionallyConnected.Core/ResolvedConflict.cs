// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a resolved conflict.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="ResolutionCode">The stable resolution code.</param>
/// <param name="ResolvedPayload">The optional resolved payload.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {ResolutionCode,nq}")]
public sealed record ResolvedConflict(
    OperationId OperationId,
    string ResolutionCode,
    PayloadEnvelope? ResolvedPayload);
