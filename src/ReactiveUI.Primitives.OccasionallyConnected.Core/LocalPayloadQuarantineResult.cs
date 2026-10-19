// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports the quarantine marker persisted for an affected stream.</summary>
/// <param name="Record">The persisted quarantine marker.</param>
/// <param name="Created">A value indicating whether this call created the marker.</param>
[System.Diagnostics.DebuggerDisplay("{Record.StreamId,nq}: Created={Created,nq}")]
public sealed record LocalPayloadQuarantineResult(LocalPayloadQuarantineRecord Record, bool Created);
