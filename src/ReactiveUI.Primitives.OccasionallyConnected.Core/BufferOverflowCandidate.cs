// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes one pending non-durable operation that a buffer overflow policy may evict.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="ClientSequence">The per-stream client sequence.</param>
/// <param name="PayloadBytes">The encoded payload length in bytes.</param>
/// <param name="Priority">The operation scheduling priority.</param>
/// <param name="TimestampUtc">The time the operation was committed locally.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} #{ClientSequence,nq}")]
public sealed record BufferOverflowCandidate(
    OperationId OperationId,
    long ClientSequence,
    long PayloadBytes,
    int Priority,
    DateTimeOffset TimestampUtc);
