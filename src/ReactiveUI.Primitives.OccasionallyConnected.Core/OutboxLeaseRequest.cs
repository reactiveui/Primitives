// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a request to lease pending outbox operations.</summary>
/// <param name="StreamId">The optional stream filter.</param>
/// <param name="MaximumOperations">The maximum operation count to lease.</param>
/// <param name="MaximumBytes">The maximum payload bytes to lease.</param>
/// <param name="LeaseDuration">The lease duration.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq}")]
public sealed record OutboxLeaseRequest(
    StreamId? StreamId,
    int MaximumOperations,
    long MaximumBytes,
    TimeSpan LeaseDuration);
