// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a durable publish receipt.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="ClientSequence">The assigned client sequence.</param>
/// <param name="State">The initial durable operation state.</param>
/// <param name="SavedAtUtc">The time the operation was saved locally.</param>
[System.Diagnostics.DebuggerDisplay("{OperationId,nq} {State,nq}")]
public sealed record PublishReceipt(
    OperationId OperationId,
    long ClientSequence,
    SyncOperationState State,
    DateTimeOffset SavedAtUtc);
