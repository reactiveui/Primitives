// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a terminal operation record that cannot be retried automatically.</summary>
/// <param name="Operation">The operation that was dead-lettered.</param>
/// <param name="ReasonCode">The stable reason code.</param>
/// <param name="Attempts">The number of upload attempts.</param>
/// <param name="DeadLetteredAtUtc">The time the record became terminal.</param>
[System.Diagnostics.DebuggerDisplay("{ReasonCode,nq}")]
public sealed record DeadLetterRecord(
    SyncOperation Operation,
    string ReasonCode,
    int Attempts,
    DateTimeOffset DeadLetteredAtUtc);
