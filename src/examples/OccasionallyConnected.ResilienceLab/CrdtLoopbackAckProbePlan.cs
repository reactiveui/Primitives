// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Describes the public batches and subscription identity used to prove ACK resume behavior.</summary>
internal sealed record CrdtLoopbackAckProbePlan
{
    /// <summary>Gets the stream under ACK probe.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the same-subscription identifier used for the full probe.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the first probe operation batch.</summary>
    public required SyncBatch FirstBatch { get; init; }

    /// <summary>Gets the second probe operation batch.</summary>
    public required SyncBatch SecondBatch { get; init; }

    /// <summary>Gets the expected first authoritative counter value.</summary>
    public required int FirstValue { get; init; }

    /// <summary>Gets the expected resumed authoritative counter value.</summary>
    public required int SecondValue { get; init; }

    /// <summary>Gets the expected event and completion count for each probe page.</summary>
    public required int ExpectedEventCount { get; init; }

    /// <summary>Gets the diagnostic fragment expected when the stale initial read rewinds an acknowledged subscription.</summary>
    public required string RewindDiagnosticFragment { get; init; }
}
