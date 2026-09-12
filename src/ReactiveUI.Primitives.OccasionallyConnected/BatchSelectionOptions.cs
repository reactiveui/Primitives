// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides caller-sampled limits and elapsed dwell time for one batch-selection pass.</summary>
internal sealed record BatchSelectionOptions
{
    /// <summary>Gets the validated local batching limits.</summary>
    public required BatchingOptions Batching { get; init; }

    /// <summary>Gets the negotiated server operation ceiling.</summary>
    public required int NegotiatedMaximumOperations { get; init; }

    /// <summary>Gets the negotiated server encoded byte ceiling.</summary>
    public required long NegotiatedMaximumBytes { get; init; }

    /// <summary>Gets the fixed encoded protocol envelope bytes.</summary>
    public required long EnvelopeBytes { get; init; }

    /// <summary>Gets the caller-sampled monotonic elapsed time since the first candidate became eligible.</summary>
    public required TimeSpan FirstEligibleElapsed { get; init; }
}
