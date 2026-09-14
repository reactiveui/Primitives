// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Reports persisted subscription acknowledgement state.</summary>
internal sealed record ServerSubscriptionState
{
    /// <summary>Gets the trusted subscription identity.</summary>
    internal required ServerSubscriptionIdentity Identity { get; init; }

    /// <summary>Gets the durable generation assigned when this binding was created.</summary>
    internal required long Generation { get; init; }

    /// <summary>Gets the durable semantic revision for this binding.</summary>
    internal required long Revision { get; init; }

    /// <summary>Gets the latest offered complete cursor.</summary>
    internal required string? LatestOfferedCursor { get; init; }

    /// <summary>Gets the latest offered complete group sequence.</summary>
    internal required long LatestOfferedGroupSequence { get; init; }

    /// <summary>Gets the acknowledged cursor.</summary>
    internal required string? AcknowledgedCursor { get; init; }

    /// <summary>Gets the acknowledged complete group sequence.</summary>
    internal required long AcknowledgedGroupSequence { get; init; }

    /// <summary>Gets the retained offered cursor count.</summary>
    internal required int OfferCount { get; init; }
}
