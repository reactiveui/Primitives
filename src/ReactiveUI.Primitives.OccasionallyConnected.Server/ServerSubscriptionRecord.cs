// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores retained acknowledgement state for one subscription.</summary>
internal sealed class ServerSubscriptionRecord
{
    /// <summary>Initializes a new instance of the <see cref="ServerSubscriptionRecord"/> class.</summary>
    /// <param name="identity">The trusted identity.</param>
    /// <param name="updatedAtUtc">The creation timestamp.</param>
    /// <param name="logicalBytes">The retained logical byte count.</param>
    internal ServerSubscriptionRecord(ServerSubscriptionIdentity identity, DateTimeOffset updatedAtUtc, long logicalBytes)
    {
        Identity = identity;
        UpdatedAtUtc = updatedAtUtc;
        LastTouchedUtc = updatedAtUtc;
        LogicalBytes = logicalBytes;
    }

    /// <summary>Gets the trusted identity.</summary>
    internal ServerSubscriptionIdentity Identity { get; }

    /// <summary>Gets or sets the immutable initial position requested for this binding.</summary>
    internal StartPosition InitialStartPosition { get; set; } = StartPosition.FromSequence(0);

    /// <summary>Gets or sets the internally resolved first-read anchor cursor.</summary>
    internal string? InitialAnchorCursor { get; set; }

    /// <summary>Gets or sets the complete group sequence represented by the initial anchor.</summary>
    internal long InitialAnchorGroupSequence { get; set; }

    /// <summary>Gets or sets a value indicating whether the initial anchor has resolved.</summary>
    internal bool InitialAnchorResolved { get; set; } = true;

    /// <summary>Gets or sets the acknowledged cursor.</summary>
    internal string? AcknowledgedCursor { get; set; }

    /// <summary>Gets or sets the acknowledged complete group sequence.</summary>
    internal long AcknowledgedGroupSequence { get; set; }

    /// <summary>Gets or sets the latest offered cursor.</summary>
    internal string? LatestOfferedCursor { get; set; }

    /// <summary>Gets or sets the latest offered complete group sequence.</summary>
    internal long LatestOfferedGroupSequence { get; set; }

    /// <summary>Gets or sets the acknowledgement timestamp.</summary>
    internal DateTimeOffset? AcknowledgedAtUtc { get; set; }

    /// <summary>Gets or sets the last mutation timestamp.</summary>
    internal DateTimeOffset UpdatedAtUtc { get; set; }

    /// <summary>Gets or sets the subscription binding retention timestamp.</summary>
    internal DateTimeOffset LastTouchedUtc { get; set; }

    /// <summary>Gets or sets the retained logical byte count.</summary>
    internal long LogicalBytes { get; set; }

    /// <summary>Gets offered complete cursors that can still be acknowledged.</summary>
    internal Dictionary<string, ServerSubscriptionOffer> Offers { get; } = [with(StringComparer.Ordinal)];
}
