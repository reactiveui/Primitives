// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Identifies one durable per-stream client operation dot.</summary>
[System.Diagnostics.DebuggerDisplay("{ClientId,nq}:{ClientSequence}")]
public sealed record CrdtDot : IComparable, IComparable<CrdtDot>
{
    /// <summary>Gets the authenticated client identifier.</summary>
    public required string ClientId { get; init; }

    /// <summary>Gets the durable per-stream client sequence.</summary>
    public required long ClientSequence { get; init; }

    /// <param name="other">The other.</param>
    /// <inheritdoc/>
    /// <returns>The result.</returns>
    public int CompareTo(CrdtDot? other)
    {
        if (other is null)
        {
            return 1;
        }

        var client = string.CompareOrdinal(ClientId, other.ClientId);
        return client != 0 ? client : ClientSequence.CompareTo(other.ClientSequence);
    }

    /// <param name="obj">The obj.</param>
    /// <inheritdoc/>
    /// <returns>The result.</returns>
    public int CompareTo(object? obj) =>
        obj is CrdtDot other
            ? CompareTo(other)
            : 1;
}
