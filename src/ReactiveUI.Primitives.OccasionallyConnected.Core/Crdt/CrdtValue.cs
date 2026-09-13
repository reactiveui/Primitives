// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Represents the canonical value derived from a CRDT state.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} Counter = {Counter}")]
public sealed record CrdtValue
{
    /// <summary>Gets the CRDT kind that produced the value.</summary>
    public required CrdtKind Kind { get; init; }

    /// <summary>Gets the derived counter value.</summary>
    public long Counter { get; init; }

    /// <summary>Gets owned bytes for LWW registers.</summary>
    public ReadOnlyMemory<byte> Bytes
    {
        get => field.ToArray();
        init => field = CrdtCopy.RegisterBytes(value);
    } = Array.Empty<byte>();

    /// <summary>Gets derived OR-set element values.</summary>
    public IReadOnlyList<ReadOnlyMemory<byte>> Elements
    {
        get;
        init => field = CrdtCopy.MemoryList(value);
    } = Array.Empty<ReadOnlyMemory<byte>>();
}
