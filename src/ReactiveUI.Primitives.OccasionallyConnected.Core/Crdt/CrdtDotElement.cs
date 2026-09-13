// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Binds one OR-set dot to the exact element bytes it observed.</summary>
[System.Diagnostics.DebuggerDisplay("{Dot,nq} Bytes = {ElementLength}")]
public sealed record CrdtDotElement
{
    /// <summary>The owned element bytes.</summary>
    private readonly byte[] _element = [];

    /// <summary>Gets the operation dot.</summary>
    public required CrdtDot Dot { get; init; }

    /// <summary>Gets an owned copy of the element bytes.</summary>
    public ReadOnlyMemory<byte> Element
    {
        get => _element.AsSpan().ToArray();
        init => _element = CrdtCopy.ElementBytes(value);
    }

    /// <summary>Gets the element byte count.</summary>
    public int ElementLength => _element.Length;

    /// <summary>Gets the internal owned element bytes without allocating.</summary>
    internal ReadOnlySpan<byte> ElementSpan => _element;
}
