// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Binds one add-wins set tag to the exact element bytes it observed.</summary>
[System.Diagnostics.DebuggerDisplay("{Tag,nq} Bytes = {ElementLength}")]
public sealed record TaggedElement
{
    /// <summary>The owned element bytes.</summary>
    private readonly byte[] _element = [];

    /// <summary>Gets the tag that identifies the operation.</summary>
    public required ElementTag Tag { get; init; }

    /// <summary>Gets an owned copy of the element bytes.</summary>
    public ReadOnlyMemory<byte> Element
    {
        get => _element.AsSpan().ToArray();
        init => _element = MergeableCopy.ElementBytes(value);
    }

    /// <summary>Gets the element byte count.</summary>
    public int ElementLength => _element.Length;

    /// <summary>Gets the internal owned element bytes without allocating.</summary>
    internal ReadOnlySpan<byte> ElementSpan => _element;
}
