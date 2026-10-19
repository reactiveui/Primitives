// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Describes a local CRDT mutation.</summary>
[System.Diagnostics.DebuggerDisplay("{ToKind(),nq} {ClientId,nq}")]
public sealed record MergeableChange
{
    /// <summary>The single immutable mutation case.</summary>
    private readonly object _case;

    /// <summary>Initializes a new instance of the <see cref="MergeableChange"/> class.</summary>
    /// <param name="value">The factory-created mutation case.</param>
    private MergeableChange(object value) => _case = value;

    /// <summary>Gets the authenticated actor for counter mutations.</summary>
    public string? ClientId => _case switch { IncrementOnlyCounterData counter => counter.ClientId, CounterData counter => counter.ClientId, _ => null };

    /// <summary>Gets the absolute G-counter component.</summary>
    public long IncrementOnlyTotal => _case is IncrementOnlyCounterData counter ? counter.Total : 0;

    /// <summary>Gets the absolute PN-counter positive component.</summary>
    public long IncrementTotal => _case is CounterData counter ? counter.Positive : 0;

    /// <summary>Gets the absolute PN-counter negative component.</summary>
    public long DecrementTotal => _case is CounterData counter ? counter.Negative : 0;

    /// <summary>Gets element or register bytes for OR-set and LWW mutations.</summary>
    public ReadOnlyMemory<byte> Bytes => ByteSpan.ToArray();

    /// <summary>Gets the byte count without allocating.</summary>
    public int ByteLength => ByteSpan.Length;

    /// <summary>Gets exact observed OR-set dots to remove.</summary>
    public IReadOnlyList<ElementTag> TagsToRemove => _case is RemoveData removal ? removal.Dots : Array.Empty<ElementTag>();

    /// <summary>Gets an optional authoritative LWW write stamp.</summary>
    public ConflictWriteStamp? LatestWriteStamp => _case is RegisterData register ? register.Stamp : null;

    /// <summary>Gets the internal owned bytes without allocating.</summary>
    internal ReadOnlySpan<byte> ByteSpan => _case switch
    {
        AddData add => add.Bytes,
        RemoveData removal => removal.Bytes,
        RegisterData register => register.Bytes,
        _ => ReadOnlySpan<byte>.Empty,
    };

    /// <summary>Creates a grow-only counter absolute component mutation.</summary>
    /// <param name="clientId">The authenticated actor.</param>
    /// <param name="component">The absolute nonnegative component.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableChange IncrementOnlyCounterSet(string clientId, long component)
    {
        ValidateCounter(clientId, component, 0);
        return new(new IncrementOnlyCounterData(clientId, component));
    }

    /// <summary>Creates a PN-counter absolute component mutation.</summary>
    /// <param name="clientId">The authenticated actor.</param>
    /// <param name="positiveComponent">The absolute nonnegative positive component.</param>
    /// <param name="negativeComponent">The absolute nonnegative negative component.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableChange CounterSet(string clientId, long positiveComponent, long negativeComponent)
    {
        ValidateCounter(clientId, positiveComponent, negativeComponent);
        return new(new CounterData(clientId, positiveComponent, negativeComponent));
    }

    /// <summary>Creates an OR-set add mutation whose dot is assigned by the committer.</summary>
    /// <param name="element">The element bytes.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableChange AddWinsSetAdd(ReadOnlyMemory<byte> element) =>
        new(new AddData(MergeableCopy.Bytes(element, MergeableCopy.MaximumOwnedMutationBytes)));

    /// <summary>Creates an OR-set remove mutation.</summary>
    /// <param name="element">The exact element bytes.</param>
    /// <param name="observedDots">The observed dots to tombstone.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableChange AddWinsSetRemove(ReadOnlyMemory<byte> element, IReadOnlyList<ElementTag> observedDots) =>
        new(new RemoveData(MergeableCopy.Bytes(element, MergeableCopy.MaximumOwnedMutationBytes), MergeableCopy.DotList(observedDots)));

    /// <summary>Creates a LWW register assignment.</summary>
    /// <param name="value">The register value bytes.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableChange LastWriterWinsValueSet(ReadOnlyMemory<byte> value) =>
        LastWriterWinsValueSet(value, null);

    /// <summary>Creates a LWW register assignment.</summary>
    /// <param name="value">The register value bytes.</param>
    /// <param name="stamp">The optional authoritative write stamp.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableChange LastWriterWinsValueSet(ReadOnlyMemory<byte> value, ConflictWriteStamp? stamp) =>
        new(new RegisterData(MergeableCopy.Bytes(value, MergeableCopy.MaximumOwnedMutationBytes), stamp));

    /// <summary>Converts the mutation case to its binary wire kind.</summary>
    /// <returns>The wire mutation kind.</returns>
    public MergeableChangeKind ToKind() => _case switch
    {
        IncrementOnlyCounterData => MergeableChangeKind.IncrementOnlyCounterSet,
        CounterData => MergeableChangeKind.CounterSet,
        AddData => MergeableChangeKind.AddWinsSetAdd,
        RemoveData => MergeableChangeKind.AddWinsSetRemove,
        _ => MergeableChangeKind.LastWriterWinsValueSet,
    };

    /// <summary>Validates the required counter case fields.</summary>
    /// <param name="clientId">The counter actor.</param>
    /// <param name="positive">The nonnegative positive component.</param>
    /// <param name="negative">The nonnegative negative component.</param>
    /// <exception cref="InvalidOperationException">The counter fields are invalid.</exception>
    private static void ValidateCounter(string clientId, long positive, long negative)
    {
        if (string.IsNullOrWhiteSpace(clientId) || positive < 0 || negative < 0)
        {
            throw new InvalidOperationException("A CRDT counter requires a client identifier and nonnegative components.");
        }
    }

    /// <summary>The increment-only counter case.</summary>
    /// <param name="ClientId">The counter actor.</param>
    /// <param name="Total">The absolute component.</param>
    private sealed record IncrementOnlyCounterData(string ClientId, long Total);

    /// <summary>The counter case.</summary>
    /// <param name="ClientId">The counter actor.</param>
    /// <param name="Positive">The positive component.</param>
    /// <param name="Negative">The negative component.</param>
    private sealed record CounterData(string ClientId, long Positive, long Negative);

    /// <summary>The add-wins set addition case.</summary>
    /// <param name="Bytes">The owned element bytes.</param>
    private sealed record AddData(byte[] Bytes);

    /// <summary>The add-wins set removal case.</summary>
    /// <param name="Bytes">The owned element bytes.</param>
    /// <param name="Dots">The owned observed dots.</param>
    private sealed record RemoveData(byte[] Bytes, IReadOnlyList<ElementTag> Dots);

    /// <summary>The last-writer-wins register case.</summary>
    /// <param name="Bytes">The owned register bytes.</param>
    /// <param name="Stamp">The optional authoritative write stamp.</param>
    private sealed record RegisterData(byte[] Bytes, ConflictWriteStamp? Stamp);
}
