// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Describes a local CRDT mutation.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} {ActorId,nq}")]
#if NET11_0_OR_GREATER
public sealed record CrdtMutation : IUnion
#else
public sealed record CrdtMutation
#endif
{
#if NET11_0_OR_GREATER
    object IUnion.Value => this;
#endif
    /// <summary>The owned element or register bytes.</summary>
    private readonly byte[] _bytes = [];

    /// <summary>Gets the mutation kind.</summary>
    public required CrdtMutationKind Kind { get; init; }

    /// <summary>Gets the authenticated actor for counter mutations.</summary>
    public string? ActorId { get; init; }

    /// <summary>Gets the absolute G-counter component.</summary>
    public long GCounterComponent { get; init; }

    /// <summary>Gets the absolute PN-counter positive component.</summary>
    public long PNCounterPositiveComponent { get; init; }

    /// <summary>Gets the absolute PN-counter negative component.</summary>
    public long PNCounterNegativeComponent { get; init; }

    /// <summary>Gets element or register bytes for OR-set and LWW mutations.</summary>
    public ReadOnlyMemory<byte> Bytes
    {
        get => _bytes.AsSpan().ToArray();
        init => _bytes = CrdtCopy.Bytes(value, CrdtCopy.MaximumOwnedMutationBytes);
    }

    /// <summary>Gets the byte count without allocating.</summary>
    public int ByteLength => _bytes.Length;

    /// <summary>Gets exact observed OR-set dots to remove.</summary>
    public IReadOnlyList<CrdtDot> ObservedDots
    {
        get;
        init => field = CrdtCopy.DotList(value);
    } = Array.Empty<CrdtDot>();

    /// <summary>Gets an optional authoritative LWW write stamp.</summary>
    public ConflictWriteStamp? RegisterStamp { get; init; }

    /// <summary>Gets the internal owned bytes without allocating.</summary>
    internal ReadOnlySpan<byte> ByteSpan => _bytes;

    /// <summary>Creates a grow-only counter absolute component mutation.</summary>
    /// <param name="actorId">The authenticated actor.</param>
    /// <param name="component">The absolute nonnegative component.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtMutation GCounterSet(string actorId, long component) =>
        new() { Kind = CrdtMutationKind.GCounterSet, ActorId = actorId, GCounterComponent = component };

    /// <summary>Creates a PN-counter absolute component mutation.</summary>
    /// <param name="actorId">The authenticated actor.</param>
    /// <param name="positiveComponent">The absolute nonnegative positive component.</param>
    /// <param name="negativeComponent">The absolute nonnegative negative component.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtMutation PNCounterSet(string actorId, long positiveComponent, long negativeComponent) =>
        new() { Kind = CrdtMutationKind.PNCounterSet, ActorId = actorId, PNCounterPositiveComponent = positiveComponent, PNCounterNegativeComponent = negativeComponent };

    /// <summary>Creates an OR-set add mutation whose dot is assigned by the committer.</summary>
    /// <param name="element">The element bytes.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtMutation ORSetAdd(ReadOnlyMemory<byte> element) =>
        new() { Kind = CrdtMutationKind.ORSetAdd, Bytes = element };

    /// <summary>Creates an OR-set remove mutation.</summary>
    /// <param name="element">The exact element bytes.</param>
    /// <param name="observedDots">The observed dots to tombstone.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtMutation ORSetRemove(ReadOnlyMemory<byte> element, IReadOnlyList<CrdtDot> observedDots) =>
        new() { Kind = CrdtMutationKind.ORSetRemove, Bytes = element, ObservedDots = observedDots };

    /// <summary>Creates a LWW register assignment.</summary>
    /// <param name="value">The register value bytes.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CrdtMutation LwwRegisterSet(ReadOnlyMemory<byte> value) =>
        LwwRegisterSet(value, null);

    /// <summary>Creates a LWW register assignment.</summary>
    /// <param name="value">The register value bytes.</param>
    /// <param name="stamp">The optional authoritative write stamp.</param>
    /// <returns>The mutation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtMutation LwwRegisterSet(ReadOnlyMemory<byte> value, ConflictWriteStamp? stamp) =>
        new() { Kind = CrdtMutationKind.LwwRegisterSet, Bytes = value, RegisterStamp = stamp };
}
