// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Stores the complete public state and metadata for one built-in CRDT stream.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} Value = {Value}")]
public sealed record MergeableState
{
    /// <summary>The owned LWW register bytes.</summary>
    private readonly byte[] _registerValue = [];

    /// <summary>Gets the built-in CRDT state family.</summary>
    public required MergeableKind Kind { get; init; }

    /// <summary>Gets grow-only counter components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> IncrementOnlyTotalsByClient
    {
        get;
        init => field = MergeableCopy.StringLongDictionary(value);
    } = MergeableCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets PN-counter positive components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> IncrementsByClient
    {
        get;
        init => field = MergeableCopy.StringLongDictionary(value);
    } = MergeableCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets PN-counter negative components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> DecrementsByClient
    {
        get;
        init => field = MergeableCopy.StringLongDictionary(value);
    } = MergeableCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets retained OR-set dot-to-element bindings.</summary>
    public IReadOnlyList<TaggedElement> Elements
    {
        get;
        init => field = MergeableCopy.DotElementList(value);
    } = Array.Empty<TaggedElement>();

    /// <summary>Gets retained OR-set tombstoned dots bound to exact element bytes.</summary>
    public IReadOnlyList<TaggedElement> RemovedTags
    {
        get;
        init => field = MergeableCopy.DotElementList(value);
    } = Array.Empty<TaggedElement>();

    /// <summary>Gets checkpointed, fully observed per-client sequence prefixes for this OR-set stream.</summary>
    /// <remarks>A missing binding at or below its client's prefix is removed. Never discard or decrease this knowledge.</remarks>
    public IReadOnlyDictionary<string, long> AddWinsSetFrontier
    {
        get;
        init => field = MergeableCopy.StringLongDictionary(value);
    } = MergeableCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets the LWW register bytes.</summary>
    public ReadOnlyMemory<byte> LatestValue
    {
        get => _registerValue.AsSpan().ToArray();
        init => _registerValue = MergeableCopy.RegisterBytes(value);
    }

    /// <summary>Gets the LWW register byte count without allocating.</summary>
    public int LatestValueLength => _registerValue.Length;

    /// <summary>Gets the authoritative LWW server write stamp when one is known.</summary>
    public ConflictWriteStamp? LatestWriteStamp { get; init; }

    /// <summary>Gets the canonical user value derived from the stored metadata.</summary>
    public MergeableValue Value => MergeableFunctions.GetValue(this);

    /// <summary>Gets the internal owned register bytes without allocating.</summary>
    internal ReadOnlySpan<byte> LatestValueSpan => _registerValue;
}
