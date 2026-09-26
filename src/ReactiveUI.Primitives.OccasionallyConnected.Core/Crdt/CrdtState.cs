// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Stores the complete public state and metadata for one built-in CRDT stream.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} Value = {Value}")]
#if NET11_0_OR_GREATER
public sealed record CrdtState : IUnion
#else
public sealed record CrdtState
#endif
{
#if NET11_0_OR_GREATER
    object IUnion.Value => this;
#endif
    /// <summary>The owned LWW register bytes.</summary>
    private readonly byte[] _registerValue = [];

    /// <summary>Gets the built-in CRDT state family.</summary>
    public required CrdtKind Kind { get; init; }

    /// <summary>Gets grow-only counter components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> GCounterComponents
    {
        get;
        init => field = CrdtCopy.StringLongDictionary(value);
    } = CrdtCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets PN-counter positive components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> PNCounterPositiveComponents
    {
        get;
        init => field = CrdtCopy.StringLongDictionary(value);
    } = CrdtCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets PN-counter negative components by authenticated client id.</summary>
    public IReadOnlyDictionary<string, long> PNCounterNegativeComponents
    {
        get;
        init => field = CrdtCopy.StringLongDictionary(value);
    } = CrdtCopy.StringLongDictionary(new Dictionary<string, long>());

    /// <summary>Gets retained OR-set dot-to-element bindings.</summary>
    public IReadOnlyList<CrdtDotElement> DotBindings
    {
        get;
        init => field = CrdtCopy.DotElementList(value);
    } = Array.Empty<CrdtDotElement>();

    /// <summary>Gets retained OR-set tombstoned dots bound to exact element bytes.</summary>
    public IReadOnlyList<CrdtDotElement> Tombstones
    {
        get;
        init => field = CrdtCopy.DotElementList(value);
    } = Array.Empty<CrdtDotElement>();

    /// <summary>Gets the LWW register bytes.</summary>
    public ReadOnlyMemory<byte> RegisterValue
    {
        get => _registerValue.AsSpan().ToArray();
        init => _registerValue = CrdtCopy.RegisterBytes(value);
    }

    /// <summary>Gets the LWW register byte count without allocating.</summary>
    public int RegisterValueLength => _registerValue.Length;

    /// <summary>Gets the authoritative LWW server write stamp when one is known.</summary>
    public ConflictWriteStamp? RegisterStamp { get; init; }

    /// <summary>Gets the canonical user value derived from the stored metadata.</summary>
    public CrdtValue Value => CrdtFunctions.GetValue(this);

    /// <summary>Gets the internal owned register bytes without allocating.</summary>
    internal ReadOnlySpan<byte> RegisterValueSpan => _registerValue;
}
