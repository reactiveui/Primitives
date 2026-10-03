// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Represents one CRDT stream input, either a local mutation or a complete authoritative state event.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
#if NET11_0_OR_GREATER
public sealed record CrdtInput : IUnion
#else
public sealed record CrdtInput
#endif
{
#if NET11_0_OR_GREATER
    object IUnion.Value => this;
#endif
    /// <summary>Gets the input kind.</summary>
    public required CrdtInputKind Kind { get; init; }

    /// <summary>Gets the local mutation when <see cref="Kind"/> is <see cref="CrdtInputKind.Mutation"/>.</summary>
    public CrdtMutation? Mutation { get; init; }

    /// <summary>Gets the complete authoritative state when <see cref="Kind"/> is <see cref="CrdtInputKind.AuthoritativeState"/>.</summary>
    public CrdtState? State { get; init; }

    /// <summary>Creates a mutation input.</summary>
    /// <param name="mutation">The mutation.</param>
    /// <returns>The CRDT input.</returns>
    public static CrdtInput ForMutation(CrdtMutation mutation) =>
        new() { Kind = CrdtInputKind.Mutation, Mutation = mutation };

    /// <summary>Creates a complete authoritative state input.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The CRDT input.</returns>
    public static CrdtInput ForAuthoritativeState(CrdtState state) =>
        new() { Kind = CrdtInputKind.AuthoritativeState, State = state };
}
