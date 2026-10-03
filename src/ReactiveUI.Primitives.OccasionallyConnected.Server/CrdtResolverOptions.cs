// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the built-in CRDT conflict resolver.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
#if NET11_0_OR_GREATER
public sealed record CrdtResolverOptions : System.Runtime.CompilerServices.IUnion
#else
public sealed record CrdtResolverOptions
#endif
{
#if NET11_0_OR_GREATER
    object System.Runtime.CompilerServices.IUnion.Value => this;
#endif
    /// <summary>Gets the registered CRDT kind.</summary>
    public required CrdtKind Kind { get; init; }

    /// <summary>Gets the server version factory used for accepted operations.</summary>
    public required IServerConflictVersionFactory VersionFactory { get; init; }

    /// <summary>Gets the finite CRDT bounds.</summary>
    public CrdtBounds Bounds { get; init; } = CrdtBounds.Default;
}
