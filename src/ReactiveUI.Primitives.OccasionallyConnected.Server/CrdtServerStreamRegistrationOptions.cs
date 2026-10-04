// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures one built-in CRDT server stream registration.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {Kind,nq}")]
#if NET11_0_OR_GREATER
public sealed record CrdtServerStreamRegistrationOptions : System.Runtime.CompilerServices.IUnion
#else
public sealed record CrdtServerStreamRegistrationOptions
#endif
{
#if NET11_0_OR_GREATER
    object System.Runtime.CompilerServices.IUnion.Value => this;
#endif
    /// <summary>Gets the stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the registered CRDT kind.</summary>
    public required CrdtKind Kind { get; init; }

    /// <summary>Gets the server version factory.</summary>
    public IServerConflictVersionFactory VersionFactory { get; init; } = new CrdtSequentialVersionFactory();

    /// <summary>Gets the initial server version.</summary>
    public string InitialVersion { get; init; } = "crdt-v0";

    /// <summary>Gets the finite CRDT bounds.</summary>
    public CrdtBounds Bounds { get; init; } = CrdtBounds.Default;
}
