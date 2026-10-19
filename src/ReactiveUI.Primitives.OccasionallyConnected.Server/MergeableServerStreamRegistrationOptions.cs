// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures one built-in CRDT server stream registration.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {Kind,nq}")]
#if NET11_0_OR_GREATER
public sealed record MergeableServerStreamRegistrationOptions : System.Runtime.CompilerServices.IUnion
#else
public sealed record MergeableServerStreamRegistrationOptions
#endif
{
#if NET11_0_OR_GREATER
    object System.Runtime.CompilerServices.IUnion.Value => this;
#endif
    /// <summary>Gets the stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the registered CRDT kind.</summary>
    public required MergeableKind Kind { get; init; }

    /// <summary>Gets the server version factory.</summary>
    public IServerConflictVersionFactory VersionFactory { get; init; } = new MergeableSequentialVersionFactory();

    /// <summary>Gets the initial server version.</summary>
    public string InitialVersion { get; init; } = "crdt-v0";

    /// <summary>Gets the finite CRDT bounds.</summary>
    public MergeableBounds Bounds { get; init; } = MergeableBounds.Default;
}
