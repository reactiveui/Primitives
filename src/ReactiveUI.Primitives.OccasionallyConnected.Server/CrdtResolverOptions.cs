// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the built-in CRDT conflict resolver.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
public sealed record CrdtResolverOptions
{
    /// <summary>Gets the registered CRDT kind.</summary>
    public required CrdtKind Kind { get; init; }

    /// <summary>Gets the server version factory used for accepted operations.</summary>
    public required IServerConflictVersionFactory VersionFactory { get; init; }

    /// <summary>Gets the finite CRDT bounds.</summary>
    public CrdtBounds Bounds { get; init; } = CrdtBounds.Default;
}
