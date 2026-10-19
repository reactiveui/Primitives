// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures the built-in CRDT server domain handler.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
public sealed record MergeableServerDomainHandlerOptions
{
    /// <summary>Gets the registered CRDT kind.</summary>
    public required MergeableKind Kind { get; init; }

    /// <summary>Gets the finite CRDT bounds.</summary>
    public MergeableBounds Bounds { get; init; } = MergeableBounds.Default;
}
