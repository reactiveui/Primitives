// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Configures sequential CRDT server versions.</summary>
[System.Diagnostics.DebuggerDisplay("{Prefix,nq}")]
public sealed record CrdtSequentialVersionFactoryOptions
{
    /// <summary>Gets the sequential version prefix.</summary>
    public string Prefix { get; init; } = "crdt-v";
}
