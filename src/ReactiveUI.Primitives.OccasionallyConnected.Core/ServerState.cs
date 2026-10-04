// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes server state used by conflict resolution.</summary>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="Version">The server version.</param>
/// <param name="State">The serialized server state.</param>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {Version,nq}")]
public sealed record ServerState(
    StreamId StreamId,
    string Version,
    PayloadEnvelope State);
