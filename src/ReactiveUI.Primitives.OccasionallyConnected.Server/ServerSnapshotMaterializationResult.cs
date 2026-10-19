// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Reports the result of server snapshot materialization.</summary>
[System.Diagnostics.DebuggerDisplay("{Status,nq}")]
public sealed record ServerSnapshotMaterializationResult
{
    /// <summary>Gets the materialization status.</summary>
    public required ServerSnapshotMaterializationStatus Status { get; init; }

    /// <summary>Gets the materialized allowlisted client state when <see cref="Status"/> is materialized.</summary>
    public PayloadEnvelope? ClientState { get; init; }

    /// <summary>Gets an optional bounded stable reason code.</summary>
    public string? ReasonCode { get; init; }
}
