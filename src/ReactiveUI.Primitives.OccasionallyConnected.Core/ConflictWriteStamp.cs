// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a write using server-owned provenance.</summary>
/// <remarks>
/// The timestamp is assigned by the server and is never derived from a client operation timestamp.
/// Constructing this record does not authenticate its contents; the server must supply trusted values.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{ClientId,nq} {OperationId,nq}")]
public sealed record ConflictWriteStamp
{
    /// <summary>Gets the server-assigned logical timestamp shared by preparation and committed effects.</summary>
    public required DateTimeOffset CommittedAtUtc { get; init; }

    /// <summary>Gets the authenticated client identifier bound by the server.</summary>
    public required string ClientId { get; init; }

    /// <summary>Gets the operation identifier associated with the write.</summary>
    public required OperationId OperationId { get; init; }
}
