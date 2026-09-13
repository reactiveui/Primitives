// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides trusted server write provenance to a conflict resolver.</summary>
/// <remarks>
/// Both write stamps are assigned by the server. Client-supplied timestamps are not trusted provenance.
/// Candidate provenance describes one incoming operation; server resolvers validate its identity against the request.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{CandidateWrite.ClientId,nq} {CandidateWrite.OperationId,nq}")]
public sealed record ConflictServerContext
{
    /// <summary>Gets the server-assigned write stamp proposed for the incoming operation.</summary>
    public required ConflictWriteStamp CandidateWrite { get; init; }

    /// <summary>Gets the server-assigned write stamp for the current state, when a prior write exists.</summary>
    public ConflictWriteStamp? CurrentWrite { get; init; }
}
