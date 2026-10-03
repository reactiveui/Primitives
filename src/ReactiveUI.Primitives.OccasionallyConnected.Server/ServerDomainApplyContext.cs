// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides inputs to a server domain handler after conflict resolution.</summary>
[System.Diagnostics.DebuggerDisplay("{Operation.OperationId,nq} {Resolution.ServerVersion,nq}")]
public sealed record ServerDomainApplyContext
{
    /// <summary>Gets the authenticated client identity.</summary>
    public required ClientIdentity Client { get; init; }

    /// <summary>Gets the accepted operation.</summary>
    public required SyncOperation Operation { get; init; }

    /// <summary>Gets the conflict context observed by the resolver.</summary>
    public required ConflictContext Conflict { get; init; }

    /// <summary>Gets the validated conflict resolution.</summary>
    public required ConflictResolutionResult Resolution { get; init; }
}
