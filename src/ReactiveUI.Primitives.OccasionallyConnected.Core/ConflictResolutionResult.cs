// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a canonical conflict decision and the events committed with its effects.</summary>
[System.Diagnostics.DebuggerDisplay("{ServerVersion,nq} {AcceptedOperations.Count}")]
public sealed record ConflictResolutionResult
{
    /// <summary>Initializes a new instance of the <see cref="ConflictResolutionResult"/> class.</summary>
    /// <param name="acceptedOperations">The accepted operation identifiers.</param>
    /// <param name="rejectedOperations">The rejected operations and stable reasons.</param>
    /// <param name="conflicts">The resolved conflict decisions.</param>
    /// <param name="producedEvents">The canonical events produced by the decision.</param>
    /// <param name="serverVersion">The resulting canonical server version.</param>
    public ConflictResolutionResult(
        IReadOnlyList<OperationId> acceptedOperations,
        IReadOnlyList<RejectedOperation> rejectedOperations,
        IReadOnlyList<ResolvedConflict> conflicts,
        IReadOnlyList<RemoteEvent> producedEvents,
        string serverVersion)
    {
        AcceptedOperations = CollectionCopy.List(acceptedOperations);
        RejectedOperations = CollectionCopy.List(rejectedOperations);
        Conflicts = CollectionCopy.List(conflicts);
        ProducedEvents = CollectionCopy.List(producedEvents);
        ServerVersion = serverVersion;
    }

    /// <summary>Gets the accepted operation identifiers.</summary>
    public IReadOnlyList<OperationId> AcceptedOperations { get; }

    /// <summary>Gets rejected operations with their stable reasons.</summary>
    public IReadOnlyList<RejectedOperation> RejectedOperations { get; }

    /// <summary>Gets the resolved conflict decisions.</summary>
    public IReadOnlyList<ResolvedConflict> Conflicts { get; }

    /// <summary>Gets the events committed with the canonical effects.</summary>
    public IReadOnlyList<RemoteEvent> ProducedEvents { get; }

    /// <summary>Gets the canonical server version following the decision.</summary>
    public string ServerVersion { get; }
}
