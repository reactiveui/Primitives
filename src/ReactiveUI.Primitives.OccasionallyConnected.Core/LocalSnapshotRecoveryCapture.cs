// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes an immutable local snapshot recovery capture.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed record LocalSnapshotRecoveryCapture
{
    /// <summary>Gets the captured stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the captured subscription identifier.</summary>
    public required SubscriptionId SubscriptionId { get; init; }

    /// <summary>Gets the captured durable server cursor.</summary>
    public string? ServerCursor { get; init; }

    /// <summary>Gets the captured local snapshot, if one exists.</summary>
    public LocalSnapshot? Snapshot
    {
        get;
        init => field = value is null ? null : OwnSnapshot(value);
    }

    /// <summary>Gets the next client sequence to assign after the capture point.</summary>
    public required long NextClientSequence { get; init; }

    /// <summary>Gets the bounded pending operations captured for remote recovery.</summary>
    public IReadOnlyList<SyncOperation> PendingOperations
    {
        get;
        init => field = OwnOperations(value, nameof(PendingOperations));
    } = [];

    /// <summary>Gets the bounded operations that must be replayed into local projection.</summary>
    public IReadOnlyList<SyncOperation> ReplayOperations
    {
        get;
        init => field = OwnOperations(value, nameof(ReplayOperations));
    } = [];

    /// <summary>Creates an owned snapshot copy.</summary>
    /// <param name="snapshot">The snapshot to copy.</param>
    /// <returns>The owned snapshot.</returns>
    private static LocalSnapshot OwnSnapshot(LocalSnapshot snapshot) =>
        new(
            snapshot.StreamId,
            snapshot.FormatVersion,
            snapshot.ServerCursor,
            OwnPayload(snapshot.State),
            snapshot.Revision,
            snapshot.SavedAtUtc) { AuthoritativeState = snapshot.AuthoritativeState is null ? null : OwnPayload(snapshot.AuthoritativeState) };

    /// <summary>Creates owned operation copies.</summary>
    /// <param name="operations">The operations to copy.</param>
    /// <param name="parameterName">The public parameter or property name.</param>
    /// <returns>The owned operations.</returns>
    private static System.Collections.ObjectModel.ReadOnlyCollection<SyncOperation> OwnOperations(IReadOnlyList<SyncOperation>? operations, string parameterName)
    {
        var copy = SnapshotRecoveryCollectionCopy.List(operations, parameterName);
        var owned = new SyncOperation[copy.Count];
        for (var index = 0; index < copy.Count; index++)
        {
            var operation = copy[index];
            owned[index] = operation with { Payload = OwnPayload(operation.Payload), Metadata = operation.Metadata };
        }

        return Array.AsReadOnly(owned);
    }

    /// <summary>Creates an owned payload envelope.</summary>
    /// <param name="payload">The payload to copy.</param>
    /// <returns>The owned payload.</returns>
    private static PayloadEnvelope OwnPayload(PayloadEnvelope payload) =>
        new(payload.ContractId, payload.SchemaVersion, payload.ContentType, payload.Payload, payload.PayloadHash);
}
