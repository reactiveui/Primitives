// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes an immutable client-originated mutation stored in the local outbox.</summary>
[DebuggerDisplay("{OperationId,nq} {Type,nq}")]
public sealed record SyncOperation
{
    /// <summary>Gets the default policy assigned to operations that do not override synchronization behavior.</summary>
    public static OperationPolicy DefaultPolicy => OperationPolicy.Default;

    /// <summary>Gets the idempotency identifier for this operation.</summary>
    public required OperationId OperationId { get; init; }

    /// <summary>Gets the target stream identifier.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the per-client, per-stream operation sequence.</summary>
    public required long ClientSequence { get; init; }

    /// <summary>Gets the client timestamp recorded for diagnostics and conflict metadata.</summary>
    public required DateTimeOffset TimestampUtc { get; init; }

    /// <summary>Gets the optional server version this operation was based on.</summary>
    public string? BaseVersion { get; init; }

    /// <summary>Gets the operation type.</summary>
    public required SyncOperationType Type { get; init; }

    /// <summary>Gets the serialized payload envelope.</summary>
    public required PayloadEnvelope Payload { get; init; }

    /// <summary>Gets the effective delivery, durability, priority, and conflict policy persisted with the operation.</summary>
    public OperationPolicy Policy { get; init; } = OperationPolicy.Default;

    /// <summary>Gets the operation metadata.</summary>
    public IReadOnlyDictionary<string, string> Metadata
    {
        get;
        init => field = CollectionCopy.Dictionary(value);
    } = CollectionCopy.Dictionary(new Dictionary<string, string>());
}
