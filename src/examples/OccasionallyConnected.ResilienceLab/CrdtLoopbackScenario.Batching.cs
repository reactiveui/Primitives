// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Creates a one-operation CRDT batch.</summary>
    /// <param name="batchSeed">The deterministic batch id seed.</param>
    /// <param name="operationSeed">The deterministic operation id seed.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="sequence">The client stream sequence.</param>
    /// <param name="timestamp">The diagnostic client timestamp.</param>
    /// <param name="mutation">The CRDT mutation.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The synchronization batch.</returns>
    private static SyncBatch CreateOperationBatch(
        int batchSeed,
        int operationSeed,
        StreamId streamId,
        long sequence,
        DateTimeOffset timestamp,
        CrdtMutation mutation,
        CrdtBounds bounds) =>
        new(CreateGuid(batchSeed), [CreateOperation(operationSeed, streamId, sequence, timestamp, mutation, bounds)]);

    /// <summary>Creates a CRDT sync operation.</summary>
    /// <param name="operationSeed">The deterministic operation id seed.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="sequence">The client stream sequence.</param>
    /// <param name="timestamp">The diagnostic client timestamp.</param>
    /// <param name="mutation">The CRDT mutation.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The synchronization operation.</returns>
    private static SyncOperation CreateOperation(
        int operationSeed,
        StreamId streamId,
        long sequence,
        DateTimeOffset timestamp,
        CrdtMutation mutation,
        CrdtBounds bounds) =>
        new()
        {
            OperationId = new(CreateGuid(operationSeed)),
            StreamId = streamId,
            ClientSequence = sequence,
            TimestampUtc = timestamp,
            Type = SyncOperationType.Update,
            Payload = CrdtServerPayloads.CreateInput(CrdtInput.ForMutation(mutation), bounds),
            Policy = OperationPolicy.Default,
        };
}
