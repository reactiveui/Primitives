// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests prepared upload size handling for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies exact prepared encoding overflow retries a smaller prefix before the send barrier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OversizedPreparedMultiOperationBatchRetriesSmallerPrefix()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStore([first, second], timeProvider: clock);
        store.Leases.Enqueue(CreateLease([first], clock));
        var session = new PreparedSession(maximumBatchOperations: ExpectedTwoOperations, maximumBatchBytes: PreparedUploadBytes)
        {
            EncodedSizes = [OversizedPreparedBytes, PreparedUploadBytes],
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
        };
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateBatchOptions(maximumOperations: ExpectedTwoOperations),
            timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);

        await Assert.That(store.LeaseRequests[0].MaximumOperations).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(store.LeaseRequests[1].MaximumOperations).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.PreparedBatches[1].Operations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies exact prepared encoding overflow dead-letters a single oversized head before retrying a barrier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OversizedPreparedSingleOperationHeadIsDeadLetteredBeforeSecondBarrier()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        store.Leases.Enqueue(CreateLease([operation]));
        var participant = new RecordingParticipant();
        var session = new PreparedSession(maximumBatchOperations: ExpectedSingleOperation, maximumBatchBytes: PreparedUploadBytes) { EncodedSizes = [OversizedPreparedBytes] };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, CreateBatchOptions(maximumOperations: ExpectedSingleOperation));
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None);

        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(participant.DeadLetterCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(participant.LastDeadLetterOperationId).IsEqualTo(operation.OperationId);
    }
}
