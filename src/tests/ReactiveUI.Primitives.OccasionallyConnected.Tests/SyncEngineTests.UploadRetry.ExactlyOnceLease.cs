// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Multiple exactly-once operations in one leased upload for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies each exactly-once operation receives a durable retry anchor before one shared batch send.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptPreparesRetryAnchorsForTwoExactlyOnceOperationsInOneLease()
    {
        var first = CreateExactlyOnceOperation();
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New()) with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        var store = CreateUploadStore([first, second]);
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
            { NegotiatedCapabilities = CreateExactlyOnceCapabilities() };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, CreateBatchOptions(ExpectedTwoOperations));
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.RetryStates.ContainsKey(first.OperationId)).IsTrue();
        await Assert.That(store.RetryStates.ContainsKey(second.OperationId)).IsTrue();
        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(store.Statuses[first.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(store.Statuses[second.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await engine.StopAsync(CancellationToken.None);
    }
}
