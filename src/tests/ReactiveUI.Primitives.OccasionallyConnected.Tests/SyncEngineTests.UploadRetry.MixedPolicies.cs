// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Mixed delivery-policy upload tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a mixed lease uploads both operations under the stronger delivery policy in either order.</summary>
    /// <param name="exactlyOnceFirst">Whether the exactly-once operation is first in the lease.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task UploadAttemptAcceptsMixedLeaseWithExactlyOncePolicyInEitherOrder(bool exactlyOnceFirst)
    {
        var atMostOnce = CreateOperation(
            sequence: exactlyOnceFirst ? ExpectedTwoOperations : FirstSequence,
            operationId: OperationId.New()) with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
        };
        var exactlyOnce = CreateOperation(
            sequence: exactlyOnceFirst ? FirstSequence : ExpectedTwoOperations,
            operationId: OperationId.New()) with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        };
        SyncOperation[] operations = exactlyOnceFirst ? [exactlyOnce, atMostOnce] : [atMostOnce, exactlyOnce];
        var store = CreateUploadStore(operations);
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
            { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes) };
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateBatchOptions(ExpectedTwoOperations));
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(store.Statuses[atMostOnce.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(store.Statuses[exactlyOnce.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
        await engine.StopAsync(CancellationToken.None);
    }
}
