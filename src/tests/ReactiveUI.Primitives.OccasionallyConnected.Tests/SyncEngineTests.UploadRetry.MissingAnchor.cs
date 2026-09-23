// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Durable retry-anchor failure tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a corrupt persisted policy releases its lease before any remote attempt begins.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRejectsInvalidLeasedPolicyBeforeRemoteEffects()
    {
        var operation = CreateOperation() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = (DeliveryGuarantee)int.MaxValue },
        };
        var store = CreateUploadStore([operation]);
        var session = CreateBatchSession(ExpectedSingleOperation);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var subscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode));

        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.BarrierCalls).IsEqualTo(0);
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches).IsEmpty();
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
        await Assert.That(faults.Values[0].Exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(faults.Values[0].Exception?.Message).Contains(nameof(ArgumentException));
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies loss of an exactly-once retry anchor after send faults without resending ambiguous work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDoesNotRetryExactlyOnceWhenAnchorDisappearsAfterSend()
    {
        var operation = CreateExactlyOnceOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateExactlyOnceCapabilities(),
            OnSend = () =>
            {
                store.RetryStates.Clear();
                throw CreateTransportFailure(RetryFailureKind.Transient);
            },
        };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var subscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode));

        await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.RetryStates).IsEmpty();
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await engine.StopAsync(CancellationToken.None);
    }
}
