// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Post-connect shared-session renewal failure tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>The fault code emitted when a shared session renewal fails.</summary>
    private const string SessionRenewalFaultCode = "OC.Engine.SessionRenewal";

    /// <summary>Verifies a connected renewal candidate is disposed when current-session validation fails.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptDisposesRenewalCandidateAfterCurrentSessionCapabilityReadFails()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var candidateReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expiredInner = CreateExpiredUploadSession();
        var expiredSession = new CapabilityThrowingAfterRenewalCandidateSession(expiredInner, candidateReturned.Task);
        var candidate = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(expiredSession);
        innerTransport.Sessions.Enqueue(candidate);
        var transport = new RenewalCancellationTrackingTransport(innerTransport, candidateReturned);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngineWithStoreAndTransportAdapter(store, transport, CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes), clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            engine.NotifyLocalCommitReady(Stream, operation);
            await DriveUploadDwellWithTraceAsync(clock, store, expiredInner, faults, operationStates: null);
            await expiredSession.ThrowingCapabilityReadEntered.Task.WaitAsync(GuardTimeout);
            expiredSession.ReleaseThrowingCapabilityRead.SetResult();
            await WaitForConditionAsync(() => candidate.DisposeCalls == ExpectedSingleOperation);
            await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == SessionRenewalFaultCode)
                && faults.Values.Exists(static fault => fault.Code == UploadAttemptFaultCode));

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(candidate.SentBatches).IsEmpty();
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(transport.RenewalCancellationObserved.Task.IsCompleted).IsFalse();
        }
        finally
        {
            _ = expiredSession.ReleaseThrowingCapabilityRead.TrySetResult();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
    }
}
