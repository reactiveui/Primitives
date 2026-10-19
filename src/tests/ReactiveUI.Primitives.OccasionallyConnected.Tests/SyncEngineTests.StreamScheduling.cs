// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Stream scheduling tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a global restart leaves an inactive stream's deferred upload parked.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GlobalRestartKeepsInactiveStreamUploadDeferredUntilStreamStarts()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var firstSession = CreateBatchSession(ExpectedSingleOperation);
        var secondSession = CreateBatchSession(ExpectedSingleOperation);
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        await engine.StopStreamAsync(Stream, CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await engine.StopAsync(CancellationToken.None);
        await engine.StartAsync(CancellationToken.None);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        clock.Advance(options.Batching.MaximumDwellTime);
        await Assert.That(firstSession.PrepareCalls).IsEqualTo(0);
        await Assert.That(secondSession.PrepareCalls).IsEqualTo(0);
        await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.QueuedForUpload);

        await engine.StartStreamAsync(Stream, CancellationToken.None);
        await WaitForConditionAsync(
            () => secondSession.SentBatches.Count == ExpectedSingleOperation
                || clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
        if (secondSession.SentBatches.Count == 0)
        {
            clock.Advance(options.Batching.MaximumDwellTime);
        }

        await WaitForConditionAsync(() => secondSession.SentBatches.Count == ExpectedSingleOperation);
        await Assert.That(secondSession.SentBatches[0].Operations[0]).IsSameReferenceAs(operation);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies stopping a stream parks a pending reschedule while its current upload is in flight.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamParksRescheduleRequestedDuringInflightUpload()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStoreWithSingleOperationLeases([first, second], timeProvider: clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));

        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, first);
        await WaitForConditionAsync(() => HasUploadDwellProgress(clock, session, faults: null));
        AdvanceUploadDwellIfStillPending(clock, session, faults: null);
        await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            engine.NotifyLocalCommitReady(Stream, second);
            await engine.StopStreamAsync(Stream, CancellationToken.None);
            session.ReleasePausedSendAttempt();
            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);

            await Assert.That(store.Statuses[second.OperationId].State).IsEqualTo(SyncOperationState.QueuedForUpload);
            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await WaitForConditionAsync(
                () => session.SentBatches.Count == ExpectedTwoOperations
                    || clock.HasTimerDueIn(options.Batching.MaximumDwellTime));
            if (session.SentBatches.Count != ExpectedTwoOperations)
            {
                clock.Advance(options.Batching.MaximumDwellTime);
            }

            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedTwoOperations);
            await Assert.That(session.SentBatches[ExpectedSingleOperation].Operations[0]).IsSameReferenceAs(second);
        }
        finally
        {
            session.ReleasePausedSendAttempt();
        }

        await engine.StopAsync(CancellationToken.None);
    }
}
