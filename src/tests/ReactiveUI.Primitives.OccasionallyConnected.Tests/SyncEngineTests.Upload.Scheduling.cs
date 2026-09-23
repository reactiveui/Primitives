// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Upload scheduling tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The initial denied lease, queued wake retry, and post-success empty probe lease.</summary>
    private const int ExpectedDeniedBarrierLeaseRequests = 3;

    /// <summary>Verifies repeated local wakes merge while an existing upload head is in flight.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InflightUploadMergesQueuedLocalWakesIntoNextScheduledHead()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var third = CreateOperation(sequence: ExpectedTwoOperations + 1, operationId: OperationId.New());
        var store = CreateUploadStore([first], timeProvider: clock);
        EnqueueQueuedOperations(store, clock, second, third);
        var options = CreateDiagnosticsBatchOptions(ExpectedTwoOperations, PreparedUploadBytes);
        var session = new PreparedSession(ExpectedTwoOperations, PreparedUploadBytes)
        {
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, PreparedUploadBytes),
            PauseBeforeSendNumber = ExpectedSingleOperation,
        };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            engine.NotifyLocalCommitReady(Stream, first);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults: null, operationStates: null);
            await session.PausedSendEntered.Task.WaitAsync(GuardTimeout);

            engine.NotifyLocalCommitReady(Stream, second);
            engine.NotifyLocalCommitReady(Stream, third);
            session.ReleasePausedSendAttempt();
            await WaitForUploadConditionWithTraceAsync(
                () => session.SentBatches.Count == ExpectedTwoOperations,
                store,
                session,
                faults: null,
                operationStates: null);

            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches[1].Operations.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.SentBatches[1].Operations[0]).IsSameReferenceAs(second);
            await Assert.That(session.SentBatches[1].Operations[1]).IsSameReferenceAs(third);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(0);
        }
        finally
        {
            session.ReleasePausedSendAttempt();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies a local wake queued during a denied upload attempt is scheduled after the attempt releases ownership.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DeniedInflightUploadSchedulesQueuedWakeWithoutRetryReschedule()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var barrierEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = CreateOperation(operationId: OperationId.New());
        var second = CreateOperation(sequence: ExpectedTwoOperations, operationId: OperationId.New());
        var store = CreateUploadStore([first], timeProvider: clock);
        store.DenyRemoteAttemptOnCall = ExpectedSingleOperation;
        store.BarrierEntered = barrierEntered;
        store.ReleaseBarrier = releaseBarrier;
        EnqueueQueuedOperations(store, clock, second);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            engine.NotifyLocalCommitReady(Stream, first);
            await DriveUploadDwellWithTraceAsync(clock, store, session, faults: null, operationStates: null);
            await barrierEntered.Task.WaitAsync(GuardTimeout);
            engine.NotifyLocalCommitReady(Stream, second);
            _ = releaseBarrier.TrySetResult();
            await AdvanceDwellAfterDeniedAttemptReleasesAsync(clock, store, session, options);
            await Assert.That(session.PreparedBatches.Count).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SentBatches[0].Operations.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.PreparedBatches[0].Operations[0]).IsSameReferenceAs(first);
            await Assert.That(session.SentBatches[0].Operations[0]).IsSameReferenceAs(second);
            await Assert.That(store.BarrierCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.LeaseRequests.Count).IsEqualTo(ExpectedDeniedBarrierLeaseRequests);
        }
        finally
        {
            _ = releaseBarrier.TrySetResult();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Waits for denied-attempt cleanup and advances the pending local wake dwell.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AdvanceDwellAfterDeniedAttemptReleasesAsync(
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        OccasionallyConnectedOptions options)
    {
        await WaitForUploadConditionWithTraceAsync(
            () => store.ReleaseLeaseCalls == ExpectedSingleOperation
                && clock.HasTimerDueIn(options.Batching.MaximumDwellTime),
            store,
            session,
            faults: null,
            operationStates: null);
        await Assert.That(session.SentBatches.Count).IsEqualTo(0);
        clock.Advance(options.Batching.MaximumDwellTime);
        await WaitForUploadConditionWithTraceAsync(
            () => session.SentBatches.Count == ExpectedSingleOperation
                && store.LeaseRequests.Count == ExpectedDeniedBarrierLeaseRequests,
            store,
            session,
            faults: null,
            operationStates: null);
    }

    /// <summary>Adds queued leases and status rows to an upload store fixture.</summary>
    /// <param name="store">The store to update.</param>
    /// <param name="clock">The lease clock.</param>
    /// <param name="operations">The queued operations.</param>
    private static void EnqueueQueuedOperations(RecordingStore store, TimeProvider clock, params SyncOperation[] operations)
    {
        store.Leases.Enqueue(CreateLease(operations, clock));
        foreach (var operation in operations)
        {
            store.Statuses[operation.OperationId] = new(
                operation.OperationId,
                operation.StreamId,
                SyncOperationState.QueuedForUpload,
                0,
                DateTimeOffset.UnixEpoch,
                null);
        }
    }
}
