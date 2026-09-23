// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests validation and revision ownership of recovered queue diagnostics.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a stream removed during metric delivery cannot receive a later queue state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnregisterDuringQueueMeasurementDoesNotPublishStaleStreamState()
    {
        IDisposable? registration = null;
        var unregistered = 0;
        using var listener = new MeterListener { InstrumentPublished = EnableEngineInstrument };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == QueuePendingMetricName && value == ExpectedSingleOperation && Interlocked.Exchange(ref unregistered, 1) == 0)
            {
                registration?.Dispose();
            }
        });
        listener.Start();

        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        var participant = new ThrowingStreamDiagnosticParticipant();
        registration = engine.RegisterParticipant(participant);
        var states = new RecordingObserver<SyncState>();
        using var stateSubscription = engine.SyncStates.Subscribe(states);

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));
        await WaitForConditionAsync(() => states.Values.Count > 0);

        await Assert.That(unregistered).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(participant.NotificationCount).IsEqualTo(0);
        await Assert.That(states.Values[^1].PendingOperations).IsEqualTo(0);
        registration.Dispose();
    }

    /// <summary>Verifies a throwing global observer and fault observer cannot stop later queue recording.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingGlobalDiagnosticAndFaultObserverDoNotInterruptLaterQueueRecording()
    {
        using var metrics = CreateEngineMetricListener(out var capture);
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var throwingFaultSubscription = engine.Faults.Subscribe(new ThrowingFaultObserver());
        using var throwingStateSubscription = engine.SyncStates.Subscribe(new ThrowingGlobalSyncStateObserver());
        var healthyStates = new RecordingObserver<SyncState>();
        using var healthyStateSubscription = engine.SyncStates.Subscribe(healthyStates);

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));
        await WaitForConditionAsync(() => faults.Values.Count >= ExpectedSingleOperation);
        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, ExpectedTwoOperations));
        await WaitForConditionAsync(() => healthyStates.Values.Exists(static state => state.PendingOperations == ExpectedTwoOperations));

        await Assert.That(faults.Values.Count).IsGreaterThanOrEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.TrueForAll(static fault => fault.Code == "OC.Engine.SyncStateObserver")).IsTrue();
        await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedTwoOperations);
    }

    /// <summary>Verifies stream diagnostics and a throwing fault observer cannot fail committed queue recording.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingStreamDiagnosticAndFaultObserverDoNotInterruptQueueRecording()
    {
        using var metrics = CreateEngineMetricListener(out var capture);
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        var participant = new ThrowingStreamDiagnosticParticipant();
        using var registration = engine.RegisterParticipant(participant);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var throwingSubscription = engine.Faults.Subscribe(new ThrowingFaultObserver());

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));
        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, ExpectedTwoOperations));

        await Assert.That(participant.NotificationCount).IsEqualTo(ExpectedTwoOperations);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedTwoOperations);
        await Assert.That(faults.Values.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(faults.Values.TrueForAll(static fault => fault.Code == "OC.Engine.StreamSyncState")).IsTrue();
        await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedTwoOperations);
    }

    /// <summary>Verifies a delayed older notification cannot follow a newer aggregate state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentQueueDiagnosticsPublishGlobalStatesInRevisionOrder()
    {
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var observer = new BlockingGlobalSyncStateObserver();
        using var subscription = engine.SyncStates.Subscribe(observer);
        var first = Task.Run(() =>
            engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision)));

        try
        {
            await observer.FirstEntered.Task.WaitAsync(GuardTimeout);
            engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, ExpectedTwoOperations));
        }
        finally
        {
            observer.ReleaseFirst.SetResult();
        }

        await first.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => observer.PendingCounts.Count >= ExpectedTwoOperations);
        var pendingCounts = observer.PendingCounts.ToArray();
        await Assert.That(pendingCounts[0]).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(pendingCounts[^1]).IsEqualTo(ExpectedTwoOperations);
    }

    /// <summary>Verifies an observer-triggered queue update follows the state that caused it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReentrantQueueDiagnosticsPublishGlobalStatesInRevisionOrder()
    {
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var observer = new ReentrantGlobalSyncStateObserver(() =>
            engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, ExpectedTwoOperations)));
        using var subscription = engine.SyncStates.Subscribe(observer);

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));
        await WaitForConditionAsync(() => observer.PendingCounts.Count >= ExpectedTwoOperations);

        var pendingCounts = observer.PendingCounts.ToArray();
        await Assert.That(pendingCounts[0]).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(pendingCounts[^1]).IsEqualTo(ExpectedTwoOperations);
    }

    /// <summary>Verifies invalid recovered aggregates cannot publish measurements or poison a subsequent valid snapshot.</summary>
    /// <param name="pendingOperations">The recovered operation count.</param>
    /// <param name="pendingBytes">The recovered retained byte count.</param>
    /// <param name="revision">The recovered diagnostic revision.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(-1L, 0L, 0L)]
    [Arguments(0L, -1L, 0L)]
    [Arguments(0L, 0L, -1L)]
    public async Task InvalidRecoveredQueueSnapshotDoesNotPublishOrPoisonLaterRecovery(long pendingOperations, long pendingBytes, long revision)
    {
        using var metrics = CreateEngineMetricListener(out var capture);
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var invalid = new QueueDiagnosticSnapshot(pendingOperations, pendingBytes, revision);
        var operation = CreateOperation();

        await Assert.That(() => engine.RecordRecoveredQueueAggregate(Stream, invalid)).ThrowsExactly<ArgumentOutOfRangeException>();
        var receipt = new PublishReceipt(operation.OperationId, operation.ClientSequence, SyncOperationState.SavedLocally, operation.TimestampUtc);
        await Assert.That(() => engine.RecordSavedLocalCommit(Stream, operation, invalid, receipt)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(capture.GetMeasurements()).IsEmpty();

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));

        await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(capture.Sum(QueueBytesMetricName)).IsEqualTo(PreparedUploadBytes);
    }

    /// <summary>Verifies stale recovery callbacks cannot restore queue measurements after a participant is removed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StaleRecoveredQueueSnapshotCannotOverwriteNewerOrUnregisteredAggregate()
    {
        using var metrics = CreateEngineMetricListener(out var capture);
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        using (engine.RegisterParticipant(new RecordingParticipant()))
        {
            engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedSingleOperation, PreparedUploadBytes, DiagnosticsRecoveredRevision));
            engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, DiagnosticsRecoveredRevision));
            engine.RecordRecoveredQueueAggregate(Stream, new(0, 0, 0));

            await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(capture.Sum(QueueBytesMetricName)).IsEqualTo(PreparedUploadBytes);
        }

        engine.RecordRecoveredQueueAggregate(Stream, new(ExpectedTwoOperations, DiagnosticsStoreBytes, ExpectedTwoOperations));

        await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(0);
        await Assert.That(capture.Sum(QueueBytesMetricName)).IsEqualTo(0);
    }

    /// <summary>Verifies a commit that completes after unregister does not restore queue diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InflightCommitAfterUnregisterCompletesWithoutQueueDiagnosticReintroduction()
    {
        using var metrics = CreateEngineMetricListener(out var capture);
        await using var engine = CreateEngine(options: CreateDiagnosticsOptions(enabled: true));
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        TaskCompletionSource commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var participant = new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit };
        var registration = engine.RegisterParticipant(participant);
        var disposeRegistration = true;
        var operation = CreateOperation();
        var publish = engine.EnqueueOperationAsync(operation, CancellationToken.None).AsTask();

        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);
            registration.Dispose();
            disposeRegistration = false;
            releaseCommit.SetResult();

            var receipt = await publish.WaitAsync(GuardTimeout);

            await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
            await Assert.That(participant.CommittedOperation).IsSameReferenceAs(operation);
            await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(0);
            await Assert.That(capture.Sum(QueueBytesMetricName)).IsEqualTo(0);
            await Assert.That(faults.Values).IsEmpty();
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
            if (disposeRegistration)
            {
                registration.Dispose();
            }

            await ObserveTaskCompletionAsync(publish).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies stale recovered work notifications cannot schedule upload after a stream is unregistered.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StaleRecoveredWorkNotificationDoesNotScheduleAfterUnregister()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = CreateBatchSession(ExpectedSingleOperation);
        var transport = new RecordingTransport { SessionOverride = session };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);
        registration.Dispose();

        engine.NotifyRecoveredLocalWorkReady(Stream, operation.Policy.Priority);
        using var replacement = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartStreamAsync(Stream, CancellationToken.None);

        await Assert.That(clock.HasTimerDueIn(options.Batching.MaximumDwellTime)).IsFalse();
        await Assert.That(store.LeaseRequests).IsEmpty();
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches).IsEmpty();
        await Assert.That(faults.Values).IsEmpty();
    }

    /// <summary>Verifies recovered work notifications after disposal do not schedule upload work or publish faults.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RecoveredWorkNotificationAfterDisposeDoesNotScheduleOrFault()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var operation = CreateOperation();
        var store = CreateUploadStore([operation], timeProvider: clock);
        var session = CreateBatchSession(ExpectedSingleOperation);
        var transport = new RecordingTransport { SessionOverride = session };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        await engine.StartAsync(CancellationToken.None);
        registration.Dispose();
        await engine.DisposeAsync();

        engine.NotifyRecoveredLocalWorkReady(Stream, operation.Policy.Priority);

        await Assert.That(clock.HasTimerDueIn(options.Batching.MaximumDwellTime)).IsFalse();
        await Assert.That(store.LeaseRequests).IsEmpty();
        await Assert.That(session.PrepareCalls).IsEqualTo(0);
        await Assert.That(session.SentBatches).IsEmpty();
        await Assert.That(faults.Values).IsEmpty();
    }
}
