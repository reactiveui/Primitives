// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics integration test helpers for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The placeholder used when a trace value is absent.</summary>
    private const string MissingTraceValue = "<none>";

    /// <summary>Creates the diagnostics memory store with the shared manual clock.</summary>
    /// <param name="clock">The shared clock.</param>
    /// <returns>The diagnostics store.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static InMemoryLocalStoreAdapter CreateDiagnosticsMemoryStore(TimeProvider clock) =>
        new(
            clock,
            maximumRecordCount: DiagnosticsStoreBytes,
            maximumEncodedBytes: DiagnosticsStoreBytes,
            retentionOptions: new());

    /// <summary>Publishes a counter operation with the volatile diagnostics fixture policy.</summary>
    /// <param name="stream">The stream.</param>
    /// <returns>The publish receipt task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<PublishReceipt> PublishVolatileCounterAsync(
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream) =>
        stream.PublishAsync(new(DiagnosticsLocalCounter), CreateVolatilePublishOptions(), CancellationToken.None);

    /// <summary>Creates an initialized diagnostics SQLite store with durable outbox support.</summary>
    /// <returns>The initialized diagnostics store.</returns>
    private static async ValueTask<SqliteLocalStoreAdapter> CreateDiagnosticsSqliteStoreAsync()
    {
        var path = Path.Combine(Directory.CreateTempSubdirectory("oc-engine-diagnostics-").FullName, "local.db");
        var store = new SqliteLocalStoreAdapter(path);
        await store.InitializeAsync(
            new("sync-engine-tests", RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            CancellationToken.None);
        return store;
    }

    /// <summary>Creates an upload session that forces a single prepared operation into dead-letter handling.</summary>
    /// <returns>The prepared session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PreparedSession CreateOversizedDiagnosticsSession() =>
        new(ExpectedSingleOperation, PreparedUploadBytes) { EncodedSizes = [OversizedPreparedBytes], PauseBeforeSendNumber = ExpectedSingleOperation };

    /// <summary>Creates receive retry options for acknowledgement-failure diagnostics tests.</summary>
    /// <returns>The retry options.</returns>
    private static OccasionallyConnectedOptions CreateReceiveRetryOptions() =>
        OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };

    /// <summary>Creates a receive batch that completes one local operation without adding remote events.</summary>
    /// <param name="operationId">The completed local operation identity.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateCompletedReceiveBatch(OperationId operationId)
    {
        var completion = new RemoteOperationCompletion(
            new(EngineClientId, operationId),
            []);
        return new(Guid.NewGuid(), Stream, ReceiveCursor, "receive-cursor-completed", []) { CompletedOperations = [completion] };
    }

    /// <summary>Runs a trigger after observing upload progress or the dwell timer.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="store">The upload store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The synchronization task.</returns>
    /// <exception cref="TimeoutException">The trigger did not reach the dwell timer or completion.</exception>
    private static async Task TriggerAndDrainUploadWithTraceAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        var dwell = TimeSpan.FromMilliseconds(ExpectedSingleOperation);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        try
        {
            await WaitForConditionAsync(() => sync.IsCompleted || clock.HasTimerDueIn(dwell));
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates), exception);
        }

        if (!sync.IsCompleted)
        {
            clock.Advance(dwell);
        }

        await AwaitSyncWithUploadTraceAsync(sync, store, session, faults, operationStates);
    }

    /// <summary>Runs a real-store trigger with session/fault/status timeout trace.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The synchronization task.</returns>
    /// <exception cref="TimeoutException">The trigger did not reach the dwell timer or completion.</exception>
    private static async Task TriggerAndDrainUploadWithTraceAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        var dwell = TimeSpan.FromMilliseconds(ExpectedSingleOperation);
        var sync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        try
        {
            await WaitForConditionAsync(() => sync.IsCompleted || clock.HasTimerDueIn(dwell));
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(session, faults, operationStates), exception);
        }

        if (!sync.IsCompleted)
        {
            clock.Advance(dwell);
        }

        await AwaitSyncWithUploadTraceAsync(sync, session, faults, operationStates);
    }

    /// <summary>Advances the upload dwell timer after observing pending upload work in a recording store fixture.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="store">The upload store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">Upload dwell progress is not observed before the guard timeout.</exception>
    private static async Task DriveUploadDwellWithTraceAsync(
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        try
        {
            await WaitForConditionAsync(() => HasUploadDwellProgress(clock, session, faults));
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates), exception);
        }

        AdvanceUploadDwellIfStillPending(clock, session, faults);
    }

    /// <summary>Awaits an upload condition while preserving recording-store diagnostics on timeout.</summary>
    /// <param name="condition">The observed condition.</param>
    /// <param name="store">The upload store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The condition was not observed before the guard timeout.</exception>
    private static async Task WaitForUploadConditionWithTraceAsync(
        Func<bool> condition,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        try
        {
            await WaitForConditionAsync(condition);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates), exception);
        }
    }

    /// <summary>Awaits a durable retry anchor while preserving upload trace on timeout.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The retry state.</returns>
    /// <exception cref="TimeoutException">The retry anchor was not observed before the guard timeout.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<RetryState> WaitForRetryAnchorWithTraceAsync(
        ILocalStoreAdapter store,
        OperationId operationId,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates) =>
        WaitForRetryStateWithTraceAsync(store, operationId, session, faults, operationStates, static _ => true);

    /// <summary>Awaits a durable retry state matching a predicate while preserving upload trace on timeout.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <param name="predicate">The retry-state predicate.</param>
    /// <returns>The retry state.</returns>
    /// <exception cref="TimeoutException">The retry state was not observed before the guard timeout.</exception>
    private static async Task<RetryState> WaitForRetryStateWithTraceAsync(
        ILocalStoreAdapter store,
        OperationId operationId,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates,
        Func<RetryState, bool> predicate)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        Exception? lastFailure = null;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            try
            {
                var retryState = await store.GetRetryStateAsync(operationId, CancellationToken.None).ConfigureAwait(false);
                if (retryState is not null && predicate(retryState))
                {
                    return retryState;
                }
            }
            catch (Exception exception)
            {
                lastFailure = exception;
            }

            await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException(CreateUploadTrace(session, faults, operationStates), lastFailure);
    }

    /// <summary>Determines whether the upload fixture has reached send progress, a terminal fault, or dwell.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <returns>True when upload progress is observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool HasUploadDwellProgress(
        ManualTimerTimeProvider clock,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults) =>
        session.PrepareCalls > 0
        || session.SentBatches.Count > 0
        || (faults?.Values.Count ?? 0) > 0
        || clock.HasTimerDueIn(TimeSpan.FromMilliseconds(ExpectedSingleOperation));

    /// <summary>Advances the manual upload dwell timer when no later upload phase has begun.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AdvanceUploadDwellIfStillPending(
        ManualTimerTimeProvider clock,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults)
    {
        if (session.PrepareCalls != 0
            || session.SentBatches.Count != 0
            || (faults?.Values.Count ?? 0) != 0
            || !clock.HasTimerDueIn(TimeSpan.FromMilliseconds(ExpectedSingleOperation)))
        {
            return;
        }

        clock.Advance(TimeSpan.FromMilliseconds(ExpectedSingleOperation));
    }

    /// <summary>Asserts dead-letter race state before the second operation is allowed to upload.</summary>
    /// <param name="store">The store.</param>
    /// <param name="receipt">The second publish receipt.</param>
    /// <param name="metrics">The metric capture.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertTypedDeadLetterPausedRaceOutcomeAsync(
        InMemoryLocalStoreAdapter store,
        PublishReceipt receipt,
        EngineMetricCapture metrics)
    {
        var recovery = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(recovery.DeadLetters.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metrics.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Asserts the durable and diagnostic outcome after the typed reconciliation race.</summary>
    /// <param name="sync">The synchronization task.</param>
    /// <param name="store">The store.</param>
    /// <param name="receipt">The second publish receipt.</param>
    /// <param name="metrics">The metric capture.</param>
    /// <param name="faults">The fault observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertTypedReconciliationRaceOutcomeAsync(
        Task sync,
        InMemoryLocalStoreAdapter store,
        PublishReceipt receipt,
        EngineMetricCapture metrics,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        if (sync.IsFaulted)
        {
            await sync.ConfigureAwait(false);
        }

        var recovery = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(metrics.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedSingleOperation);
        var faultTrace = string.Join(",", faults.Values.Select(static item => item.Code));
        await Assert.That(faultTrace).IsEqualTo(string.Empty);
    }

    /// <summary>Advances an exact dwell timer when a race test is waiting on a later upload phase.</summary>
    /// <param name="sync">The synchronization task.</param>
    /// <param name="observed">The phase task being awaited.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="dwell">The expected dwell duration.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">Neither the phase nor dwell timer was observed before the guard.</exception>
    private static async Task AdvanceDwellIfUploadRaceIsWaitingAsync(
        Task sync,
        Task observed,
        ManualTimerTimeProvider clock,
        TimeSpan dwell)
    {
        await WaitForConditionAsync(
            () => observed.IsCompleted || sync.IsCompleted || clock.HasTimerDueIn(dwell))
            .WaitAsync(GuardTimeout);
        if (observed.IsCompleted || sync.IsCompleted)
        {
            return;
        }

        clock.Advance(dwell);
    }

    /// <summary>Awaits a real-store trigger while preserving upload state in timeout failures.</summary>
    /// <param name="sync">The trigger task.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The synchronization task.</returns>
    /// <exception cref="TimeoutException">The trigger did not complete before the guard.</exception>
    private static async Task AwaitSyncWithUploadTraceAsync(
        Task sync,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates = null)
    {
        try
        {
            await sync.WaitAsync(GuardTimeout);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(session, faults, operationStates), exception);
        }
    }

    /// <summary>Awaits a trigger while preserving upload state in timeout failures.</summary>
    /// <param name="sync">The trigger task.</param>
    /// <param name="store">The upload store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The synchronization task.</returns>
    /// <exception cref="TimeoutException">The trigger did not complete before the guard.</exception>
    private static async Task AwaitSyncWithUploadTraceAsync(
        Task sync,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates = null)
    {
        try
        {
            await sync.WaitAsync(GuardTimeout);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateUploadTrace(store, session, faults, operationStates), exception);
        }
    }

    /// <summary>Awaits receive retry timer while preserving subscription state in timeout failures.</summary>
    /// <param name="session">The receive session.</param>
    /// <param name="projection">The projection under observation.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="faults">The fault observer.</param>
    /// <param name="operationStates">The operation status observer.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The receive retry timer was not armed before the guard.</exception>
    private static async Task AwaitReceiveRetryTimerWithTraceAsync(
        ReceiveSession session,
        ReceiveCounterProjection projection,
        ManualTimerTimeProvider clock,
        RecordingObserver<OccasionallyConnectedFault> faults,
        RecordingObserver<SyncOperationStatus> operationStates)
    {
        try
        {
            await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException(CreateReceiveTrace(session, projection, clock, faults, operationStates), exception);
        }
    }

    /// <summary>Creates a compact receive trace for focused test timeout failures.</summary>
    /// <param name="session">The receive session.</param>
    /// <param name="projection">The projection under observation.</param>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="faults">The fault observer.</param>
    /// <param name="operationStates">The operation status observer.</param>
    /// <returns>The trace text.</returns>
    private static string CreateReceiveTrace(
        ReceiveSession session,
        ReceiveCounterProjection projection,
        ManualTimerTimeProvider clock,
        RecordingObserver<OccasionallyConnectedFault> faults,
        RecordingObserver<SyncOperationStatus> operationStates)
    {
        var request = session.SubscribeRequests.Count == 0 ? null : session.SubscribeRequests[0];
        var cursor = request?.Cursor ?? "<null>";
        var position = request?.InitialPosition.ToString() ?? MissingTraceValue;
        var faultTrace = string.Join(",", faults.Values.Select(static item => item.Code));
        var faultDetails = string.Join(",", faults.Values.Select(CreateFaultDetail));
        var stateTrace = string.Join(",", operationStates.Values.Select(static item => item.State));
        return $"subscribe={session.SubscribeRequests.Count};cursor={cursor};position={position};"
            + $"batches={session.Batches.Count};acks={session.Acknowledgements.Count};"
            + $"remoteApply={projection.RemoteApplyCalls};retry1s={clock.HasTimerDueIn(TimeSpan.FromSeconds(1))};"
            + $"faults={faultTrace};faultDetails={faultDetails};states={stateTrace}";
    }

    /// <summary>Creates a compact sanitized fault detail for receive timeout traces.</summary>
    /// <param name="fault">The fault.</param>
    /// <returns>The sanitized detail.</returns>
    private static string CreateFaultDetail(OccasionallyConnectedFault fault)
    {
        var exception = fault.Exception;
        if (exception is null)
        {
            return MissingTraceValue;
        }

        var inner = exception.InnerException?.GetType().Name ?? MissingTraceValue;
        return $"{exception.GetType().Name}:{exception.Message}:inner={inner}";
    }

    /// <summary>Creates a compact upload trace for real-store focused test timeout failures.</summary>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The trace text.</returns>
    private static string CreateUploadTrace(
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        var faultTrace = faults is null
            ? string.Empty
            : string.Join(",", faults.Values.Select(static item => item.Code));
        var stateTrace = operationStates is null
            ? string.Empty
            : string.Join(",", operationStates.Values.Select(static item => item.State));
        return $"prepare={session.PrepareCalls};sent={session.SentBatches.Count};"
            + $"faults={faultTrace};states={stateTrace}";
    }

    /// <summary>Creates a compact upload trace for focused test timeout failures.</summary>
    /// <param name="store">The upload store.</param>
    /// <param name="session">The prepared session.</param>
    /// <param name="faults">The optional fault capture.</param>
    /// <param name="operationStates">The optional operation-state capture.</param>
    /// <returns>The trace text.</returns>
    private static string CreateUploadTrace(
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault>? faults,
        RecordingObserver<SyncOperationStatus>? operationStates)
    {
        var faultTrace = faults is null
            ? string.Empty
            : string.Join(",", faults.Values.Select(static item => item.Code));
        var faultDetails = faults is null
            ? string.Empty
            : string.Join(",", faults.Values.Select(CreateFaultDetail));
        var stateTrace = operationStates is null
            ? string.Empty
            : string.Join(",", operationStates.Values.Select(static item => item.State));
        return $"leases={store.LeaseRequests.Count};barriers={store.BarrierCalls};"
            + $"releases={store.ReleaseLeaseCalls};prepare={session.PrepareCalls};"
            + $"sent={session.SentBatches.Count};faults={faultTrace};"
            + $"faultDetails={faultDetails};states={stateTrace}";
    }
}
