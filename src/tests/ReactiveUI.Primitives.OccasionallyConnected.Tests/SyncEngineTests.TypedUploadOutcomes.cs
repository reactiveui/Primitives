// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests durable terminal outcomes through the engine and typed stream facade.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a terminal rejection updates the paired pending and state helpers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TerminalRejectionPublishesPairedPendingAndSynchronizedState()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateDiagnosticsMemoryStore(clock);
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes) { ResultKind = OperationResultKind.Rejected };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var pending = new RecordingObserver<PendingSyncSummary>();
        var synchronized = new RecordingObserver<ReceiveCounterState>();
        var local = new RecordingObserver<ReceiveCounterState>();
        using var pendingSubscription = stream.ObservePending().Subscribe(pending);
        using var synchronizedSubscription = stream.WhereSynchronized().Subscribe(synchronized);
        using var localSubscription = stream.Local.Subscribe(local);
        var receipt = await PublishVolatileCounterAsync(stream);
        await WaitForConditionAsync(() => pending.Values.Exists(static value => value.OperationCount == 1));
        await WaitForConditionAsync(() => synchronized.Values.Exists(static value => value.Sum == 0));
        await WaitForConditionAsync(() => local.Values.Exists(static value => value.Sum == DiagnosticsLocalCounter));
        var pendingBeforeRejection = pending.Values.Count;
        var synchronizedBeforeRejection = synchronized.Values.Count;
        var localBeforeRejection = local.Values.Count;

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() =>
            pending.Values.Count > pendingBeforeRejection && pending.Values[^1].OperationCount == 0);
        await WaitForConditionAsync(() => synchronized.Values.Count > synchronizedBeforeRejection);
        await WaitForConditionAsync(() => local.Values.Count > localBeforeRejection);

        var durable = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(durable?.State).IsEqualTo(SyncOperationState.Rejected);
        await Assert.That(pending.Values[^1].OperationCount).IsEqualTo(0);
        await Assert.That(synchronized.Values[^1].Sum).IsEqualTo(local.Values[^1].Sum);
    }

    /// <summary>Verifies a terminal upload decision is persisted and published once through the typed stream.</summary>
    /// <param name="resultKind">The server decision.</param>
    /// <param name="expectedState">The expected durable and observable operation state.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(OperationResultKind.Rejected, SyncOperationState.Rejected)]
    [Arguments(OperationResultKind.Conflict, SyncOperationState.Conflict)]
    public async Task TerminalUploadDecisionMatchesDurableAndTypedObservableStatus(OperationResultKind resultKind, SyncOperationState expectedState)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateDiagnosticsMemoryStore(clock);
        using var metrics = CreateEngineMetricListener(out var capture);
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes) { ResultKind = resultKind };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var statuses = new RecordingObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(statuses);
        var receipt = await PublishVolatileCounterAsync(stream);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var durable = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);

        await Assert.That(durable!.State).IsEqualTo(expectedState);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(resultKind == OperationResultKind.Conflict ? ExpectedSingleOperation : 0);
        await Assert.That(statuses.Values.Count(status => status.OperationId == receipt.OperationId && status.State == expectedState)).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(capture.Sum(QueuePendingMetricName)).IsEqualTo(recovered.PendingOperations.Count);
        await Assert.That(capture.Sum(QueueBytesMetricName)).IsEqualTo(recovered.PendingOperations.Sum(SyncEngine.GetOperationRetainedBytes));

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(statuses.Values.Count(status => status.OperationId == receipt.OperationId && status.State == expectedState)).IsEqualTo(ExpectedSingleOperation);
    }
}
