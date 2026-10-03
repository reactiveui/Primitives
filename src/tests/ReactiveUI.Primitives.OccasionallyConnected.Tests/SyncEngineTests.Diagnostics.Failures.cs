// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics failure isolation tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies failed retry telemetry preserves backoff and retries the same durable operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingTelemetryPreservesRetryBackoffAndOperationIdentity()
    {
        using var metrics = CreateThrowingEngineMetricListener();
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateDiagnosticsMemoryStore(clock);
        var attempts = 0;
        var retryDelay = TimeSpan.FromSeconds(ExpectedSingleOperation);
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes)
        {
            OnSend = () =>
            {
                if (Interlocked.Increment(ref attempts) == ExpectedSingleOperation)
                {
                    throw CreateTransportFailure(RetryFailureKind.Transient, retryDelay);
                }
            },
        };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes) with
        {
            Retry = RetryOptions.Default with { MinimumDelay = retryDelay, MaximumDelay = retryDelay },
        };
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var receipt = await PublishVolatileCounterAsync(stream);

        await engine.StartAsync(CancellationToken.None);
        var firstSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        await WaitForConditionAsync(() => clock.HasTimerDueIn(retryDelay));
        var retry = await store.GetRetryStateAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(retry!.TransientAttemptCount).IsEqualTo(ExpectedSingleOperation);
        var retrySync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);

        clock.Advance(retryDelay);
        await Task.WhenAll(firstSync, retrySync).WaitAsync(GuardTimeout);
        var status = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(status!.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedTwoOperations);
        await Assert.That(session.SentBatches[0].Operations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(session.SentBatches[1].Operations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Verifies failed telemetry cannot hide an oversized operation's durable dead-letter outcome.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingTelemetryPreservesOversizedTypedOperationDeadLetter()
    {
        using var metrics = CreateThrowingEngineMetricListener();
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateDiagnosticsMemoryStore(clock);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes) { EncodedSizes = [OversizedPreparedBytes] };
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var receipt = await PublishVolatileCounterAsync(stream);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        var status = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(status!.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(session.SentBatches).IsEmpty();
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);

        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(session.PrepareCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies failed telemetry cannot prevent durable upload reconciliation or duplicate a completed upload.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingTelemetryPreservesTypedUploadOutcomeAndDoesNotDuplicateEffects()
    {
        using var metrics = CreateThrowingEngineMetricListener();
        using var activities = CreateThrowingEngineActivityStopListener();
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var store = CreateDiagnosticsMemoryStore(clock);
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(store, new() { SessionOverride = session }, options, timeProvider: clock);
        await using var stream = CreateUploadOnlyCounterStream(store, engine, new(), clock);
        var receipt = await PublishVolatileCounterAsync(stream);

        await engine.StartAsync(CancellationToken.None);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        var synchronized = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(synchronized!.State).IsEqualTo(SyncOperationState.Synchronized);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);

        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SentBatches[0].Operations[0].OperationId).IsEqualTo(receipt.OperationId);
        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
