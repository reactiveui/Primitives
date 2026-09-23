// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Recovered outbox scheduling tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The remote version returned by the recovered upload scheduling transport.</summary>
    private const string RecoveredUploadServerVersion = "recovered-v1";

    /// <summary>The counter delta used by the recovered upload scheduling test.</summary>
    private const int RecoveredUploadCounterDelta = 1;

    /// <summary>The expected operation count in the recovered mixed-priority upload batch.</summary>
    private const int RecoveredUploadMixedPriorityBatchCount = 2;

    /// <summary>The retry delay used by the recovered upload retry-due test.</summary>
    private const int RecoveredUploadRetryDelaySeconds = 3;

    /// <summary>The polling interval used while waiting for persisted recovered upload state.</summary>
    private const int RecoveredUploadPollMilliseconds = 10;

    /// <summary>The SQLite database file name used by recovered upload scheduling tests.</summary>
    private const string RecoveredUploadDatabaseFileName = "store.db";

    /// <summary>The lease byte budget used by recovered upload scheduling tests.</summary>
    private const long RecoveredUploadLeaseBytes = 1024;

    /// <summary>The lower recovered upload priority.</summary>
    private const int RecoveredUploadLowPriority = -6;

    /// <summary>The middle recovered upload priority.</summary>
    private const int RecoveredUploadMiddlePriority = 0;

    /// <summary>The higher recovered upload priority.</summary>
    private const int RecoveredUploadHighPriority = 7;

    /// <summary>The clock origin used by the recovered upload scheduling test.</summary>
    private static readonly DateTimeOffset RecoveredUploadTimestamp = new(2026, 9, 20, 9, 30, 0, TimeSpan.Zero);

    /// <summary>The middle-priority recovered upload stream.</summary>
    private static readonly StreamId RecoveredUploadMiddleStream = new("builder/recovered-middle");

    /// <summary>The higher-priority recovered upload stream.</summary>
    private static readonly StreamId RecoveredUploadHighStream = new("builder/recovered-high");

    /// <summary>Verifies starting a reopened context schedules recovered pending outbox work without a manual trigger.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartAsyncSchedulesRecoveredPendingOutboxWithoutManualTrigger()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-outbox-").FullName, RecoveredUploadDatabaseFileName);
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        PublishReceipt receipt;
        SubscriptionId subscriptionId;
        await using (var firstStore = await CreateRecoveredUploadStoreAsync(databasePath, clock))
        await using (var firstContext = CreateReadyBuilder(firstStore, new()).UseTimeProvider(clock).Build())
        {
            var firstStream = firstContext.GetOrCreateStream(CreateDefinition());
            receipt = await firstStream.PublishAsync(new(RecoveredUploadCounterDelta), null, CancellationToken.None);
            subscriptionId = firstStream.SubscriptionId;
        }

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, clock);
        var recovered = await reopenedStore.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);

        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(reopenedStore, reopenedTransport).UseTimeProvider(clock).Build();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        var operationStates = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var faultSubscription = reopenedContext.SyncEngine.Faults.Subscribe(faults);
        using var operationSubscription = reopenedContext.SyncEngine.OperationStates.Subscribe(operationStates);
        _ = reopenedContext.GetOrCreateStream(CreateDefinition());

        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.DwellTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await AwaitRecoveredUploadSynchronizedAsync(
            reopenedContext,
            reopenedStore,
            reopenedTransport,
            receipt.OperationId,
            clock,
            faults,
            operationStates);
        var synchronized = await reopenedStore.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        var afterUpload = await reopenedStore.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var pushed = await reopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

        await Assert.That(pushed.Operations.Count).IsEqualTo(1);
        await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(synchronized?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(afterUpload.PendingOperations).IsEmpty();
    }

    /// <summary>Verifies recovered retry-due work resumes automatically when the persisted due time arrives.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ReopenedRetryDueOutboxSynchronizesAfterPersistedDueWithoutManualTrigger()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-retry-due-").FullName, RecoveredUploadDatabaseFileName);
        var firstClock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        var retryOptions = CreateRecoveredUploadRetryOptions();
        var recoveredRetry = await CreateRecoveredUploadRetryDueAsync(databasePath, firstClock, retryOptions);
        var receipt = recoveredRetry.Receipt;
        var retryDueUtc = recoveredRetry.RetryState.DueUtc.GetValueOrDefault();
        var reopenedClock = new RecoveredUploadTimeProvider(firstClock.GetUtcNow());

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, reopenedClock);
        var observedStore = new RecoveredUploadObservedStore(reopenedStore, reopenedClock, retryDueUtc);
        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(observedStore, reopenedTransport)
            .UseOptions(retryOptions)
            .UseTimeProvider(reopenedClock)
            .Build();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        var operationStates = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var faultSubscription = reopenedContext.SyncEngine.Faults.Subscribe(faults);
        using var operationSubscription = reopenedContext.SyncEngine.OperationStates.Subscribe(operationStates);
        _ = reopenedContext.GetOrCreateStream(CreateDefinition());

        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await AwaitRecoveredUploadFutureWakeAsync(
            reopenedClock,
            observedStore,
            retryDueUtc,
            retryOptions.Batching.MaximumDwellTime);
        await Assert.That(reopenedClock.GetUtcNow()).IsLessThan(retryDueUtc);
        await Assert.That(reopenedTransport.PrepareCalls).IsEqualTo(0);
        await Assert.That(reopenedTransport.SendCalls).IsEqualTo(0);
        await Assert.That(reopenedTransport.Pushed.Task.IsCompleted).IsFalse();

        AdvanceRecoveredUploadClockTo(reopenedClock, retryDueUtc);
        await AwaitRecoveredUploadSynchronizedAsync(
            reopenedContext,
            reopenedStore,
            reopenedTransport,
            receipt.OperationId,
            reopenedClock,
            faults,
            operationStates);
        var pushed = await reopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

        await Assert.That(pushed.Operations.Count).IsEqualTo(1);
        await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Verifies recovered retry-due work keeps its future blocker when same-stream work and a manual trigger arrive early.</summary>
    /// <param name="dwellSeconds">The configured batching dwell seconds.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(RecoveredUploadRetryDelaySeconds)]
    [Arguments(RecoveredUploadRetryDelaySeconds + 1)]
    public async Task ReopenedRetryDueOutboxPreservesFutureBlockerAfterSameStreamPublishAndManualTrigger(int dwellSeconds)
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-retry-merge-").FullName, RecoveredUploadDatabaseFileName);
        var firstClock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        var retryOptions = CreateRecoveredUploadRetryOptions(TimeSpan.FromSeconds(dwellSeconds));
        var recoveredRetry = await CreateRecoveredUploadRetryDueAsync(databasePath, firstClock, retryOptions);
        var retryDueUtc = recoveredRetry.RetryState.DueUtc.GetValueOrDefault();
        var reopenedClock = new RecoveredUploadTimeProvider(firstClock.GetUtcNow());

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, reopenedClock);
        var observedStore = new RecoveredUploadObservedStore(reopenedStore, reopenedClock, retryDueUtc);
        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(observedStore, reopenedTransport)
            .UseOptions(retryOptions)
            .UseTimeProvider(reopenedClock)
            .Build();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        var operationStates = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var faultSubscription = reopenedContext.SyncEngine.Faults.Subscribe(faults);
        using var operationSubscription = reopenedContext.SyncEngine.OperationStates.Subscribe(operationStates);
        var stream = reopenedContext.GetOrCreateStream(CreateDefinition());
        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await AwaitRecoveredUploadFutureWakeAsync(
            reopenedClock,
            observedStore,
            retryDueUtc,
            retryOptions.Batching.MaximumDwellTime);
        await PublishAndAssertRecoveredUploadMergeAsync(new()
        {
            ReopenedContext = reopenedContext,
            ReopenedStream = stream,
            ReopenedStore = reopenedStore,
            ObservedStore = observedStore,
            ReopenedTransport = reopenedTransport,
            RecoveredRetry = recoveredRetry,
            ReopenedClock = reopenedClock,
            MaximumDwellTime = retryOptions.Batching.MaximumDwellTime,
            Faults = faults,
            OperationStates = operationStates,
            ObservedWakeCount = reopenedClock.UploadWakeTimerCount,
        });
    }

    /// <summary>Verifies recovered upload scheduling waits for an unexpired durable lease left by a previous process.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ReopenedLeasedOutboxSynchronizesAfterPersistedLeaseExpiryWithoutManualTrigger()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-lease-").FullName, RecoveredUploadDatabaseFileName);
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        PublishReceipt receipt;
        DateTimeOffset leaseExpiresAtUtc;
        await using (var store = await CreateRecoveredUploadStoreAsync(databasePath, clock))
        await using (var context = CreateReadyBuilder(store, new()).UseTimeProvider(clock).Build())
        {
            var stream = context.GetOrCreateStream(CreateDefinition());
            receipt = await stream.PublishAsync(new(RecoveredUploadCounterDelta), null, CancellationToken.None);
            var lease = await LeaseRecoveredUploadBatchAsync(store, TimeSpan.FromMinutes(1));
            leaseExpiresAtUtc = lease.ExpiresAtUtc;
        }

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, clock);
        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(reopenedStore, reopenedTransport).UseTimeProvider(clock).Build();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        var operationStates = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var faultSubscription = reopenedContext.SyncEngine.Faults.Subscribe(faults);
        using var operationSubscription = reopenedContext.SyncEngine.OperationStates.Subscribe(operationStates);
        _ = reopenedContext.GetOrCreateStream(CreateDefinition());

        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await AwaitRecoveredUploadFutureWakeAsync(
            clock,
            null,
            leaseExpiresAtUtc,
            OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await Assert.That(reopenedTransport.PrepareCalls).IsEqualTo(0);
        await Assert.That(reopenedTransport.SendCalls).IsEqualTo(0);

        AdvanceRecoveredUploadClockTo(clock, leaseExpiresAtUtc);
        await AwaitRecoveredUploadSynchronizedAsync(reopenedContext, reopenedStore, reopenedTransport, receipt.OperationId, clock, faults, operationStates);
        var pushed = await reopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

        await Assert.That(pushed.Operations.Count).IsEqualTo(1);
        await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Verifies recovered upload scheduling uses the highest recovered priority without reordering a stream's pending operations.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RecoveredPendingOutboxUsesHighestPriorityForFirstUpload()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-priority-").FullName, RecoveredUploadDatabaseFileName);
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        PublishReceipt middleReceipt;
        PublishReceipt mixedLowReceipt;
        PublishReceipt mixedHighReceipt;
        await using (var firstStore = await CreateRecoveredUploadStoreAsync(databasePath, clock))
        await using (var firstContext = CreateReadyBuilder(firstStore, new()).UseTimeProvider(clock).Build())
        {
            var middle = firstContext.GetOrCreateStream(CreateDefinition(RecoveredUploadMiddleStream));
            var mixed = firstContext.GetOrCreateStream(CreateDefinition(RecoveredUploadHighStream));
            mixedLowReceipt = await mixed.PublishAsync(
                new(RecoveredUploadCounterDelta),
                CreatePublishOptions(RecoveredUploadHighStream, RecoveredUploadLowPriority),
                CancellationToken.None);
            middleReceipt = await middle.PublishAsync(
                new(RecoveredUploadCounterDelta),
                CreatePublishOptions(RecoveredUploadMiddleStream, RecoveredUploadMiddlePriority),
                CancellationToken.None);
            mixedHighReceipt = await mixed.PublishAsync(
                new(RecoveredUploadCounterDelta),
                CreatePublishOptions(RecoveredUploadHighStream, RecoveredUploadHighPriority),
                CancellationToken.None);
        }

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, clock);
        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(reopenedStore, reopenedTransport)
            .UseOptions(OccasionallyConnectedOptions.Default with { MaxConcurrentStreams = 1 })
            .UseTimeProvider(clock)
            .Build();
        var middleStream = reopenedContext.GetOrCreateStream(CreateDefinition(RecoveredUploadMiddleStream));
        var mixedStream = reopenedContext.GetOrCreateStream(CreateDefinition(RecoveredUploadHighStream));
        await middleStream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await mixedStream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(clock.DwellTimerRegistered.Task.IsCompleted).IsFalse();

        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.DwellTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        var pushed = await reopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

        await Assert.That(pushed.Operations.Count).IsEqualTo(RecoveredUploadMixedPriorityBatchCount);
        await Assert.That(pushed.Operations[0].StreamId).IsEqualTo(RecoveredUploadHighStream);
        await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(mixedLowReceipt.OperationId);
        await Assert.That(pushed.Operations[1].OperationId).IsEqualTo(mixedHighReceipt.OperationId);
        await Assert.That(pushed.Operations.Any(operation => operation.OperationId == middleReceipt.OperationId)).IsFalse();
    }

    /// <summary>Verifies recovered pending work initialized before context start is deferred until the context starts.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamStartedBeforeContextDefersRecoveredOutboxUntilContextStart()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-context-recovered-deferred-").FullName, RecoveredUploadDatabaseFileName);
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        PublishReceipt receipt;
        await using (var firstStore = await CreateRecoveredUploadStoreAsync(databasePath, clock))
        await using (var firstContext = CreateReadyBuilder(firstStore, new()).UseTimeProvider(clock).Build())
        {
            var firstStream = firstContext.GetOrCreateStream(CreateDefinition());
            receipt = await firstStream.PublishAsync(new(RecoveredUploadCounterDelta), null, CancellationToken.None);
        }

        await using var reopenedStore = await CreateRecoveredUploadStoreAsync(databasePath, clock);
        var reopenedTransport = new RecoveredUploadTransportAdapter();
        await using var reopenedContext = CreateReadyBuilder(reopenedStore, reopenedTransport).UseTimeProvider(clock).Build();
        var reopenedStream = reopenedContext.GetOrCreateStream(CreateDefinition());

        await reopenedStream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(clock.DwellTimerRegistered.Task.IsCompleted).IsFalse();
        await reopenedContext.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.DwellTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await reopenedContext.SyncEngine
            .AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout, clock, CancellationToken.None)
            .AsTask()
            .WaitAsync(GuardTimeout);
        var pushed = await reopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

        await Assert.That(pushed.Operations.Count).IsEqualTo(1);
        await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Publishes same-stream work before retry due and verifies the recovered future blocker survives.</summary>
    /// <param name="fixture">The merge assertion fixture.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task PublishAndAssertRecoveredUploadMergeAsync(RecoveredUploadMergeFixture fixture)
    {
        Task? triggerTask = null;
        try
        {
            var secondReceipt = await fixture.ReopenedStream.PublishAsync(new(RecoveredUploadCounterDelta), null, CancellationToken.None);
            triggerTask = fixture.ReopenedContext.SyncEngine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitRecoveredUploadMergeProcessedBeforeDueAsync(
                fixture.ReopenedClock,
                fixture.ObservedStore,
                fixture.RecoveredRetry.RetryState.DueUtc.GetValueOrDefault(),
                fixture.MaximumDwellTime,
                fixture.ObservedWakeCount);
            await AssertRecoveredUploadNotSentBeforeDueAsync(
                fixture.ReopenedClock,
                fixture.ReopenedTransport,
                fixture.RecoveredRetry.RetryState.DueUtc.GetValueOrDefault());
            AdvanceRecoveredUploadClockTo(
                fixture.ReopenedClock,
                GetRecoveredUploadExpectedWakeUtc(
                    fixture.ReopenedClock,
                    fixture.RecoveredRetry.RetryState.DueUtc.GetValueOrDefault(),
                    fixture.MaximumDwellTime));
            await AwaitRecoveredUploadSynchronizedAsync(
                fixture.ReopenedContext,
                fixture.ReopenedStore,
                fixture.ReopenedTransport,
                fixture.RecoveredRetry.Receipt.OperationId,
                fixture.ReopenedClock,
                fixture.Faults,
                fixture.OperationStates);
            await triggerTask.WaitAsync(GuardTimeout);
            var pushed = await fixture.ReopenedTransport.Pushed.Task.WaitAsync(GuardTimeout);

            await Assert.That(pushed.Operations.Count).IsEqualTo(RecoveredUploadMixedPriorityBatchCount);
            await Assert.That(pushed.Operations[0].OperationId).IsEqualTo(fixture.RecoveredRetry.Receipt.OperationId);
            await Assert.That(pushed.Operations[1].OperationId).IsEqualTo(secondReceipt.OperationId);
        }
        finally
        {
            if (triggerTask is not null && !triggerTask.IsCompleted)
            {
                AdvanceRecoveredUploadClockTo(
                    fixture.ReopenedClock,
                    GetRecoveredUploadExpectedWakeUtc(
                        fixture.ReopenedClock,
                        fixture.RecoveredRetry.RetryState.DueUtc.GetValueOrDefault(),
                        fixture.MaximumDwellTime));
                await triggerTask.WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies the recovered upload has not sent before its not-before due time.</summary>
    /// <param name="clock">The reopened clock.</param>
    /// <param name="transport">The reopened transport.</param>
    /// <param name="retryDueUtc">The persisted retry due timestamp.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertRecoveredUploadNotSentBeforeDueAsync(
        RecoveredUploadTimeProvider clock,
        RecoveredUploadTransportAdapter transport,
        DateTimeOffset retryDueUtc)
    {
        await Assert.That(clock.GetUtcNow()).IsLessThan(retryDueUtc);
        await Assert.That(transport.PrepareCalls).IsEqualTo(0);
        await Assert.That(transport.SendCalls).IsEqualTo(0);
        await Assert.That(transport.Pushed.Task.IsCompleted).IsFalse();
    }

    /// <summary>Creates recovered upload work whose first send persisted a retry-due value.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="clock">The shared fake clock.</param>
    /// <param name="options">The retry options.</param>
    /// <returns>The recovered retry fixture.</returns>
    private static async Task<RecoveredUploadRetryFixture> CreateRecoveredUploadRetryDueAsync(
        string databasePath,
        RecoveredUploadTimeProvider clock,
        OccasionallyConnectedOptions options)
    {
        var sendFailure = new IOException("recovered upload retry");
        var transport = new RecoveredUploadTransportAdapter(sendFailure);
        await using var store = await CreateRecoveredUploadStoreAsync(databasePath, clock);
        await using var context = CreateReadyBuilder(store, transport)
            .UseOptions(options)
            .UseTimeProvider(clock)
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var receipt = await stream.PublishAsync(new(RecoveredUploadCounterDelta), null, CancellationToken.None);

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var initialWakeDelay = await clock.UploadWakeTimerRegistered.Task.WaitAsync(GuardTimeout);
        await Assert.That(initialWakeDelay).IsEqualTo(options.Batching.MaximumDwellTime);
        clock.Advance(options.Batching.MaximumDwellTime);
        var retryState = await AwaitRecoveredUploadRetryStateAsync(store, receipt.OperationId);
        var expectedDueUtc = clock.GetUtcNow().Add(TimeSpan.FromSeconds(RecoveredUploadRetryDelaySeconds));
        await Assert.That(retryState.DueUtc).IsEqualTo(expectedDueUtc);
        await Assert.That(transport.PrepareCalls).IsEqualTo(1);
        await Assert.That(transport.SendCalls).IsEqualTo(1);

        await context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        return new(receipt, retryState);
    }

    /// <summary>Awaits the persisted retry state created by the first recovered upload attempt.</summary>
    /// <param name="store">The SQLite store.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The retry state with a due time.</returns>
    /// <exception cref="TimeoutException">The retry state was not persisted before the guard timeout.</exception>
    private static async Task<RetryState> AwaitRecoveredUploadRetryStateAsync(SqliteLocalStoreAdapter store, OperationId operationId)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            var retryState = await store.GetRetryStateAsync(operationId, CancellationToken.None);
            if (retryState?.DueUtc is not null)
            {
                return retryState;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(RecoveredUploadPollMilliseconds)).ConfigureAwait(false);
        }

        throw new TimeoutException("Recovered upload retry state was not persisted.");
    }

    /// <summary>Creates options with deterministic retry delay for recovered upload scheduling tests.</summary>
    /// <param name="maximumDwellTime">The optional maximum batching dwell time.</param>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateRecoveredUploadRetryOptions(TimeSpan? maximumDwellTime = null) =>
        OccasionallyConnectedOptions.Default with
    {
        Batching = OccasionallyConnectedOptions.Default.Batching with
        {
            MaximumDwellTime = maximumDwellTime ?? OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime,
        },
        Retry = OccasionallyConnectedOptions.Default.Retry with
        {
            MinimumDelay = TimeSpan.FromSeconds(RecoveredUploadRetryDelaySeconds),
            MaximumDelay = TimeSpan.FromSeconds(RecoveredUploadRetryDelaySeconds),
        },
    };

    /// <summary>Advances the recovered upload clock to a due time when it is still in the future.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="dueUtc">The target due time.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AdvanceRecoveredUploadClockTo(RecoveredUploadTimeProvider clock, DateTimeOffset dueUtc)
    {
        var remaining = dueUtc - clock.GetUtcNow();
        if (remaining > TimeSpan.Zero)
        {
            clock.Advance(remaining);
        }
    }

    /// <summary>Awaits the first future upload wake observed by the recovered upload fake clock.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="store">The optional observed store used by the causal RED fixture.</param>
    /// <param name="dueUtc">The persisted not-before due time.</param>
    /// <param name="dwellTime">The normal batching dwell time.</param>
    /// <returns>A task representing the wait.</returns>
    private static async Task AwaitRecoveredUploadFutureWakeAsync(
        RecoveredUploadTimeProvider clock,
        RecoveredUploadObservedStore? store,
        DateTimeOffset dueUtc,
        TimeSpan dwellTime)
    {
        var scheduledDelay = await clock.UploadWakeTimerRegistered.Task.WaitAsync(GuardTimeout);
        var scheduledUtc = clock.GetUtcNow().Add(scheduledDelay);
        if (scheduledUtc < dueUtc && store is not null)
        {
            clock.Advance(scheduledDelay);
            await store.EmptyLeaseBeforeDue.Task.WaitAsync(GuardTimeout);
            return;
        }

        await Assert.That(scheduledUtc).IsEqualTo(GetRecoveredUploadExpectedWakeUtc(clock, dueUtc, dwellTime));
    }

    /// <summary>Gets the expected recovered upload wake after applying normal dwell and a future not-before constraint.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="dueUtc">The persisted not-before due time.</param>
    /// <param name="dwellTime">The normal batching dwell time.</param>
    /// <returns>The expected wake timestamp.</returns>
    private static DateTimeOffset GetRecoveredUploadExpectedWakeUtc(
        RecoveredUploadTimeProvider clock,
        DateTimeOffset dueUtc,
        TimeSpan dwellTime)
    {
        var dwellUtc = clock.GetUtcNow().Add(dwellTime);
        return dueUtc > dwellUtc ? dueUtc : dwellUtc;
    }

    /// <summary>Waits until the pre-due merge has either scheduled a future wake or attempted an invalid early lease.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="store">The observed store.</param>
    /// <param name="dueUtc">The persisted not-before due time.</param>
    /// <param name="dwellTime">The normal batching dwell time.</param>
    /// <param name="observedWakeCount">The number of upload wake timers already observed.</param>
    /// <returns>A task representing the wait.</returns>
    private static async Task AwaitRecoveredUploadMergeProcessedBeforeDueAsync(
        RecoveredUploadTimeProvider clock,
        RecoveredUploadObservedStore store,
        DateTimeOffset dueUtc,
        TimeSpan dwellTime,
        int observedWakeCount)
    {
        var expectedWakeUtc = GetRecoveredUploadExpectedWakeUtc(clock, dueUtc, dwellTime);
        var wakeTask = clock.WaitForUploadWakeAfterAsync(observedWakeCount);
        var completed = await Task.WhenAny(wakeTask, store.EmptyLeaseBeforeDue.Task).WaitAsync(GuardTimeout);
        await Assert.That(completed).IsSameReferenceAs(wakeTask);
        var scheduledDelay = await wakeTask.WaitAsync(GuardTimeout);

        await Assert.That(clock.GetUtcNow().Add(scheduledDelay)).IsEqualTo(expectedWakeUtc);
    }

    /// <summary>Leases the recovered upload batch without releasing it, matching a process crash boundary.</summary>
    /// <param name="store">The SQLite store.</param>
    /// <param name="duration">The durable lease duration.</param>
    /// <returns>The leased batch.</returns>
    /// <exception cref="InvalidOperationException">The store does not return a lease.</exception>
    private static async ValueTask<LeasedOperationBatch> LeaseRecoveredUploadBatchAsync(SqliteLocalStoreAdapter store, TimeSpan duration)
    {
        LeasedOperationBatch? result = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(new(Stream, 1, RecoveredUploadLeaseBytes, duration), CancellationToken.None))
        {
            result = batch;
        }

        return result ?? throw new InvalidOperationException("Expected a recovered upload lease.");
    }

    /// <summary>Awaits recovered upload synchronization and reports bounded diagnostics on timeout.</summary>
    /// <param name="context">The reopened context.</param>
    /// <param name="store">The reopened store.</param>
    /// <param name="transport">The recording transport.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="clock">The fake clock used by synchronization waits.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="operationStates">The observed operation states.</param>
    /// <returns>A task representing the wait.</returns>
    /// <exception cref="TimeoutException">The operation did not synchronize within the guard timeout.</exception>
    private static async Task AwaitRecoveredUploadSynchronizedAsync(
        OccasionallyConnectedContext context,
        SqliteLocalStoreAdapter store,
        RecoveredUploadTransportAdapter transport,
        OperationId operationId,
        TimeProvider clock,
        RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault> faults,
        RecoveredUploadDiagnosticObserver<SyncOperationStatus> operationStates)
    {
        try
        {
            await context.SyncEngine
                .AwaitSynchronizedAsync(operationId, GuardTimeout, clock, CancellationToken.None)
                .AsTask()
                .WaitAsync(GuardTimeout);
        }
        catch (TimeoutException exception)
        {
            var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None);
            throw new TimeoutException(CreateRecoveredUploadDiagnosticMessage(transport, status, faults, operationStates), exception);
        }
    }

    /// <summary>Creates the recovered upload diagnostic timeout message.</summary>
    /// <param name="transport">The recording transport.</param>
    /// <param name="status">The persisted operation status.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="operationStates">The observed operation states.</param>
    /// <returns>The diagnostic timeout message.</returns>
    private static string CreateRecoveredUploadDiagnosticMessage(
        RecoveredUploadTransportAdapter transport,
        SyncOperationStatus? status,
        RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault> faults,
        RecoveredUploadDiagnosticObserver<SyncOperationStatus> operationStates)
    {
        var stateTrace = string.Join(',', operationStates.Values.Select(static state => state.State.ToString()));
        var faultTrace = string.Join('|', faults.Values.Select(CreateRecoveredUploadFaultDiagnostic));
        return $"Recovered upload did not synchronize. PrepareCalls={transport.PrepareCalls}; SendCalls={transport.SendCalls}; "
            + $"Pushed={transport.Pushed.Task.IsCompleted}; PersistedState={status?.State.ToString() ?? "null"}; "
            + $"PersistedAttempt={status?.Attempt.ToString() ?? "null"}; PersistedReason={status?.ReasonCode ?? "null"}; "
            + $"ObservedStates={stateTrace}; Faults={faultTrace}";
    }

    /// <summary>Creates one recovered upload fault diagnostic entry.</summary>
    /// <param name="fault">The observed fault.</param>
    /// <returns>The diagnostic entry.</returns>
    private static string CreateRecoveredUploadFaultDiagnostic(OccasionallyConnectedFault fault)
    {
        var exception = fault.Exception is null
            ? "null"
            : $"{fault.Exception.GetType().Name}:{fault.Exception.Message}";
        return $"{fault.Code}:{fault.Message}:{exception}";
    }

    /// <summary>Creates a builder using a recovered-upload scheduling test transport.</summary>
    /// <param name="store">The store dependency.</param>
    /// <param name="transport">The transport dependency.</param>
    /// <returns>The configured builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreateReadyBuilder(
        ILocalStoreAdapter store,
        RecoveredUploadTransportAdapter transport) =>
        CreateBuilder()
            .UseClient(new(ClientId))
            .UseBorrowedStore(store)
            .UseBorrowedTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity);

    /// <summary>Creates publish options for a recovered upload scheduling test operation.</summary>
    /// <param name="streamId">The target stream.</param>
    /// <param name="priority">The scheduling priority.</param>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreatePublishOptions(StreamId streamId, int priority) => new() { StreamId = streamId, Priority = priority };

    /// <summary>Creates an initialized SQLite store for recovered-upload scheduling.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="timeProvider">The clock shared by the store and context.</param>
    /// <returns>The initialized store.</returns>
    private static async ValueTask<SqliteLocalStoreAdapter> CreateRecoveredUploadStoreAsync(string databasePath, RecoveredUploadTimeProvider timeProvider)
    {
        var store = new SqliteLocalStoreAdapter(databasePath, new() { TimeProvider = timeProvider });
        await store.InitializeAsync(new(StoreIdentity, 1, false) { ClientId = ClientId, Outbox = OccasionallyConnectedOptions.Default.Outbox }, CancellationToken.None);
        return store;
    }

    /// <summary>Stores recovered retry-due fixture state.</summary>
    /// <param name="Receipt">The original publish receipt.</param>
    /// <param name="RetryState">The persisted retry state.</param>
    private readonly record struct RecoveredUploadRetryFixture(PublishReceipt Receipt, RetryState RetryState);
}
