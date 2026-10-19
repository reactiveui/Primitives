// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Stream telemetry tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies context synchronization states use the public sequencer configured by the builder.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextSyncStatesUseConfiguredPublicSequencer()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new RecordingSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var states = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var subscription = context.SyncStates.Subscribe(states);

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => TryFindSyncState(states, SyncLifecycleStatus.Online, 0, out _));

        await Assert.That(sequencer.ScheduleCalls).IsGreaterThan(0);
    }

    /// <summary>Verifies a rejected sequencer schedule retains the latest context state for a later transition.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextSyncStatesRecoverAfterPublicSequencerRejection()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new RecordingSequencer();
        sequencer.RejectNextSchedule();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var states = new RecoveredUploadDiagnosticObserver<SyncState>();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var stateSubscription = context.SyncStates.Subscribe(states);
        using var faultSubscription = context.SyncEngine.Faults.Subscribe(faults);

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == "OC.Engine.SyncStateObserver"));
        await context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => TryFindSyncState(states, SyncLifecycleStatus.Stopped, 0, out _));

        await Assert.That(sequencer.ScheduleCalls).IsGreaterThan(1);
    }

    /// <summary>Verifies offline local publication reports stream-owned pending work without claiming network availability.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OfflineStreamLocalPublishReportsPendingQueueWithoutNetworkAvailability()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var states = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var subscription = stream.SyncStates.Subscribe(states);

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => TryFindSyncState(states, SyncLifecycleStatus.Created, 1, out _));

        var state = RequireSyncState(states, SyncLifecycleStatus.Created, 1);
        await Assert.That(state.NetworkAvailable).IsFalse();
        await Assert.That(state.PendingOperations).IsEqualTo(1);
        await Assert.That(state.PendingBytes).IsGreaterThan(0);
    }

    /// <summary>Verifies stream synchronization state queue counts are isolated by stream identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamSyncStatesKeepPendingQueueCountsIsolatedByStream()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport)
            .UseSequencer(new RecordingSequencer())
            .Build();
        const int FirstStreamPendingCount = 2;
        var first = context.GetOrCreateStream(CreateDefinition(Stream));
        var second = context.GetOrCreateStream(CreateDefinition(RecoveredUploadMiddleStream));
        var firstStates = new RecoveredUploadDiagnosticObserver<SyncState>();
        var secondStates = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var firstSubscription = first.SyncStates.Subscribe(firstStates);
        using var secondSubscription = second.SyncStates.Subscribe(secondStates);

        var firstStart = firstStates.Values.Count;
        var secondStart = secondStates.Values.Count;
        _ = await first.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() =>
            firstStates.Values.Count > firstStart && GetLatestSyncState(firstStates).PendingOperations == 1);
        await Assert.That(secondStates.Values.Count).IsEqualTo(secondStart);

        firstStart = firstStates.Values.Count;
        secondStart = secondStates.Values.Count;
        _ = await first.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await AssertNewSyncStatesHavePendingCountAsync(firstStates, firstStart, FirstStreamPendingCount);
        await Assert.That(secondStates.Values.Count).IsEqualTo(secondStart);

        firstStart = firstStates.Values.Count;
        secondStart = secondStates.Values.Count;
        _ = await second.PublishAsync(
            new(1),
            new RemotePublishOptions { StreamId = RecoveredUploadMiddleStream, Durable = false },
            CancellationToken.None);
        await Assert.That(firstStates.Values.Count).IsEqualTo(firstStart);
        await WaitForConditionAsync(() =>
            secondStates.Values.Count > secondStart && GetLatestSyncState(secondStates).PendingOperations == 1);
        await Assert.That(GetLatestSyncState(firstStates).PendingOperations).IsEqualTo(FirstStreamPendingCount);
        await Assert.That(GetLatestSyncState(secondStates).PendingOperations).IsEqualTo(1);
    }

    /// <summary>Verifies stream operation state publication uses durable upload attempts after reconciliation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamOperationStatesPublishDurableAttemptAfterUploadReconciliation()
    {
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        await using var store = new RecordingStoreAdapter();
        var transport = new RecoveredUploadTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).UseTimeProvider(clock).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var states = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var subscription = stream.OperationStates.Subscribe(states);

        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.UploadWakeTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await WaitForConditionAsync(() => TryFindOperationStatus(states, receipt.OperationId, SyncOperationState.Synchronized, out _));

        var published = RequireOperationStatus(states, receipt.OperationId, SyncOperationState.Synchronized);
        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(persisted?.Attempt).IsEqualTo(1);
        await Assert.That((int?)published.Attempt).IsEqualTo(persisted?.Attempt);
    }

    /// <summary>Verifies context lifecycle diagnostics include durable pending work after an offline commit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ContextSyncStatesIncludePendingQueueAfterOfflineLocalPublish()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var states = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var subscription = context.SyncStates.Subscribe(states);

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => TryFindSyncState(states, SyncLifecycleStatus.Created, 1, out _));

        var state = RequireSyncState(states, SyncLifecycleStatus.Created, 1);
        await Assert.That(state.NetworkAvailable).IsFalse();
        await Assert.That(state.PendingBytes).IsGreaterThan(0);
    }

    /// <summary>Verifies a failed post-commit status lookup cannot make a synchronized operation retry.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamStatusLookupFaultPreservesDurableReconciliation()
    {
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        await using var store = new RecordingStoreAdapter();
        var transport = new RecoveredUploadTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).UseTimeProvider(clock).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var subscription = stream.Faults.Subscribe(faults);
        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        store.AfterGetOperationStatus = static status =>
        {
            if (status?.State == SyncOperationState.Synchronized)
            {
                throw new InvalidOperationException("Diagnostic lookup failed.");
            }
        };

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.UploadWakeTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await WaitForConditionAsync(() => faults.Values.Exists(fault => fault.OperationId == receipt.OperationId));

        store.AfterGetOperationStatus = static _ => { };
        var persisted = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(persisted?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(persisted?.Attempt).IsEqualTo(1);
        await Assert.That(transport.SendCalls).IsEqualTo(1);
    }

    /// <summary>Finds a stream synchronization state by status and pending count.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="status">The expected lifecycle status.</param>
    /// <param name="pendingOperations">The expected pending operation count.</param>
    /// <param name="state">The matching state.</param>
    /// <returns>Whether a matching state was found.</returns>
    private static bool TryFindSyncState(
        RecoveredUploadDiagnosticObserver<SyncState> observer,
        SyncLifecycleStatus status,
        int pendingOperations,
        out SyncState? state)
    {
        for (var i = observer.Values.Count - 1; i >= 0; i--)
        {
            var candidate = observer.Values[i];
            if (candidate.Status != status || candidate.PendingOperations != pendingOperations)
            {
                continue;
            }

            state = candidate;
            return true;
        }

        state = null;
        return false;
    }

    /// <summary>Gets a required stream synchronization state.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="status">The expected lifecycle status.</param>
    /// <param name="pendingOperations">The expected pending operation count.</param>
    /// <returns>The matching state.</returns>
    /// <exception cref="InvalidOperationException">The expected notification was not observed.</exception>
    private static SyncState RequireSyncState(
        RecoveredUploadDiagnosticObserver<SyncState> observer,
        SyncLifecycleStatus status,
        int pendingOperations) =>
        TryFindSyncState(observer, status, pendingOperations, out var state) && state is not null
            ? state
            : throw new InvalidOperationException("Expected stream synchronization state was not observed.");

    /// <summary>Asserts all newly observed stream synchronization states use the expected pending count.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="startIndex">The first new item index.</param>
    /// <param name="pendingOperations">The expected pending operation count.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertNewSyncStatesHavePendingCountAsync(
        RecoveredUploadDiagnosticObserver<SyncState> observer,
        int startIndex,
        int pendingOperations)
    {
        await Assert.That(observer.Values.Count).IsGreaterThan(startIndex);
        for (var index = startIndex; index < observer.Values.Count; index++)
        {
            await Assert.That(observer.Values[index].PendingOperations).IsEqualTo(pendingOperations);
        }
    }

    /// <summary>Gets the latest observed stream synchronization state.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>The latest observed state.</returns>
    /// <exception cref="InvalidOperationException">The expected notification was not observed.</exception>
    private static SyncState GetLatestSyncState(RecoveredUploadDiagnosticObserver<SyncState> observer) =>
        observer.Values.Count > 0
            ? observer.Values[^1]
            : throw new InvalidOperationException("Expected at least one stream synchronization state.");

    /// <summary>Finds an operation status by identity and durable state.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="state">The expected operation state.</param>
    /// <param name="status">The matching status.</param>
    /// <returns>Whether a matching status was found.</returns>
    private static bool TryFindOperationStatus(
        RecoveredUploadDiagnosticObserver<SyncOperationStatus> observer,
        OperationId operationId,
        SyncOperationState state,
        out SyncOperationStatus? status)
    {
        for (var i = observer.Values.Count - 1; i >= 0; i--)
        {
            var candidate = observer.Values[i];
            if (candidate.OperationId != operationId || candidate.State != state)
            {
                continue;
            }

            status = candidate;
            return true;
        }

        status = null;
        return false;
    }

    /// <summary>Gets a required operation status by identity and durable state.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="state">The expected operation state.</param>
    /// <returns>The matching status.</returns>
    /// <exception cref="InvalidOperationException">The expected notification was not observed.</exception>
    private static SyncOperationStatus RequireOperationStatus(
        RecoveredUploadDiagnosticObserver<SyncOperationStatus> observer,
        OperationId operationId,
        SyncOperationState state) =>
        TryFindOperationStatus(observer, operationId, state, out var status) && status is not null
            ? status
            : throw new InvalidOperationException("Expected operation status was not observed.");
}
