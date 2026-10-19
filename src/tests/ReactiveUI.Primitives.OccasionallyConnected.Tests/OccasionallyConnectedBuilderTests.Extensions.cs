// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests paired convenience observables through a public context.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The queue size after typed and direct engine publication.</summary>
    private const int DirectEnqueuePendingCount = 2;

    /// <summary>Verifies a real engine acknowledgement updates paired helpers without another local publish.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextPairedHelpersAdvanceAfterEngineAcknowledgement()
    {
        var clock = new RecoveredUploadTimeProvider(RecoveredUploadTimestamp);
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecoveredUploadTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).UseTimeProvider(clock).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var pending = new RecoveredUploadDiagnosticObserver<PendingSyncSummary>();
        var synchronized = new RecoveredUploadDiagnosticObserver<CounterState>();
        using var pendingSubscription = stream.ObservePending().Subscribe(pending);
        using var synchronizedSubscription = stream.WhereSynchronized().Subscribe(synchronized);

        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => pending.Values.Exists(static value => value.OperationCount == 1));
        var pendingBeforeAcknowledgement = pending.Values.Count;
        await Assert.That(synchronized.Values.Exists(static value => value.Sum == 1)).IsFalse();

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await clock.UploadWakeTimerRegistered.Task.WaitAsync(GuardTimeout);
        clock.Advance(OccasionallyConnectedOptions.Default.Batching.MaximumDwellTime);
        await context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout, TimeProvider.System, CancellationToken.None);
        await WaitForConditionAsync(() =>
            pending.Values.Count > pendingBeforeAcknowledgement && pending.Values[^1].OperationCount == 0);
        await WaitForConditionAsync(() => synchronized.Values.Exists(static value => value.Sum == 1));

        var durable = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(durable?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(pending.Values[^1].OperationCount).IsEqualTo(0);
        await Assert.That(synchronized.Values[^1].Sum).IsEqualTo(1);
    }

    /// <summary>Verifies direct engine publication into a context participant updates the paired projection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextPairedHelpersTrackDirectEngineEnqueue()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var pending = new RecoveredUploadDiagnosticObserver<PendingSyncSummary>();
        var states = new RecoveredUploadDiagnosticObserver<CounterState>();
        using var pendingSubscription = stream.ObservePending().Subscribe(pending);
        using var localSubscription = stream.Local.Subscribe(states);

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        var before = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        var source = before.PendingOperations[0];
        var operation = source with
        {
            OperationId = OperationId.New(),
            ClientSequence = before.NextClientSequence,
        };

        _ = await context.SyncEngine.EnqueueOperationAsync(operation, CancellationToken.None);
        await WaitForConditionAsync(() => pending.Values.Exists(static value => value.OperationCount == DirectEnqueuePendingCount));
        await WaitForConditionAsync(() => states.Values.Exists(static value => value.Sum == DirectEnqueuePendingCount));

        var after = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(after.PendingOperations.Count).IsEqualTo(DirectEnqueuePendingCount);
        await Assert.That(pending.Values[^1].OperationCount).IsEqualTo(DirectEnqueuePendingCount);
        await Assert.That(states.Values[^1].Sum).IsEqualTo(DirectEnqueuePendingCount);
    }
}
