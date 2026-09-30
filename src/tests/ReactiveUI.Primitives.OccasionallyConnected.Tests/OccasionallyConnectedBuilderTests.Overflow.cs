// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Public <c>PublishAsync</c> DropOldest and DropNewest outbox overflow tests for built contexts.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>
    /// Verifies DropOldest evicts the oldest pending non-durable operation first, dead-letters it with a stable reason,
    /// reports the fault and status transition, excludes it from local state, and then commits the new publication.
    /// </summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropOldestEvictsOldestNonDurableOperationFirst()
    {
        await using var store = CreatePublicAdmissionStore("oc-overflow-oldest-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        var statuses = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        var local = new RecoveredUploadDiagnosticObserver<CounterState>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        using var statusSubscription = stream.OperationStates.Subscribe(statuses);
        using var localSubscription = stream.Local.Subscribe(local);
        var dropOldest = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);

        var first = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestEvictsOldestNonDurableOperationFirst),
            FirstOverflowPublication);
        var second = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestEvictsOldestNonDurableOperationFirst),
            SecondOverflowPublication);
        var third = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestEvictsOldestNonDurableOperationFirst),
            ThirdOverflowPublication);

        await AssertPendingValuesAsync(store, stream.SubscriptionId, OverflowSecondDelta, OverflowThirdDelta);
        await AssertDeadLetteredAsync(store, first.OperationId, DroppedOldestReasonCode);
        await WaitForConditionAsync(() => local.Values.Exists(static state => state.Sum == OverflowSumAfterFirstEviction));
        await WaitForConditionAsync(() => statuses.Values.Exists(status =>
            status.OperationId == first.OperationId && status.State == SyncOperationState.DeadLettered));
        await WaitForConditionAsync(() => faults.Values.Count == 1);
        await Assert.That(faults.Values[0].Code).IsEqualTo(OutboxOverflowFaultCode);
        await Assert.That(faults.Values[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(faults.Values[0].Category).IsEqualTo(FaultCategory.Capacity);
        await Assert.That(third.ClientSequence).IsEqualTo(first.ClientSequence + OverflowSecondDelta);

        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowFourthDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestEvictsOldestNonDurableOperationFirst),
            FourthOverflowPublication);

        await AssertPendingValuesAsync(store, stream.SubscriptionId, OverflowThirdDelta, OverflowFourthDelta);
        await AssertDeadLetteredAsync(store, second.OperationId, DroppedOldestReasonCode);
    }

    /// <summary>Verifies DropOldest skips durable operations and fails when every pending operation is durable.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropOldestNeverEvictsDurableOperations()
    {
        await using var store = CreatePublicAdmissionStore("oc-overflow-durable-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var durable = CreatePublicPublishOptions(BufferStrategy.Reject, durable: true);
        var dropOldest = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);

        var first = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), durable, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsDurableOperations),
            FirstOverflowPublication);
        var second = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsDurableOperations),
            SecondOverflowPublication);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsDurableOperations),
            ThirdOverflowPublication);

        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowThirdDelta);
        await AssertDeadLetteredAsync(store, second.OperationId, DroppedOldestReasonCode);
        var firstStatus = await store.GetOperationStatusAsync(first.OperationId, CancellationToken.None);
        await Assert.That(firstStatus?.State).IsNotEqualTo(SyncOperationState.DeadLettered);
    }

    /// <summary>Verifies DropOldest fails with a capacity error and reports the overflow when only durable work is pending.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropOldestRejectsWhenOnlyDurableOperationsArePending()
    {
        await using var store = CreatePublicAdmissionStore("oc-overflow-all-durable-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        var durable = CreatePublicPublishOptions(BufferStrategy.Reject, durable: true);
        var dropOldest = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);

        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), durable, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestRejectsWhenOnlyDurableOperationsArePending),
            FirstOverflowPublication);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), durable, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestRejectsWhenOnlyDurableOperationsArePending),
            SecondOverflowPublication);

        await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => AwaitOverflowPublicationAsync(
                stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
                nameof(PublicDropOldestRejectsWhenOnlyDurableOperationsArePending),
                ThirdOverflowPublication));
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
        await WaitForConditionAsync(() => faults.Values.Count == 1);
        await Assert.That(faults.Values[0].Code).IsEqualTo(OutboxOverflowFaultCode);
        await Assert.That(faults.Values[0].OperationId).IsNull();
    }

    /// <summary>Verifies DropOldest never evicts an operation leased for upload, and evicts it once the lease is released.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropOldestNeverEvictsLeasedOperations()
    {
        await using var store = CreatePublicAdmissionStore("oc-overflow-leased-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var dropOldest = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);
        var first = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsLeasedOperations),
            FirstOverflowPublication);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsLeasedOperations),
            SecondOverflowPublication);
        var lease = await LeaseHeadAsync(store);

        await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => AwaitOverflowPublicationAsync(
                stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
                nameof(PublicDropOldestNeverEvictsLeasedOperations),
                ThirdOverflowPublication));
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);

        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropOldestNeverEvictsLeasedOperations),
            "third after lease release publication");

        await AssertPendingValuesAsync(store, stream.SubscriptionId, OverflowSecondDelta, OverflowThirdDelta);
        await AssertDeadLetteredAsync(store, first.OperationId, DroppedOldestReasonCode);
    }

    /// <summary>Verifies DropNewest rejects the incoming publication and reports the overflow without evicting committed work.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropNewestReportsOverflowAndKeepsCommittedWork()
    {
        await using var store = CreatePublicAdmissionStore("oc-overflow-newest-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        var dropNewest = CreatePublicPublishOptions(BufferStrategy.DropNewest, durable: false);

        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), dropNewest, CancellationToken.None).AsTask(),
            nameof(PublicDropNewestReportsOverflowAndKeepsCommittedWork),
            FirstOverflowPublication);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), dropNewest, CancellationToken.None).AsTask(),
            nameof(PublicDropNewestReportsOverflowAndKeepsCommittedWork),
            SecondOverflowPublication);

        await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => AwaitOverflowPublicationAsync(
                stream.PublishAsync(new(OverflowThirdDelta), dropNewest, CancellationToken.None).AsTask(),
                nameof(PublicDropNewestReportsOverflowAndKeepsCommittedWork),
                ThirdOverflowPublication));
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
        await WaitForConditionAsync(() => faults.Values.Count == 1);
        await Assert.That(faults.Values[0].Code).IsEqualTo(OutboxOverflowFaultCode);
    }

    /// <summary>Verifies DropOldest evictions and DropNewest rejections increment the overflow metric.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicDropStrategiesRecordOverflowMetric()
    {
        var counter = new OverflowMeasurementCounter();
        using var listener = CreateOverflowListener(counter);
        await using var store = CreatePublicAdmissionStore("oc-overflow-metric-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var dropOldest = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);

        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(1), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropStrategiesRecordOverflowMetric),
            FirstOverflowPublication);
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowSecondDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropStrategiesRecordOverflowMetric),
            SecondOverflowPublication);
        var beforeOverflow = counter.Total;
        _ = await AwaitOverflowPublicationAsync(
            stream.PublishAsync(new(OverflowThirdDelta), dropOldest, CancellationToken.None).AsTask(),
            nameof(PublicDropStrategiesRecordOverflowMetric),
            ThirdOverflowPublication);
        await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => AwaitOverflowPublicationAsync(
                stream.PublishAsync(
                    new(OverflowFourthDelta),
                    CreatePublicPublishOptions(BufferStrategy.DropNewest, durable: false),
                    CancellationToken.None).AsTask(),
                nameof(PublicDropStrategiesRecordOverflowMetric),
                FourthOverflowPublication));

        await Assert.That(counter.Total - beforeOverflow).IsGreaterThanOrEqualTo(OverflowSecondDelta);
    }
}
