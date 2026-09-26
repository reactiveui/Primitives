// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Public <c>stream.Input</c> and <see cref="IRemoteObserver{T}"/> admission tests for built contexts.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies synchronous observer bridges reject Block and Custom admission during construction.</summary>
    /// <param name="strategy">The unsupported observer strategy.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(BufferStrategy.Block)]
    [Arguments(BufferStrategy.Custom)]
    public async Task PublicObserverBridgesRejectUnsupportedInputStrategiesAtConstruction(BufferStrategy strategy)
    {
        await using var store = CreatePublicAdmissionStore("oc-public-bridge-options-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateByteBoundOutbox()).Build();
        var invalid = CreateObserverInputDefinition(strategy, ObserverInputBufferCount);
        var valid = CreateObserverInputDefinition(BufferStrategy.Reject, ObserverInputBufferCount);
        var sink = new RecordingObserver<CounterInput>();
        var publish = CreatePublicPublishOptions(BufferStrategy.Reject, durable: false);

        await Assert.That(() => context.GetOrCreateStream(invalid)).Throws<InvalidOperationException>();
        await Assert.That(() => sink.ToRemoteObserver(context, invalid, publish)).Throws<InvalidOperationException>();
        await using var adapter = sink.ToRemoteObserver(context, valid, publish);
        await Assert.That(() => adapter.AsObserver(publish, new() { BufferStrategy = strategy })).Throws<InvalidOperationException>();
        await Assert.That(() => adapter.AsObserver(CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: true), null))
            .Throws<InvalidOperationException>();
    }

    /// <summary>Verifies stream input byte overflow reports a fault while admitted input still commits and drains on disposal.</summary>
    /// <param name="strategy">The supported observer strategy.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    public async Task PublicStreamInputByteLimitReportsOverflowAndDrainsAdmittedInput(BufferStrategy strategy)
    {
        await using var store = CreatePublicAdmissionStore("oc-public-input-");
        await using var transport = new RecordingTransportAdapter();
        var serializer = new PaddedCounterPayloadSerializer(GatedInputDelta);
        var context = CreatePublicAdmissionBuilder(store, transport, serializer, CreateByteBoundOutbox()).Build();
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        SubscriptionId subscriptionId;
        try
        {
            var stream = context.GetOrCreateStream(CreateObserverInputDefinition(strategy, ObserverInputBufferCount));
            using var faultSubscription = stream.Faults.Subscribe(faults);

            stream.Input.OnNext(new(GatedInputDelta));
            await serializer.GateEntered.Task.WaitAsync(GuardTimeout);
            stream.Input.OnNext(new(RejectedInputDelta));
            await WaitForConditionAsync(() => faults.Values.Count == 1);
            serializer.ReleaseGate();
            subscriptionId = stream.SubscriptionId;
        }
        finally
        {
            serializer.ReleaseGate();
            await context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        }

        await AssertFaultCodesAsync(faults, InputOverflowFaultCode);
        await AssertPendingValuesAsync(store, subscriptionId, GatedInputDelta);
    }

    /// <summary>Verifies remote observer publication applies outbox count limits and cancels a blocked caller before commit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicRemoteObserverPublishAsyncAppliesOutboxLimitsAndCancellation()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-remote-publish-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateSingleOperationOutbox(1)).Build();
        var definition = CreateObserverInputDefinition(BufferStrategy.Reject, ObserverInputBufferCount);
        await using var adapter = new RecordingObserver<CounterInput>()
            .ToRemoteObserver(context, definition, CreatePublicPublishOptions(BufferStrategy.Reject, durable: true));
        var reject = CreatePublicPublishOptions(BufferStrategy.Reject, durable: true);
        var block = CreatePublicPublishOptions(BufferStrategy.Block, durable: true);

        var receipt = await adapter.PublishAsync(new(1), reject, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(async () => await adapter.PublishAsync(new(RejectedInputDelta), reject, CancellationToken.None))
            .ThrowsExactly<QueueCapacityExceededException>();
        using CancellationTokenSource cancellation = new();
        var blocked = adapter.PublishAsync(new(RejectedInputDelta), block, cancellation.Token).AsTask();
        await Assert.That(blocked.IsCompleted).IsFalse();
        await cancellation.CancelAsync();
        await Assert.That(async () => await blocked.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();

        var stream = context.GetOrCreateStream(definition);
        await Assert.That(receipt.ClientSequence).IsEqualTo(1L);
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1);
    }

    /// <summary>
    /// Verifies remote observer producers report overflow and OnError on the stream fault channel while terminal signals
    /// close only the producer that received them.
    /// </summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicRemoteObserverAsObserverFaultsAndTerminalsAreProducerScoped()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-remote-observer-");
        await using var transport = new RecordingTransportAdapter();
        var serializer = new PaddedCounterPayloadSerializer(GatedInputDelta);
        var context = CreatePublicAdmissionBuilder(store, transport, serializer, CreateByteBoundOutbox()).Build();
        var definition = CreateObserverInputDefinition(BufferStrategy.Reject, 1);
        var publish = CreatePublicPublishOptions(BufferStrategy.Reject, durable: false);
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        SubscriptionId subscriptionId;
        try
        {
            var stream = context.GetOrCreateStream(definition);
            using var faultSubscription = stream.Faults.Subscribe(faults);
            await using var adapter = new RecordingObserver<CounterInput>().ToRemoteObserver(context, definition, publish);
            var failing = adapter.AsObserver(publish, null);
            var independent = adapter.AsObserver(publish, null);

            failing.OnNext(new(GatedInputDelta));
            await serializer.GateEntered.Task.WaitAsync(GuardTimeout);
            failing.OnNext(new(RejectedInputDelta));
            await WaitForConditionAsync(() => faults.Values.Count == 1);
            failing.OnError(new InvalidOperationException("producer failed"));
            failing.OnNext(new(RejectedInputDelta));
            await WaitForConditionAsync(() => faults.Values.Count == 2);
            independent.OnNext(new(IndependentInputDelta));
            serializer.ReleaseGate();
            independent.OnCompleted();
            await adapter.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
            subscriptionId = stream.SubscriptionId;
        }
        finally
        {
            serializer.ReleaseGate();
            await context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        }

        await AssertFaultCodesAsync(faults, InputOverflowFaultCode, InputProducerFaultCode);
        await AssertPendingValuesAsync(store, subscriptionId, GatedInputDelta, IndependentInputDelta);
    }
}
