// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Convenience wrapper lifecycle races.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies repeated starts share one source subscription and stream views remain context-owned aliases.</summary>
    /// <returns>A task that completes when wrapper properties are inspected.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamRepeatedStartKeepsOneSubscriptionAndForwardsViews()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler());
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        await wrapped.StartAsync(CancellationToken.None);
        await wrapped.StartAsync(CancellationToken.None);

        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(wrapped.StreamId).IsEqualTo(stream.StreamId);
        await Assert.That(wrapped.SubscriptionId).IsEqualTo(stream.SubscriptionId);
        await Assert.That(wrapped.Local).IsNotNull();
        await Assert.That(wrapped.Remote).IsNotNull();
        await Assert.That(wrapped.SyncStates).IsNotNull();
        await Assert.That(wrapped.OperationStates).IsNotNull();
        await Assert.That(wrapped.Faults).IsNotNull();
        await Assert.That(wrapped.Input).IsSameReferenceAs(stream.Input);
        await Assert.That(((IOccasionallyConnectedCommittedStateQueueSnapshots<CounterState>)wrapped).CommittedStateQueueSnapshots).IsNotNull();
    }

    /// <summary>Verifies canceled lifecycle waiters leave no retained semaphore owner.</summary>
    /// <returns>A task that completes after a later start and stop succeeds.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamCanceledWaitsDoNotPoisonLaterLifecycle()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler());
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        _ = await Assert.ThrowsExactlyAsync<TaskCanceledException>(async () => await wrapped.StartAsync(cancellation.Token));
        _ = await Assert.ThrowsExactlyAsync<TaskCanceledException>(async () => await wrapped.StopAsync(cancellation.Token));
        await wrapped.StartAsync(CancellationToken.None);
        await wrapped.StopAsync(CancellationToken.None);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies a queued stop settles after disposal overtakes a delayed start.</summary>
    /// <returns>A task that completes when all lifecycle requests settle.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamDisposeDrainsQueuedStopWaiter()
    {
        await using var store = await CreateInitializedStoreAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { StartEntered = entered, ReleaseStart = release };
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);

        var start = wrapped.StartAsync(CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var dispose = wrapped.DisposeAsync().AsTask();
        var stop = wrapped.StopAsync(CancellationToken.None).AsTask();
        _ = release.TrySetResult();
        await Task.WhenAll(start, dispose, stop);

        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies a failing source unsubscription still stops the inner stream.</summary>
    /// <returns>A task that completes after both cleanup paths have run.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopPreservesSubscriptionFailureAndStopsInner()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new BlockingDisposeObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        await wrapped.StartAsync(CancellationToken.None);
        source.ReleaseDispose();

        var failure = await Assert.That(() => wrapped.StopAsync(CancellationToken.None).AsTask()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(failure).IsNotNull();
        if (failure is { } observedFailure)
        {
            await Assert.That(observedFailure.Message).IsEqualTo("Subscription disposal failed after release.");
        }

        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies both source unsubscription and inner stop failures remain observable.</summary>
    /// <returns>A task that completes after both cleanup attempts finish.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopPreservesBothCleanupFailures()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store) { ThrowOnStop = true };
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new BlockingDisposeObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        await wrapped.StartAsync(CancellationToken.None);
        source.ReleaseDispose();

        var failure = await Assert.That(() => wrapped.StopAsync(CancellationToken.None).AsTask()).ThrowsExactly<AggregateException>();
        await Assert.That(failure).IsNotNull();
        if (failure is { } observedFailure)
        {
            await Assert.That(observedFailure.InnerExceptions).Count().IsEqualTo(TwoNotifications);
        }

        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        coordinator.ThrowOnStop = false;
    }

    /// <summary>Verifies an inner stop failure remains visible after source cleanup succeeds.</summary>
    /// <returns>A task that completes after the source subscription is removed.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopPropagatesInnerFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store) { ThrowOnStop = true };
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        await wrapped.StartAsync(CancellationToken.None);

        _ = await Assert.That(() => wrapped.StopAsync(CancellationToken.None).AsTask()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        coordinator.ThrowOnStop = false;
    }

    /// <summary>Verifies accepted producer diagnostics delegate to the inner serialized publisher.</summary>
    /// <returns>A task that completes after the forwarded fault is inspected.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamForwardsProducerFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler());
        var publisher = new RecordingFaultPublisher();
        await using var wrapped = new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new ManualObservable<MutableCounterInput>(),
            stream,
            definition,
            publisher);

        ((IOccasionallyConnectedSerializedInputPublisher)wrapped).PublishInputFault(
            "OC.Test.Forwarded",
            "Owned producer fault.",
            null,
            new InvalidOperationException("Owned producer failed."));
        await Assert.That(publisher.FaultCalls).IsEqualTo(1);
        await Assert.That(publisher.Code).IsEqualTo("OC.Test.Forwarded");
    }

    /// <summary>Verifies a public source error reaches the context-owned stream fault observable.</summary>
    /// <returns>A task that completes after the stream diagnostic is delivered.</returns>
    [Test]
    public async Task ToOccasionallyConnectedSourceErrorPublishesStreamFault()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, scheduler);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = wrapped.Faults.Subscribe(faults);
        await wrapped.StartAsync(CancellationToken.None);

        source.ErrorActive(new InvalidOperationException("The source failed."));
        scheduler.RunAll();

        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.InputProducer");
        await Assert.That(faults.Values[0].StreamId).IsEqualTo(Stream);
    }

    /// <summary>Verifies internal wrapper composition rejects streams without the paired queue facet.</summary>
    /// <returns>A task that completes after constructor validation is inspected.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamRejectsMissingPairedSnapshotFacet()
    {
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var inner = new UnsupportedExternalStream<CounterState, MutableCounterInput>();
        _ = await Assert.That(() => new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new ManualObservable<MutableCounterInput>(),
            inner,
            definition,
            new RecordingFaultPublisher())).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies internal wrapper composition rejects either missing owned producer dependency.</summary>
    /// <returns>A task that completes after both invalid constructions are rejected.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamRejectsMissingProducerDefinitionPieces()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        _ = await Assert.That(() => new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new ManualObservable<MutableCounterInput>(),
            stream,
            definition with { Input = null },
            (IOccasionallyConnectedSerializedInputPublisher)stream)).ThrowsExactly<InvalidOperationException>();
        _ = await Assert.That(() => new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new ManualObservable<MutableCounterInput>(),
            stream,
            definition with { InputCapture = null },
            (IOccasionallyConnectedSerializedInputPublisher)stream)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(coordinator.StopCalls).IsEqualTo(0);
    }

    /// <summary>Verifies a real source producer terminal failure reaches Stop after source and inner cleanup.</summary>
    /// <returns>A task that completes after the producer fault and stop failure are inspected.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopPropagatesTerminalProducerFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        var source = new ManualObservable<MutableCounterInput>();
        var publisher = new FatalDiagnosticPublisher();
        await using var wrapped = new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(source, stream, definition, publisher);
        await wrapped.StartAsync(CancellationToken.None);
        source.Publish(new(FirstValue));
        await publisher.FaultReported.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));

        var failure = await Assert.That(() => wrapped.StopAsync(CancellationToken.None).AsTask()).ThrowsExactly<OutOfMemoryException>();
        await Assert.That(failure).IsSameReferenceAs(publisher.Failure);
        await Assert.That(source.ActiveSubscriptions).IsEqualTo(0);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
        await Assert.That(publisher.FaultCalls).IsEqualTo(TwoNotifications);
    }

    /// <summary>Verifies subscription cleanup and terminal producer failures are both preserved.</summary>
    /// <returns>A task that completes after both cleanup paths and inner stop.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamStopAggregatesSubscriptionAndProducerFailures()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        var source = new BlockingDisposeObservable<MutableCounterInput>();
        var publisher = new FatalDiagnosticPublisher();
        await using var wrapped = new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(source, stream, definition, publisher);
        await wrapped.StartAsync(CancellationToken.None);
        source.Publish(new(FirstValue));
        await publisher.FaultReported.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        source.ReleaseDispose();

        var failure = await Assert.That(() => wrapped.StopAsync(CancellationToken.None).AsTask()).ThrowsExactly<AggregateException>();
        await Assert.That(failure).IsNotNull();
        if (failure is { } observedFailure)
        {
            await Assert.That(observedFailure.InnerExceptions).Count().IsEqualTo(TwoNotifications);
            await Assert.That(observedFailure.InnerExceptions[0]).IsTypeOf<InvalidOperationException>();
            await Assert.That(observedFailure.InnerExceptions[1]).IsSameReferenceAs(publisher.Failure);
        }

        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies subscription failure and terminal producer drain failure remain independently observable.</summary>
    /// <returns>A task that completes after the failed source admission stops the inner stream.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamSubscribeAndProducerDrainFailuresAggregate()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        var publisher = new FatalDiagnosticPublisher();
        await using var wrapped = new SourceOccasionallyConnectedStream<CounterState, MutableCounterInput>(
            new ThrowingAfterPublishObservable<MutableCounterInput>(new(FirstValue)),
            stream,
            definition,
            publisher);

        var failure = await Assert.That(() => wrapped.StartAsync(CancellationToken.None).AsTask()).ThrowsExactly<AggregateException>();
        await Assert.That(failure).IsNotNull();
        if (failure is { } observedFailure)
        {
            await Assert.That(observedFailure.InnerExceptions).Count().IsEqualTo(TwoNotifications);
            await Assert.That(observedFailure.InnerExceptions[0]).IsTypeOf<InvalidOperationException>();
            await Assert.That(observedFailure.InnerExceptions[1]).IsSameReferenceAs(publisher.Failure);
        }

        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies concurrent wrapper disposers share cleanup failure after the inner stream stops.</summary>
    /// <returns>A task that completes after both disposal callers observe the same failure.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamDisposeSharesSourceCleanupFailure()
    {
        await using var store = await CreateInitializedStoreAsync();
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler(), coordinator);
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new BlockingDisposeObservable<MutableCounterInput>();
        var wrapped = source.ToOccasionallyConnected(context, definition);
        await wrapped.StartAsync(CancellationToken.None);

        var first = Task.Run(async () => await wrapped.DisposeAsync());
        await source.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var second = wrapped.DisposeAsync().AsTask();
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();
        source.ReleaseDispose();
        var firstFailure = await Assert.That(() => first).ThrowsExactly<InvalidOperationException>();
        var secondFailure = await Assert.That(() => second).ThrowsExactly<InvalidOperationException>();
        await Assert.That(secondFailure).IsSameReferenceAs(firstFailure);
        await Assert.That(coordinator.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies a disposed wrapper rejects publication while its context-owned stream remains usable.</summary>
    /// <returns>A task that completes when ownership assertions finish.</returns>
    [Test]
    public async Task SourceOccasionallyConnectedStreamRejectsPublicationsAfterWrapperDisposal()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateMutableInputDefinition() with
        {
            SubscriptionId = ExplicitSubscription,
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };
        await using var stream = CreateMutableInputStream(store, definition, new ControlledObserverScheduler());
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        await using var wrapped = source.ToOccasionallyConnected(context, definition);
        await Assert.That(wrapped.Input).IsSameReferenceAs(stream.Input);
        var publisher = (IOccasionallyConnectedSerializedInputPublisher)wrapped;
        var captured = new MutableCounterInputCapture().Capture(new(FirstValue));
        _ = await publisher.PublishSerializedInputAsync(captured, null, CancellationToken.None);
        await wrapped.DisposeAsync();

        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () =>
        {
            await wrapped.StartAsync(CancellationToken.None);
        });

        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () =>
        {
            _ = await wrapped.PublishAsync(new(FirstValue), null, CancellationToken.None);
        });
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () =>
        {
            _ = await publisher.PublishSerializedInputAsync(captured, null, CancellationToken.None);
        });

        _ = await stream.PublishAsync(new(SecondValue), null, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).ToArray(), [FirstValue, SecondValue]);
    }

    /// <summary>Publisher whose first input fault callback fails with a controlled fatal exception.</summary>
    private sealed class FatalDiagnosticPublisher : IOccasionallyConnectedSerializedInputPublisher
    {
        /// <summary>Gets the controlled fatal callback exception.</summary>
        public Exception Failure { get; } = (Exception)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(OutOfMemoryException));

        /// <summary>Gets the first diagnostic callback signal.</summary>
        public TaskCompletionSource FaultReported { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the diagnostic callback count.</summary>
        public int FaultCalls { get; private set; }

        /// <inheritdoc />
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishSerializedInputAsync(PayloadEnvelope payload, RemotePublishOptions? options, CancellationToken cancellationToken) =>
            ValueTask.FromException<PublishReceipt>(new InvalidOperationException("The simulated publisher rejected the captured input."));

        /// <inheritdoc />
        public void PublishInputFault(string code, string message, OperationId? operationId, Exception exception)
        {
            FaultCalls++;
            _ = FaultReported.TrySetResult();
            if (FaultCalls == 1)
            {
                throw Failure;
            }
        }
    }

    /// <summary>Source that offers one input and then rejects registration without retaining a subscription.</summary>
    /// <typeparam name="T">The source input type.</typeparam>
    /// <param name="value">The input published during registration.</param>
    private sealed class ThrowingAfterPublishObservable<T>(T value) : IObservable<T>
    {
        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnNext(value);
            throw new InvalidOperationException("The source rejected subscription after publishing input.");
        }
    }
}
