// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Remote adapter producer ownership races.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The message for deliberately unused typed publication dependencies.</summary>
    private const string NoTypedPublicationExpected = "No typed publication is expected.";

    /// <summary>Verifies typed adapter publication targets the context stream and stops at adapter disposal.</summary>
    /// <returns>A task that completes when durable receipts are inspected.</returns>
    [Test]
    public async Task RemoteObserverAdapterPublishAsyncForwardsWhileOwnedAndRejectsAfterDisposal()
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
        var adapter = new RecordingObserver<MutableCounterInput>()
            .ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = Stream });

        _ = await adapter.PublishAsync(new(FirstValue), new RemotePublishOptions { StreamId = Stream }, CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
        {
            _ = await adapter.PublishAsync(new(SecondValue), new RemotePublishOptions { StreamId = new("counter/other") }, CancellationToken.None);
        });
        await adapter.DisposeAsync();
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () =>
        {
            _ = await adapter.PublishAsync(new(SecondValue), new RemotePublishOptions { StreamId = Stream }, CancellationToken.None);
        });

        _ = await stream.PublishAsync(new(SecondValue), null, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).ToArray(), [FirstValue, SecondValue]);
    }

    /// <summary>Verifies adapter disposal joins terminal producer cleanup already in progress.</summary>
    /// <returns>A task that completes after the producer releases.</returns>
    [Test]
    public async Task RemoteObserverAdapterDisposeWaitsForCompletedProducerDrain()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var producer = new DelayedDisposeProducer<MutableCounterInput>();
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = _ => producer,
        });

        var observer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);
        observer.OnCompleted();
        observer.OnCompleted();
        await producer.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var dispose = adapter.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        producer.ReleaseDispose();
        await dispose;

        await Assert.That(producer.DisposeCalls).IsEqualTo(1);
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies one producer cleanup failure cannot skip another independently owned producer.</summary>
    /// <returns>A task that completes after all producers are released.</returns>
    [Test]
    public async Task RemoteObserverAdapterDisposeContinuesAfterProducerCleanupFailure()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var failing = new FailingDisposeProducer<MutableCounterInput>();
        var delayed = new DelayedDisposeProducer<MutableCounterInput>();
        var producers = new Queue<IOccasionallyConnectedInputProducer<MutableCounterInput>>([failing, delayed]);
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = _ => producers.Dequeue(),
        });
        var firstObserver = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);
        var secondObserver = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);

        var dispose = adapter.DisposeAsync().AsTask();
        await delayed.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        await Assert.That(dispose.IsCompleted).IsFalse();
        delayed.ReleaseDispose();
        _ = await Assert.That(() => dispose).ThrowsExactly<InvalidOperationException>();

        await Assert.That(failing.DisposeCalls).IsEqualTo(1);
        await Assert.That(delayed.DisposeCalls).IsEqualTo(1);
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);
        firstObserver.OnCompleted();
        secondObserver.OnCompleted();
        await Assert.That(failing.DisposeCalls).IsEqualTo(1);
        await Assert.That(delayed.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies disposal owns a producer whose factory was already admitted but has not returned.</summary>
    /// <returns>A task that completes after the late producer drains.</returns>
    [Test]
    public async Task RemoteObserverAdapterDisposeWaitsForInFlightProducerFactory()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var producer = new DelayedDisposeProducer<MutableCounterInput>();
        var factoryEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var factoryRelease = new ManualResetEventSlim();
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = _ => CreateAfterGate(),
        });

        var creating = Task.Run(() => adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null));
        await factoryEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        var dispose = adapter.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        factoryRelease.Set();
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(async () => _ = await creating);
        await producer.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        await Assert.That(dispose.IsCompleted).IsFalse();
        producer.ReleaseDispose();
        await dispose;

        await Assert.That(producer.DisposeCalls).IsEqualTo(1);
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);

        IOccasionallyConnectedInputProducer<MutableCounterInput> CreateAfterGate()
        {
            _ = factoryEntered.TrySetResult();
            if (!factoryRelease.Wait(TimeSpan.FromSeconds(DisposalTimeoutSeconds)))
            {
                throw new TimeoutException("The test did not release producer creation.");
            }

            return producer;
        }
    }

    /// <summary>Verifies remote terminal signals and independent input error cleanup preserve observer ownership.</summary>
    /// <returns>A task that completes after both adapters and the input producer drain.</returns>
    [Test]
    public async Task RemoteObserverAdapterForwardsRemoteTerminalsAndInputErrorOnce()
    {
        var completionSource = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var completed = new RecordingObserver<MutableCounterInput>();
        var completedProducer = new DelayedDisposeProducer<MutableCounterInput>();
        var completionAdapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = completed,
            StreamId = Stream,
            RemoteMessages = completionSource,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = _ => completedProducer,
        });
        var input = completionAdapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);
        var inputError = new InvalidOperationException("Input producer failed.");
        input.OnError(inputError);
        input.OnError(inputError);
        await completedProducer.DisposeEntered.Task.WaitAsync(TimeSpan.FromSeconds(DisposalTimeoutSeconds));
        completionSource.CompleteActive();
        await Assert.That(completed.CompletedCount).IsEqualTo(1);
        completedProducer.ReleaseDispose();
        await completionAdapter.DisposeAsync();
        await Assert.That(((RecordingObserver<MutableCounterInput>)completedProducer.Observer).Error).IsSameReferenceAs(inputError);
        await Assert.That(completedProducer.DisposeCalls).IsEqualTo(1);

        var errorSource = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var errored = new RecordingObserver<MutableCounterInput>();
        var remoteError = new InvalidOperationException("Remote source failed.");
        var errorAdapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = errored,
            StreamId = Stream,
            RemoteMessages = errorSource,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = static _ => new DelayedDisposeProducer<MutableCounterInput>(),
        });
        errorSource.ErrorActive(remoteError);
        await Assert.That(errored.Error).IsSameReferenceAs(remoteError);
        await errorAdapter.DisposeAsync();
        await Assert.That(errorSource.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies invalid adapter composition cannot leave a remote subscription or producer owner behind.</summary>
    /// <returns>A task that completes after factory failure and adapter disposal.</returns>
    [Test]
    public async Task RemoteObserverAdapterRejectsMissingIdentityAndNullProducerFactoryResult()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        _ = await Assert.That(() => new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = default,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = static _ => null,
        })).ThrowsExactly<ArgumentException>();
        await Assert.That(remote.SubscribeCount).IsEqualTo(0);

        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = new ThrowingFaultPublisher(),
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = static _ => null,
        });
        _ = await Assert.That(() => adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null)).ThrowsExactly<InvalidOperationException>();
        await adapter.DisposeAsync();
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Verifies a failed completed-producer drain reports one bounded input fault.</summary>
    /// <returns>A task that completes after the producer cleanup attempt.</returns>
    [Test]
    public async Task RemoteObserverAdapterReportsCompletedProducerCleanupFailure()
    {
        var remote = new ManualObservable<RemoteMessage<MutableCounterInput>>();
        var producer = new FailingDisposeProducer<MutableCounterInput>();
        var publisher = new RecordingFaultPublisher();
        var adapter = new RemoteObserverAdapter<MutableCounterInput>(new()
        {
            Observer = new RecordingObserver<MutableCounterInput>(),
            StreamId = Stream,
            RemoteMessages = remote,
            InputOptions = new ObserverInputOptions { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            Capture = new MutableCounterInputCapture(),
            Publisher = publisher,
            PublishAsync = static (_, _, _) => ValueTask.FromException<PublishReceipt>(new NotSupportedException(NoTypedPublicationExpected)),
            CreateProducer = _ => producer,
        });
        var observer = adapter.AsObserver(new RemotePublishOptions { StreamId = Stream }, null);
        observer.OnCompleted();
        await adapter.DisposeAsync();

        await Assert.That(publisher.FaultCalls).IsEqualTo(1);
        await Assert.That(publisher.Code).IsEqualTo("OC.Input.CompletedProducer");
        await Assert.That(producer.DisposeCalls).IsEqualTo(1);
        await Assert.That(remote.ActiveSubscriptions).IsEqualTo(0);
    }

    /// <summary>Producer whose cleanup waits for an external release.</summary>
    /// <typeparam name="T">The input type.</typeparam>
    private sealed class DelayedDisposeProducer<T> : IOccasionallyConnectedInputProducer<T>
    {
        /// <summary>The cleanup release gate.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the cleanup entry signal.</summary>
        public TaskCompletionSource DisposeEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the cleanup attempt count.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc />
        public IObserver<T> Observer { get; } = new RecordingObserver<T>();

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            DisposeCalls++;
            _ = DisposeEntered.TrySetResult();
            await _release.Task.ConfigureAwait(false);
        }

        /// <summary>Releases a blocked producer cleanup.</summary>
        public void ReleaseDispose() => _ = _release.TrySetResult();
    }

    /// <summary>Records a completed producer cleanup diagnostic.</summary>
    private sealed class RecordingFaultPublisher : IOccasionallyConnectedSerializedInputPublisher
    {
        /// <summary>Gets the number of fault reports.</summary>
        public int FaultCalls { get; private set; }

        /// <summary>Gets the reported code.</summary>
        public string? Code { get; private set; }

        /// <inheritdoc />
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishSerializedInputAsync(PayloadEnvelope payload, RemotePublishOptions? options, CancellationToken cancellationToken) =>
            ValueTask.FromException<PublishReceipt>(new NotSupportedException("No serialized publication is expected."));

        /// <inheritdoc />
        public void PublishInputFault(string code, string message, OperationId? operationId, Exception exception)
        {
            FaultCalls++;
            Code = code;
        }
    }
}
