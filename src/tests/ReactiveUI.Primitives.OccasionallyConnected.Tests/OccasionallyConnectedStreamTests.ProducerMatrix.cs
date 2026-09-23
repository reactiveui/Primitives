// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Public producer acceptance matrix tests.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies volatile publish options with lossy admission commit their policy through the public facade.</summary>
    /// <param name="admissionStrategy">The lossy admission strategy accepted for volatile work.</param>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    public async Task PublishAsyncCommitsVolatileDroppingAdmissionStrategyAsVolatileOperation(BufferStrategy admissionStrategy)
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var local = new RecordingObserver<CounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        scheduler.RunAll();
        var options = new RemotePublishOptions { StreamId = Stream, Durable = false, AdmissionStrategy = admissionStrategy };

        var receipt = await stream.PublishAsync(new(FirstValue), options, CancellationToken.None);
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(recovery.PendingOperations[0].Policy.Durability).IsEqualTo(OperationDurability.Volatile);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(FirstValue);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
    }

    /// <summary>Verifies concurrent public publishers cannot exceed the stream work lane count limit.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    /// <exception cref="InvalidOperationException">A publish task was not created.</exception>
    [Test]
    public async Task PublishAsyncRejectsThirdConcurrentProducerWhenWorkCapacityIsFull()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource inputSerializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInputSerialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var serializer = new ScriptedPayloadSerializer { InputSerializeEntered = inputSerializeEntered, ReleaseInputSerialize = releaseInputSerialize };
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler, serializer: serializer);
        await stream.StartAsync(CancellationToken.None);
        var local = new RecordingObserver<CounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        scheduler.RunAll();
        Task<PublishReceipt>? first = null;
        Task<PublishReceipt>? second = null;
        Exception? originalFailure = null;

        try
        {
            first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
            await inputSerializeEntered.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
            second = stream.PublishAsync(new(SecondValue), null, CancellationToken.None).AsTask();

            await Assert.That(() => PublishCounterInputAsync(stream, new(ThirdValue), null, CancellationToken.None))
                .ThrowsExactly<InvalidOperationException>();
        }
        catch (Exception exception)
        {
            originalFailure = exception;
        }
        finally
        {
            _ = releaseInputSerialize.TrySetResult();
            await CompleteCleanupAsync(
                    originalFailure,
                    () => WaitForStartedTasksAsync(
                        TimeSpan.FromSeconds(TestWaitTimeoutSeconds),
                        first,
                        second))
                .ConfigureAwait(false);
        }

        var completedFirst = first ?? throw new InvalidOperationException("The first publish was not started.");
        var completedSecond = second ?? throw new InvalidOperationException("The second publish was not started.");
        var receipts = await Task.WhenAll(completedFirst, completedSecond).WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await AssertSequenceAsync(receipts.Select(static receipt => receipt.ClientSequence).ToArray(), [FirstSequence, SecondSequence]);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => operation.ClientSequence).ToArray(), [FirstSequence, SecondSequence]);
        await AssertSequenceAsync(recovery.PendingOperations.Select(static operation => ParsePayloadValue(operation.Payload)).ToArray(), [FirstValue, SecondValue]);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue, FirstValue + SecondValue]);
    }

    /// <summary>Verifies cancellation removes a queued public producer before it can serialize or commit input.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    /// <exception cref="InvalidOperationException">The first publish task was not created.</exception>
    [Test]
    public async Task PublishAsyncCancellationRemovesQueuedProducerBeforeSerialization()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource inputSerializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInputSerialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var serializer = new ScriptedPayloadSerializer { InputSerializeEntered = inputSerializeEntered, ReleaseInputSerialize = releaseInputSerialize };
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler, serializer: serializer);
        await stream.StartAsync(CancellationToken.None);
        var local = new RecordingObserver<CounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        scheduler.RunAll();
        using CancellationTokenSource queuedCancellation = new();
        Task<PublishReceipt>? first = null;
        Exception? originalFailure = null;

        try
        {
            first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
            await inputSerializeEntered.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
            var queued = stream.PublishAsync(new(SecondValue), null, queuedCancellation.Token).AsTask();
            await queuedCancellation.CancelAsync();

            await Assert.That(async () => await queued.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds)).ConfigureAwait(false))
                .Throws<OperationCanceledException>();
        }
        catch (Exception exception)
        {
            originalFailure = exception;
        }
        finally
        {
            _ = releaseInputSerialize.TrySetResult();
            await CompleteCleanupAsync(
                    originalFailure,
                    () => first?.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds)) ?? Task.CompletedTask)
                .ConfigureAwait(false);
        }

        var completed = first ?? throw new InvalidOperationException("The first publish was not started.");
        var receipt = await completed.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(FirstValue);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondSequence);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
    }

    /// <summary>Verifies synchronous stream input overflow reports a fault and preserves the admitted volatile operation.</summary>
    /// <param name="strategy">The non-blocking observer input strategy.</param>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    public async Task InputObserverOverflowCommitsOnlyAdmittedVolatileInput(BufferStrategy strategy)
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource stateSerializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStateSerialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var serializer = new BlockingInputObserverPayloadSerializer(stateSerializeEntered, releaseStateSerialize);
        TaskCompletionSource initialObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource committedObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource overflowObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var definition = CreateVolatileProducerDefinition(strategy);
        await using var stream = CreateStream(store, definition, scheduler: ThreadPoolObserverNotificationScheduler.Instance, serializer: serializer);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var local = new RecordingObserver<CounterState>();
        using var faultSubscription = stream.Faults.Subscribe(new ActionObserver<OccasionallyConnectedFault>(fault =>
        {
            faults.OnNext(fault);
            _ = overflowObserved.TrySetResult();
        }));
        using var localSubscription = stream.Local.Subscribe(CreateProducerStateObserver(local, initialObserved, committedObserved));
        await stream.StartAsync(CancellationToken.None);
        var subscriptionId = stream.SubscriptionId;
        await initialObserved.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        Exception? originalFailure = null;

        try
        {
            stream.Input.OnNext(new(FirstValue));
            await stateSerializeEntered.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
            stream.Input.OnNext(new(SecondValue));
            await overflowObserved.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
            _ = releaseStateSerialize.TrySetResult();
            await committedObserved.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        }
        catch (Exception exception)
        {
            originalFailure = exception;
        }
        finally
        {
            _ = releaseStateSerialize.TrySetResult();
            await CompleteCleanupAsync(
                    originalFailure,
                    () => stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds)))
                .ConfigureAwait(false);
        }

        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(faults.Values.Count).IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.InputOverflow");
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].Policy.Durability).IsEqualTo(OperationDurability.Volatile);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(FirstValue);
        await AssertSequenceAsync(local.Values.Select(static state => state.Sum).ToArray(), [0, FirstValue]);
    }

    /// <summary>Creates a volatile stream with one retained observer input slot.</summary>
    /// <param name="strategy">The overflow strategy.</param>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StreamDefinition<CounterState, CounterInput> CreateVolatileProducerDefinition(BufferStrategy strategy) =>
        CreateDefinition() with
        {
            Publish = new RemotePublishOptions { StreamId = Stream, Durable = false },
            Input = new() { BufferStrategy = strategy, BufferCapacity = 1, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new CounterInputCapture(),
        };

    /// <summary>Records delivered states and signals initial recovery and committed publication separately.</summary>
    /// <param name="recording">The recorded states.</param>
    /// <param name="initialObserved">The initial state observation.</param>
    /// <param name="committedObserved">The committed state observation.</param>
    /// <returns>The observer used by the live subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ActionObserver<CounterState> CreateProducerStateObserver(
        RecordingObserver<CounterState> recording,
        TaskCompletionSource initialObserved,
        TaskCompletionSource committedObserved) =>
        new(state =>
        {
            recording.OnNext(state);
            _ = (state.Sum == 0 ? initialObserved : committedObserved).TrySetResult();
        });

    /// <summary>Waits for every task that was started before cleanup began.</summary>
    /// <param name="timeout">The cleanup timeout.</param>
    /// <param name="tasks">The optional tasks to observe.</param>
    /// <returns>A task that completes when all started tasks complete.</returns>
    private static Task WaitForStartedTasksAsync(TimeSpan timeout, params Task?[] tasks)
    {
        var started = new List<Task>(tasks.Length);
        foreach (var task in tasks)
        {
            if (task is not null)
            {
                started.Add(task);
            }
        }

        return started.Count == 0
            ? Task.CompletedTask
            : Task.WhenAll(started).WaitAsync(timeout);
    }

    /// <summary>Runs bounded cleanup without masking an earlier test failure.</summary>
    /// <param name="originalFailure">The original failure to preserve.</param>
    /// <param name="cleanup">The cleanup operation.</param>
    /// <returns>A task that completes after cleanup or throws the correct failure.</returns>
    private static async Task CompleteCleanupAsync(Exception? originalFailure, Func<Task> cleanup)
    {
        Exception? cleanupFailure = null;
        try
        {
            await cleanup().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            cleanupFailure = exception;
        }

        if (originalFailure is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(originalFailure).Throw();
        }

        if (cleanupFailure is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(cleanupFailure).Throw();
        }
    }
}
