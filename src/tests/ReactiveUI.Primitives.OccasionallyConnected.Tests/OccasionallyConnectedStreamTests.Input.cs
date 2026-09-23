// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Observer input facade tests.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies observer input drains through the stream-owned durable lane on dispose.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task InputObserverCommitsSerializedPayloadAndDisposeDrainsBeforeLaneClose()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource stateSerializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStateSerialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var serializer = new BlockingInputObserverPayloadSerializer(stateSerializeEntered, releaseStateSerialize);
        var definition = CreateDefinition() with
        {
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new CounterInputCapture(),
        };
        var coordinator = new RecordingCoordinator(store);
        await using var stream = CreateStream(store, definition: definition, coordinator: coordinator, serializer: serializer);
        await stream.StartAsync(CancellationToken.None);
        var subscriptionId = stream.SubscriptionId;

        stream.Input.OnNext(new(FirstValue));
        await stateSerializeEntered.Task.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        var admissionsDuringPublish = coordinator.EnterLocalCommitCalls;
        var completionsDuringPublish = coordinator.CompleteLocalCommitCalls;
        var dispose = stream.DisposeAsync().AsTask();

        await Assert.That(dispose.IsCompleted).IsFalse();

        _ = releaseStateSerialize.TrySetResult();
        await dispose.WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds));
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(FirstValue);
        await Assert.That(coordinator.CommitReadyCalls).IsEqualTo(1);
        await Assert.That(admissionsDuringPublish).IsEqualTo(1);
        await Assert.That(completionsDuringPublish).IsEqualTo(0);
        await Assert.That(coordinator.CompleteLocalCommitCalls).IsEqualTo(1);
    }

    /// <summary>Verifies the inert input facade ignores observer calls when input capture is not configured.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task InputObserverWithoutCaptureIgnoresSignalsAndLeavesPublishAvailable()
    {
        await using var store = await CreateInitializedStoreAsync();
        await using var stream = CreateStream(store);

        stream.Input.OnCompleted();
        stream.Input.OnError(new InvalidOperationException("ignored input observer error"));
        stream.Input.OnNext(new(FirstValue));
        await stream.StartAsync(CancellationToken.None);
        var receipt = await stream.PublishAsync(new(SecondValue), null, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies observer input commits use explicit definition publish options for serialized payloads.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task InputObserverUsesDefinitionPublishOptionsForSerializedCommit()
    {
        await using var store = await CreateInitializedStoreAsync();
        var definition = CreateDefinition() with
        {
            Publish = new RemotePublishOptions { StreamId = Stream, BaseVersion = ExplicitBaseVersion, Durable = true, Priority = SecondValue, ConflictPolicy = ConflictPolicy.LastWriterWins },
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new CounterInputCapture(),
        };
        var scheduler = new ControlledObserverScheduler();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var stream = CreateStream(store, definition, scheduler: scheduler);
        using var faultSubscription = stream.Faults.Subscribe(faults);
        await stream.StartAsync(CancellationToken.None);
        var subscriptionId = stream.SubscriptionId;

        stream.Input.OnNext(new(FirstValue));
        await stream.DisposeAsync();
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(faults.Values).IsEmpty();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].BaseVersion).IsEqualTo(ExplicitBaseVersion);
        await Assert.That(recovery.PendingOperations[0].Policy.Priority).IsEqualTo(SecondValue);
        await Assert.That(recovery.PendingOperations[0].Policy.ConflictPolicy).IsEqualTo(ConflictPolicy.LastWriterWins);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies pre-start observer input can initialize and drain during disposal without opening public admission.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task InputObserverBeforeStartInitializesOfflineDuringDispose()
    {
        await using var store = await CreateInitializedStoreAsync();
        var scheduler = new ControlledObserverScheduler();
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var definition = CreateDefinition(ExplicitSubscription) with
        {
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new CounterInputCapture(),
        };
        await using var stream = CreateStream(store, definition, scheduler: scheduler);
        using var faultSubscription = stream.Faults.Subscribe(faults);

        stream.Input.OnNext(new(SecondValue));
        await stream.DisposeAsync();
        scheduler.RunAll();
        var recovery = await store.RecoverStreamAsync(Stream, ExplicitSubscription, CancellationToken.None);

        await Assert.That(faults.Values).IsEmpty();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(ParsePayloadValue(recovery.PendingOperations[0].Payload)).IsEqualTo(SecondValue);
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => stream.StartAsync(CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => PublishCounterInputAsync(stream, new(ThirdValue), null, CancellationToken.None));
    }

    /// <summary>Captures counter input into owned serialized payload envelopes.</summary>
    private sealed class CounterInputCapture : IOccasionallyConnectedInputCapture<CounterInput>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRetainedByteCount(CounterInput value) => NotificationCapacityBytes;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadEnvelope Capture(CounterInput value) => CreatePayload(value.Delta);
    }

    /// <summary>Blocks serialization of the first committed input-observer state.</summary>
    /// <param name="entered">The signal set when blocked serialization starts.</param>
    /// <param name="release">The signal that releases blocked serialization.</param>
    private sealed class BlockingInputObserverPayloadSerializer(
        TaskCompletionSource entered,
        TaskCompletionSource release) : ScriptedPayloadSerializer
    {
        /// <inheritdoc />
        public override async ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var numeric = value switch
            {
                CounterInput input => input.Delta,
                CounterState state => await ReadStateAsync(state, cancellationToken).ConfigureAwait(false),
                _ => throw new InvalidOperationException(UnexpectedPayloadTypeMessage),
            };
            var text = numeric.ToString(CultureInfo.InvariantCulture);
            return new(
                contractId,
                schemaVersion,
                ContentType,
                System.Text.Encoding.UTF8.GetBytes(text),
                $"hash-{text}");
        }

        /// <inheritdoc />
        public override ValueTask<object> DeserializeAsync(
            PayloadEnvelope envelope,
            Type targetType,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var value = ParsePayloadValue(envelope);
            if (targetType == typeof(CounterInput))
            {
                return ValueTask.FromResult<object>(new CounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return ValueTask.FromResult<object>(new CounterState(value));
            }

            throw new InvalidOperationException(UnexpectedTargetTypeMessage);
        }

        /// <summary>Reads the state value and blocks the first committed observer input state.</summary>
        /// <param name="state">The local state.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The state value.</returns>
        private async ValueTask<int> ReadStateAsync(CounterState state, CancellationToken cancellationToken)
        {
            if (state.Sum != FirstValue)
            {
                return state.Sum;
            }

            _ = entered.TrySetResult();
            await release.Task
                .WaitAsync(TimeSpan.FromSeconds(TestWaitTimeoutSeconds), cancellationToken)
                .ConfigureAwait(false);
            return state.Sum;
        }
    }
}
