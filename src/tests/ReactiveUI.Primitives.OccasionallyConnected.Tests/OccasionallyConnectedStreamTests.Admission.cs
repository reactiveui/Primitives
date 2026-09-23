// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Admission tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The retained-byte charge used to prove typed admission is configured, not a sentinel.</summary>
    private const long ConfiguredTypedAdmissionBytes = 256;

    /// <summary>The expected number of completed admissions.</summary>
    private const int ExpectedCompletedAdmissions = 2;

    /// <summary>The record capacity in the byte-bound store fixture.</summary>
    private const int ByteBoundStoreRecords = 16;

    /// <summary>The payload-byte capacity that permits identity, snapshots, and small edits but cannot fit the large edit.</summary>
    private const long ByteBoundStoreBytes = 4096;

    /// <summary>The encoded byte length used to make a valid counter command impossible for an empty store.</summary>
    private const int OversizedCounterPayloadBytes = 8192;

    /// <summary>Verifies an edit too large for an empty store fails promptly and does not consume a sequence.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task PublishAsyncRejectsImpossibleStoreCapacityAndAllowsSmallerEdit()
    {
        await using var store = new InMemoryLocalStoreAdapter(new FixedTimeProvider(Now), ByteBoundStoreRecords, ByteBoundStoreBytes, new());
        await store.InitializeAsync(new(StoreIdentity, 1, false) { ClientId = ClientId }, CancellationToken.None);
        var coordinator = new RecordingCoordinator(store);
        var definition = CreateDefinition() with { Publish = new() { StreamId = Stream, Durable = false, DeliveryGuarantee = DeliveryGuarantee.AtMostOnce } };
        var scheduler = new ControlledObserverScheduler();
        var serializer = new OversizedCounterInputPayloadSerializer(
            new ScriptedPayloadSerializer(),
            oversizedValue: int.MaxValue,
            paddedByteCount: OversizedCounterPayloadBytes);
        await using var stream = CreateStream(store, definition, coordinator: coordinator, scheduler: scheduler, serializer: serializer);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);

        var failure = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => stream.PublishAsync(new(int.MaxValue), null, CancellationToken.None).AsTask().WaitAsync(GuardTimeout));
        var rejected = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(failure?.CanFitWhenEmpty).IsFalse();
        await Assert.That(coordinator.CapacityReleaseWaits).IsEqualTo(0);
        await Assert.That(rejected.PendingOperations.Count).IsEqualTo(0);
        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var saved = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(saved.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies rejecting admission leaves the active edit intact and accepts a later edit after capacity returns.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task PublishAsyncRejectsFullLaneWithoutWaitingOrConsumingSequence()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new RecordingCoordinator(store) { IdentityEntered = identityEntered, ReleaseIdentity = releaseIdentity };
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, coordinator: coordinator, scheduler: scheduler, workCapacity: SingleWorkCapacity);
        var local = new RecordingObserver<CounterState>();
        using var subscription = stream.Local.Subscribe(local);
        var options = new RemotePublishOptions { StreamId = Stream, AdmissionStrategy = BufferStrategy.Reject };
        var first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        try
        {
            await identityEntered.Task.WaitAsync(GuardTimeout);
            await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
                () => stream.PublishAsync(new(SecondValue), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout));
            await Assert.That(coordinator.CapacityReleaseWaits).IsEqualTo(0);
            await Assert.That(first.IsCompleted).IsFalse();
            releaseIdentity.SetResult();
            var firstReceipt = await first.WaitAsync(GuardTimeout);
            var laterReceipt = await stream.PublishAsync(new(ThirdValue), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
            scheduler.RunAll();

            await Assert.That(firstReceipt.ClientSequence).IsEqualTo(FirstSequence);
            await Assert.That(laterReceipt.ClientSequence).IsEqualTo(SecondSequence);
            await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedCompletedAdmissions);
            await Assert.That(local.Values[^1].Sum).IsEqualTo(FirstValue + ThirdValue);
        }
        finally
        {
            _ = releaseIdentity.TrySetResult();
            _ = await first.WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Verifies typed capacity waits use the same configured retained-byte charge as admission.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task PublishAsyncCapacityWaitUsesConfiguredAdmissionRetainedBytes()
    {
        await using var store = await CreateInitializedStoreAsync();
        TaskCompletionSource identityEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseIdentity = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource capacityWaitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCapacityWait = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var coordinator = new ByteRecordingCoordinator(store)
        {
            IdentityEntered = identityEntered,
            ReleaseIdentity = releaseIdentity,
            CapacityWaitEntered = capacityWaitEntered,
            ReleaseCapacityWait = releaseCapacityWait,
        };
        var serializer = new ScriptedPayloadSerializer();
        await using var stream = new OccasionallyConnectedStream<CounterState, CounterInput>(new()
        {
            Definition = CreateDefinition(),
            Store = store,
            Serializer = serializer,
            TimeProvider = new FixedTimeProvider(Now),
            OperationIdSource = new SequenceOperationIdSource(),
            Coordinator = coordinator,
            InputProducer = new RecordingInputProducer<CounterInput>(),
            LocalStateSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterStateSnapshot(payload)),
            RemoteInputSnapshotFactory = static (payload, _) => new(ScriptedPayloadSerializer.CreateCounterInputSnapshot(payload)),
            NotificationScheduler = new ControlledObserverScheduler(),
            NotificationOptions = new(NotificationCapacity, NotificationCapacityBytes, ObserverNotificationOverflowMode.CoalesceLatest),
            WorkCapacity = SingleWorkCapacity,
            LocalAdmissionRetainedBytes = ConfiguredTypedAdmissionBytes,
            ClientId = ClientId,
        });

        var first = stream.PublishAsync(new(FirstValue), null, CancellationToken.None).AsTask();
        await identityEntered.Task.WaitAsync(GuardTimeout);
        var second = stream.PublishAsync(new(SecondValue), null, CancellationToken.None).AsTask();
        await capacityWaitEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(coordinator.LastAdmissionRetainedBytes).IsEqualTo(ConfiguredTypedAdmissionBytes);
        await Assert.That(coordinator.LastCapacityWaitRetainedBytes).IsEqualTo(ConfiguredTypedAdmissionBytes);
        releaseIdentity.SetResult();
        _ = await first.WaitAsync(GuardTimeout);
        releaseCapacityWait.SetResult();
        var receipt = await second.WaitAsync(GuardTimeout);

        await Assert.That(receipt.ClientSequence).IsEqualTo(SecondSequence);
        await Assert.That(coordinator.CompletedAdmissions).IsEqualTo(ExpectedCompletedAdmissions);
    }

    /// <summary>Records retained-byte charges passed through the stream coordinator.</summary>
    /// <param name="store">The backing store used for durable identity.</param>
    private sealed class ByteRecordingCoordinator(ILocalStoreAdapter store) : IOccasionallyConnectedStreamCoordinator
    {
        /// <summary>Gets the optional signal set when identity resolution starts.</summary>
        public TaskCompletionSource? IdentityEntered { get; init; }

        /// <summary>Gets the optional signal that releases identity resolution.</summary>
        public TaskCompletionSource? ReleaseIdentity { get; init; }

        /// <summary>Gets the optional signal set when capacity wait begins.</summary>
        public TaskCompletionSource? CapacityWaitEntered { get; init; }

        /// <summary>Gets the optional signal that releases capacity wait.</summary>
        public TaskCompletionSource? ReleaseCapacityWait { get; init; }

        /// <summary>Gets the last retained-byte charge used for local admission.</summary>
        public long LastAdmissionRetainedBytes { get; private set; }

        /// <summary>Gets the last retained-byte charge used for capacity waiting.</summary>
        public long LastCapacityWaitRetainedBytes { get; private set; }

        /// <summary>Gets the number of completed admissions.</summary>
        public int CompletedAdmissions { get; private set; }

        /// <inheritdoc/>
        public IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant)
        {
            ArgumentNullException.ThrowIfNull(participant);
            return new Registration();
        }

        /// <inheritdoc/>
        public async ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken)
        {
            _ = IdentityEntered?.TrySetResult();
            if (ReleaseIdentity is not null)
            {
                await ReleaseIdentity.Task.ConfigureAwait(false);
            }

            return await store.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(StreamId streamId, long retainedBytes, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LastAdmissionRetainedBytes = retainedBytes;
            return new(new LocalCommitAdmission(streamId, retainedBytes, Guid.NewGuid()));
        }

        /// <inheritdoc/>
        public void CompleteLocalCommit(LocalCommitAdmission admission)
        {
            _ = admission;
            CompletedAdmissions++;
        }

        /// <inheritdoc/>
        public long GetCapacityReleaseGeneration(StreamId streamId)
        {
            _ = streamId;
            return 1;
        }

        /// <inheritdoc/>
        public async ValueTask WaitForCapacityReleaseAsync(
            StreamId streamId,
            long observedGeneration,
            long retainedBytes,
            CancellationToken cancellationToken)
        {
            _ = streamId;
            _ = observedGeneration;
            LastCapacityWaitRetainedBytes = retainedBytes;
            cancellationToken.ThrowIfCancellationRequested();
            _ = CapacityWaitEntered?.TrySetResult();
            if (ReleaseCapacityWait is not null)
            {
                await ReleaseCapacityWait.Task.ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken)
        {
            _ = streamId;
            cancellationToken.ThrowIfCancellationRequested();
            return default;
        }

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken) =>
            StartStreamAsync(streamId, cancellationToken);

        /// <inheritdoc/>
        public void RecordSavedLocalCommit(
            StreamId streamId,
            SyncOperation operation,
            QueueDiagnosticSnapshot snapshot,
            PublishReceipt receipt)
        {
            _ = snapshot;
            _ = receipt;
            NotifyLocalCommitReady(streamId, operation);
        }

        /// <inheritdoc/>
        public void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation)
        {
            _ = streamId;
            _ = operation;
        }

        /// <inheritdoc/>
        public void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot)
        {
            _ = streamId;
            _ = snapshot;
        }

        /// <inheritdoc/>
        public void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null)
        {
            _ = streamId;
            _ = priority;
            _ = notBeforeUtc;
        }

        /// <inheritdoc/>
        public void NotifyCapacityReleased(StreamId streamId) => _ = streamId;

        /// <summary>Participant registration handle.</summary>
        private sealed class Registration : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose()
            {
            }
        }
    }
}
