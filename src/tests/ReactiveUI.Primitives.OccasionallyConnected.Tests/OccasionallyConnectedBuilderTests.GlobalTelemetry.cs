// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests global diagnostic subscription behavior for <see cref="OccasionallyConnectedBuilder"/>.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The global state and operation work items queued by one offline publish.</summary>
    private const int OfflinePublishDiagnosticScheduleCount = 2;

    /// <summary>The configured number of pending notifications per diagnostic subscriber.</summary>
    private const int GlobalDiagnosticQueueCapacity = 256;

    /// <summary>One more event than a stalled observer queue can retain.</summary>
    private const int OverflowingDiagnosticEventCount = GlobalDiagnosticQueueCapacity + 1;

    /// <summary>The small serialized body used for direct engine operations.</summary>
    private static readonly byte[] GlobalDiagnosticPayload = "1"u8.ToArray();

    /// <summary>Verifies typed stream commits also appear on the shared engine operation feed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextOperationStatesIncludeTypedStreamSavedReceipt()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var states = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var subscription = context.SyncEngine.OperationStates.Subscribe(states);
        var stream = context.GetOrCreateStream(CreateDefinition());

        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() =>
        {
            sequencer.Drain();
            return states.Values.Exists(status => status.OperationId == receipt.OperationId);
        });
        sequencer.Drain();
        var status = states.Values.Single(item => item.OperationId == receipt.OperationId);
        await Assert.That(status.State).IsEqualTo(receipt.State);
        await Assert.That(status.ChangedAtUtc).IsEqualTo(receipt.SavedAtUtc);
        await Assert.That(states.Values.Count(item => item.OperationId == receipt.OperationId)).IsEqualTo(1);
    }

    /// <summary>Verifies an inline sequencer's slow observer does not hold another observer or a local commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextOperationStatesIsolateBlockingSubscriber()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim release = new();
        var blocking = new BlockingDiagnosticObserver<SyncOperationStatus>(release);
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(new RecordingSequencer()).Build();
        using var blockingSubscription = context.SyncEngine.OperationStates.Subscribe(blocking);
        var healthy = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var healthySubscription = context.SyncEngine.OperationStates.Subscribe(healthy);
        var stream = context.GetOrCreateStream(CreateDefinition());

        try
        {
            var receipt = await context.SyncEngine.EnqueueOperationAsync(CreateGlobalDiagnosticOperation(), CancellationToken.None)
                .AsTask().WaitAsync(GuardTimeout);
            await blocking.Entered.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => healthy.Values.Exists(status => status.OperationId == receipt.OperationId));
            var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
            await Assert.That(recovered.PendingOperations.Any(operation => operation.OperationId == receipt.OperationId)).IsTrue();
        }
        finally
        {
            release.Set();
        }
    }

    /// <summary>Verifies stalled event delivery disconnects after the bounded queue fills.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextOperationStatesDisconnectOnQueuedOverflow()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var observer = new TerminalDiagnosticObserver<SyncOperationStatus>();
        using var subscription = context.SyncEngine.OperationStates.Subscribe(observer);
        _ = context.GetOrCreateStream(CreateDefinition());

        for (var i = 0; i < OverflowingDiagnosticEventCount; i++)
        {
            _ = await context.SyncEngine.EnqueueOperationAsync(CreateGlobalDiagnosticOperation(i + 1), CancellationToken.None);
        }

        await WaitForConditionAsync(() => sequencer.PendingCount >= OfflinePublishDiagnosticScheduleCount);
        sequencer.Drain();
        var error = await observer.Error.WaitAsync(GuardTimeout);
        await Assert.That(error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(observer.Count).IsEqualTo(GlobalDiagnosticQueueCapacity);
    }

    /// <summary>Verifies stalled sequencer work keeps finite capacity after subscriptions are disposed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextSyncStatesBoundDisposedQueuedSequencerWork()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var initial = new RecoveredUploadDiagnosticObserver<SyncState>();
        var initialSubscription = context.SyncStates.Subscribe(initial);
        var stream = context.GetOrCreateStream(CreateDefinition());
        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() =>
        {
            sequencer.Drain();
            return TryFindSyncState(initial, SyncLifecycleStatus.Created, 1, out _);
        });
        initialSubscription.Dispose();

        for (var i = 0; i < GlobalDiagnosticQueueCapacity; i++)
        {
            context.SyncStates.Subscribe(new RecoveredUploadDiagnosticObserver<SyncState>()).Dispose();
        }

        await WaitForConditionAsync(() => sequencer.PendingCount >= GlobalDiagnosticQueueCapacity);
        await Assert.That(() => context.SyncStates.Subscribe(new RecoveredUploadDiagnosticObserver<SyncState>()))
            .ThrowsExactly<InvalidOperationException>();

        sequencer.Drain();
        var resumed = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var resumedSubscription = context.SyncStates.Subscribe(resumed);
        await WaitForConditionAsync(() =>
        {
            sequencer.Drain();
            return TryFindSyncState(resumed, SyncLifecycleStatus.Created, 1, out _);
        });
    }

    /// <summary>Verifies context operation notifications wait for the configured public sequencer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextOperationStatesWaitForConfiguredSequencer()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var states = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var subscription = context.SyncEngine.OperationStates.Subscribe(states);
        _ = context.GetOrCreateStream(CreateDefinition());

        var receipt = await context.SyncEngine.EnqueueOperationAsync(CreateGlobalDiagnosticOperation(), CancellationToken.None);
        await Assert.That(states.Values.Count).IsEqualTo(0);
        await WaitForConditionAsync(() => sequencer.PendingCount > 0);
        await WaitForConditionAsync(() =>
        {
            sequencer.Drain();
            return states.Values.Exists(status => status.OperationId == receipt.OperationId);
        });
        await Assert.That(states.Values.Count(status => status.OperationId == receipt.OperationId)).IsEqualTo(1);
    }

    /// <summary>Verifies a fault caused by a state observer also waits for the configured public sequencer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextFaultsWaitForConfiguredSequencer()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        await using var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        using var throwing = context.SyncStates.Subscribe(new ThrowingContextSyncObserver());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var subscription = context.SyncEngine.Faults.Subscribe(faults);
        var stream = context.GetOrCreateStream(CreateDefinition());

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => sequencer.PendingCount > 0);
        await Assert.That(faults.Values.Count).IsEqualTo(0);
        sequencer.Drain();
        await Assert.That(faults.Values.Count).IsEqualTo(0);
        await WaitForConditionAsync(() =>
        {
            sequencer.Drain();
            return faults.Values.Exists(static fault => fault.Code == "OC.Engine.SyncStateObserver");
        });
    }

    /// <summary>Verifies disposal discards queued operation callbacks while preserving the durable commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextDisposalDropsQueuedOperationCallbackAfterDurableCommit()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new PausingDiagnosticSequencer();
        var context = CreateReadyBuilder(store, transport).UseSequencer(sequencer).Build();
        var states = new RecoveredUploadDiagnosticObserver<SyncOperationStatus>();
        using var subscription = context.SyncEngine.OperationStates.Subscribe(states);
        var stream = context.GetOrCreateStream(CreateDefinition());

        var receipt = await context.SyncEngine.EnqueueOperationAsync(CreateGlobalDiagnosticOperation(), CancellationToken.None);
        await WaitForConditionAsync(() => sequencer.PendingCount >= OfflinePublishDiagnosticScheduleCount);
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Any(operation => operation.OperationId == receipt.OperationId)).IsTrue();

        await context.DisposeAsync();
        sequencer.Drain();
        await Assert.That(states.Values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a late context subscriber receives the latest committed queue state without another mutation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextSyncStatesReplayLatestQueueToLateSubscriber()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        var early = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var earlySubscription = context.SyncStates.Subscribe(early);
        var stream = context.GetOrCreateStream(CreateDefinition());
        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => TryFindSyncState(early, SyncLifecycleStatus.Created, 1, out _));

        var late = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var lateSubscription = context.SyncStates.Subscribe(late);
        await WaitForConditionAsync(() => TryFindSyncState(late, SyncLifecycleStatus.Created, 1, out _));

        await Assert.That(RequireSyncState(late, SyncLifecycleStatus.Created, 1).PendingBytes)
            .IsEqualTo(RequireSyncState(early, SyncLifecycleStatus.Created, 1).PendingBytes);
    }

    /// <summary>Verifies one application observer cannot prevent another from seeing committed context state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContextSyncStatesIsolateThrowingSubscriber()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        using var throwing = context.SyncStates.Subscribe(new ThrowingContextSyncObserver());
        var healthy = new RecoveredUploadDiagnosticObserver<SyncState>();
        using var subscription = context.SyncStates.Subscribe(healthy);
        var stream = context.GetOrCreateStream(CreateDefinition());
        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);
        await WaitForConditionAsync(() => TryFindSyncState(healthy, SyncLifecycleStatus.Created, 1, out _));

        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Creates a public engine operation for global diagnostic delivery controls.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation to commit through the context engine.</returns>
    private static SyncOperation CreateGlobalDiagnosticOperation(long sequence = 1) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = sequence,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        Type = SyncOperationType.Update,
        Payload = new(InputContract, 1, "application/json", GlobalDiagnosticPayload, "hash"),
        Policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile },
    };

    /// <summary>Models an application synchronization-state observer that fails during delivery.</summary>
    private sealed class ThrowingContextSyncObserver : IObserver<SyncState>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error) => _ = error;

        /// <inheritdoc />
        public void OnNext(SyncState value)
        {
            _ = value;
            throw new InvalidOperationException("The application synchronization-state observer failed.");
        }
    }

    /// <summary>Holds configured observer work until the test releases it.</summary>
    private sealed class PausingDiagnosticSequencer : ISequencer
    {
        /// <summary>Protects queued work items.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores work until the test drains it.</summary>
        private readonly Queue<IWorkItem> _pending = new();

        /// <inheritdoc />
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc />
        public long Timestamp => 0;

        /// <summary>Gets the number of scheduled work items.</summary>
        public int PendingCount
        {
            get
            {
                lock (_gate)
                {
                    return _pending.Count;
                }
            }
        }

        /// <inheritdoc />
        public void Schedule(IWorkItem item)
        {
            lock (_gate)
            {
                _pending.Enqueue(item);
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Executes the currently scheduled work items.</summary>
        public void Drain()
        {
            IWorkItem[] pending;
            lock (_gate)
            {
                pending = [.. _pending];
                _pending.Clear();
            }

            foreach (var item in pending)
            {
                item.Execute();
            }
        }
    }

    /// <summary>Blocks one observer callback until a test releases it.</summary>
    /// <typeparam name="T">The diagnostic type.</typeparam>
    /// <param name="release">The release gate.</param>
    private sealed class BlockingDiagnosticObserver<T>(ManualResetEventSlim release) : IObserver<T>
    {
        /// <summary>Signals that the observer callback began.</summary>
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the callback entry task.</summary>
        internal Task Entered => _entered.Task;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error) => _ = error;

        /// <inheritdoc />
        public void OnNext(T value)
        {
            _ = value;
            _ = _entered.TrySetResult();
            release.Wait();
        }
    }

    /// <summary>Captures a queued event overflow terminal signal.</summary>
    /// <typeparam name="T">The diagnostic type.</typeparam>
    private sealed class TerminalDiagnosticObserver<T> : IObserver<T>
    {
        /// <summary>Completes with the terminal observer error.</summary>
        private readonly TaskCompletionSource<Exception> _error = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores the number of delivered values.</summary>
        private int _count;

        /// <summary>Gets the number of delivered values.</summary>
        internal int Count => Volatile.Read(ref _count);

        /// <summary>Gets the terminal error task.</summary>
        internal Task<Exception> Error => _error.Task;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error) => _ = _error.TrySetResult(error);

        /// <inheritdoc />
        public void OnNext(T value)
        {
            _ = value;
            _ = Interlocked.Increment(ref _count);
        }
    }
}
