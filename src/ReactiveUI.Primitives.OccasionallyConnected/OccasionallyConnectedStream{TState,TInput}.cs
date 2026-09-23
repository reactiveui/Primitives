// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Concrete typed facade for a locally committed occasionally connected stream.</summary>
/// <typeparam name="TState">The local state type.</typeparam>
/// <typeparam name="TInput">The input value type.</typeparam>
internal sealed partial class OccasionallyConnectedStream<TState, TInput> :
    IOccasionallyConnectedStream<TState, TInput>,
    IOccasionallyConnectedStreamParticipant,
    IOccasionallyConnectedStreamDiagnosticsSink,
    IReportsSavedLocalCommitDiagnostics,
    IOccasionallyConnectedSerializedInputPublisher,
    IOccasionallyConnectedCommittedStateQueueSnapshots<TState>
{
    /// <summary>The minimum byte size assigned to non-payload notifications.</summary>
    private const long MinimumNotificationSizeBytes = 1;

    /// <summary>The stable fault code for an unavailable durable operation status.</summary>
    private const string OperationStatusFaultCode = "OC.Stream.OperationStatus";

    /// <summary>Protects identity, latest-state, and lifecycle fields.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores immutable facade options.</summary>
    private readonly OccasionallyConnectedStreamOptions<TState, TInput> _options;

    /// <summary>Serializes typed store mutations so the fail-fast committer never sees overlap.</summary>
    private readonly BoundedSerializedStreamWorkLane _workLane;

    /// <summary>Owns the public synchronous input observer.</summary>
    private readonly IOccasionallyConnectedInputProducer<TInput> _inputProducer;

    /// <summary>Dispatches local state notifications.</summary>
    private readonly ObserverNotificationDispatcher<TState> _local;

    /// <summary>Dispatches remote message notifications.</summary>
    private readonly ObserverNotificationDispatcher<RemoteMessage<TInput>> _remote;

    /// <summary>Dispatches synchronization state notifications.</summary>
    private readonly ObserverNotificationDispatcher<SyncState> _syncStates;

    /// <summary>Dispatches operation state notifications.</summary>
    private readonly ObserverNotificationDispatcher<SyncOperationStatus> _operationStates;

    /// <summary>Dispatches stream fault notifications.</summary>
    private readonly ObserverNotificationDispatcher<OccasionallyConnectedFault> _faults;

    /// <summary>Stores the context-owned participant registration.</summary>
    private readonly IDisposable _participantRegistration;

    /// <summary>Stores the shared initialization task after the first asynchronous use.</summary>
    private Task<LocalStreamCommitter<TState, TInput>>? _initializeTask;

    /// <summary>Stores the serialized lifecycle convergence task.</summary>
    private Task _lifecycleTask = Task.CompletedTask;

    /// <summary>Stores the last exception published as a lifecycle fault to suppress duplicate continuation reports.</summary>
    private Exception? _lastLifecycleFault;

    /// <summary>Stores the shared disposal task after the first disposal call.</summary>
    private Task? _disposeTask;

    /// <summary>Stores the latest local state for replaying new local subscribers.</summary>
    private LatestLocal? _latestLocal;

    /// <summary>Stores the durable subscription identity once configured or resolved.</summary>
    private SubscriptionId? _subscriptionId;

    /// <summary>Tracks whether stream lifecycle has been started.</summary>
    private bool _started;

    /// <summary>Tracks the desired stream lifecycle state.</summary>
    private bool _desiredStarted;

    /// <summary>Tracks whether remote work is explicitly parked for this stream.</summary>
    private bool _remoteStopped;

    /// <summary>Tracks a pending remote stop request that does not require typed initialization.</summary>
    private bool _stopRequested;

    /// <summary>Tracks whether the stream has been disposed.</summary>
    private bool _disposed;

    /// <summary>Rejects engine diagnostic notifications overtaken by a newer state.</summary>
    private long _lastSyncDiagnosticRevision;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedStream{TState,TInput}"/> class.</summary>
    /// <param name="options">The stream options.</param>
    internal OccasionallyConnectedStream(OccasionallyConnectedStreamOptions<TState, TInput> options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _subscriptionId = GetPreferredSubscriptionId(options.Definition);
        _workLane = new(options.WorkCapacity);
        _faults = new(options.NotificationScheduler);
        _committedStateQueueSnapshots = new(options.NotificationScheduler, ReportObserverFault);
        _local = new(options.NotificationScheduler, ReportObserverFault);
        _remote = new(options.NotificationScheduler, ReportObserverFault);
        _syncStates = new(options.NotificationScheduler, ReportObserverFault);
        _operationStates = new(options.NotificationScheduler, ReportObserverFault);
        _inputProducer = options.InputProducer ?? CreateInputProducer();
        _participantRegistration = options.Coordinator.RegisterParticipant(this);
    }

    /// <inheritdoc />
    public StreamId StreamId => _options.Definition.StreamId;

    /// <inheritdoc />
    public SubscriptionId SubscriptionId
    {
        get
        {
            lock (_gate)
            {
                ThrowIfDisposed();
                if (_subscriptionId is { } subscriptionId)
                {
                    return subscriptionId;
                }
            }

            throw new InvalidOperationException("SubscriptionId is unavailable until the stream has initialized.");
        }
    }

    /// <inheritdoc />
    public IObservable<TState> Local => new LocalObservable(this);

    /// <inheritdoc />
    public IObservable<RemoteMessage<TInput>> Remote => new DispatcherObservable<RemoteMessage<TInput>>(_remote, _options.NotificationOptions);

    /// <inheritdoc />
    public IObservable<SyncState> SyncStates => new DispatcherObservable<SyncState>(_syncStates, _options.NotificationOptions);

    /// <inheritdoc />
    public IObservable<SyncOperationStatus> OperationStates => new DispatcherObservable<SyncOperationStatus>(_operationStates, _options.NotificationOptions);

    /// <inheritdoc />
    public IObservable<OccasionallyConnectedFault> Faults => new DispatcherObservable<OccasionallyConnectedFault>(_faults, _options.NotificationOptions);

    /// <inheritdoc />
    public IObserver<TInput> Input => _inputProducer.Observer;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool IReportsSavedLocalCommitDiagnostics.ReportsSavedLocalCommitTo(SyncEngine engine) =>
        ReferenceEquals(_options.Coordinator, engine);

    /// <inheritdoc />
    public IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> CommittedStateQueueSnapshots =>
        new PairedObservable(this);

    /// <inheritdoc />
    void IOccasionallyConnectedStreamDiagnosticsSink.PublishSyncState(SyncState state, long revision)
    {
        var schedules = new List<Action>();
        lock (_gate)
        {
            if (_disposed || revision <= _lastSyncDiagnosticRevision)
            {
                return;
            }

            _lastSyncDiagnosticRevision = revision;
            _ = _syncStates.PublishLatestDeferred(_ => new(state), MinimumNotificationSizeBytes, schedules);
        }

        for (var i = 0; i < schedules.Count; i++)
        {
            schedules[i]();
        }
    }

    /// <inheritdoc />
    public ValueTask<PublishReceipt> PublishAsync(
        TInput value,
        RemotePublishOptions? options,
        CancellationToken cancellationToken) =>
        new(PublishWithAdmissionAsync(value, options, cancellationToken));

    /// <inheritdoc/>
    async ValueTask<ReceiveStreamSubscription?> IOccasionallyConnectedStreamParticipant.PrepareReceiveAsync(CancellationToken cancellationToken)
    {
        var subscription = _options.Definition.Subscription;
        if (subscription is null)
        {
            return null;
        }

        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        SubscriptionId subscriptionId;
        lock (_gate)
        {
            subscriptionId = _subscriptionId ?? throw new InvalidOperationException("SubscriptionId is unavailable until the stream has initialized.");
        }

        return new(StreamId, subscriptionId, committer.Current.ServerCursor, subscription.StartPosition, subscription.DeliveryGuarantee);
    }

    /// <inheritdoc/>
    async ValueTask<PublishReceipt> IOccasionallyConnectedStreamParticipant.CommitSerializedAsync(
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        while (true)
        {
            var generation = _options.Coordinator.GetCapacityReleaseGeneration(StreamId);
            try
            {
                Task<PublishReceipt> task;
                lock (_gate)
                {
                    ThrowIfDisposed();
                    task = _workLane.EnqueueAsync(token => CommitSerializedCoreAsync(operation, token), cancellationToken);
                }

                return await task.ConfigureAwait(false);
            }
            catch (QueueCapacityExceededException exception) when (exception.CanFitWhenEmpty)
            {
                var retainedBytes = operation.Payload.Payload.IsEmpty
                    ? SyncEngine.UnknownProducerRetainedBytes
                    : operation.Payload.Payload.Length;
                await _options.Coordinator
                    .WaitForCapacityReleaseAsync(StreamId, generation, retainedBytes, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc/>
    ValueTask<ParticipantQueueTransitionResult> IOccasionallyConnectedStreamParticipant.ApplySyncResultAsync(
        SyncBatch batch,
        RemoteSyncResult result,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(result);
        Task<ParticipantQueueTransitionResult> task;
        lock (_gate)
        {
            ThrowIfDisposed();
            task = _workLane.EnqueueAsync(token => ApplySyncResultCoreAsync(batch, result, token), cancellationToken);
        }

        return new(task);
    }

    /// <inheritdoc/>
    async ValueTask<ParticipantRemoteApplyResult> IOccasionallyConnectedStreamParticipant.ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        CancellationToken cancellationToken)
    {
        var result = await ApplyRemoteBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        return new(result.Receipt, result.QueueSnapshot, result.CursorAdvanced);
    }

    /// <inheritdoc/>
    ValueTask<ParticipantQueueTransitionResult> IOccasionallyConnectedStreamParticipant.DeadLetterOperationAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        Task<ParticipantQueueTransitionResult> task;
        lock (_gate)
        {
            ThrowIfDisposed();
            task = _workLane.EnqueueAsync(token => DeadLetterOperationCoreAsync(leaseId, operationId, reasonCode, token), cancellationToken);
        }

        return new(task);
    }

    /// <summary>Starts or resumes remote work for this stream and initializes the typed local committer.</summary>
    /// <param name="cancellationToken">The cancellation token used while waiting for lifecycle convergence.</param>
    /// <returns>The start operation.</returns>
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        var task = SetDesiredLifecycleState(true);
        await WaitForLifecycleAsync(task, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Stops remote work for this stream without closing local publication admission or completing public observers.</summary>
    /// <param name="cancellationToken">The cancellation token used while waiting for lifecycle convergence.</param>
    /// <returns>The stop operation.</returns>
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        var task = SetDesiredLifecycleState(false, forceStop: true);
        await WaitForLifecycleAsync(task, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource<bool>? completion = null;
        Task task;
        lock (_gate)
        {
            if (_disposeTask is null)
            {
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = completion.Task;
            }

            task = _disposeTask;
        }

        if (completion is not null)
        {
            _ = CompleteDisposeAsync(completion);
        }

        return new(task);
    }

    /// <summary>Applies a remote event batch through the typed committer.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable remote apply result.</returns>
    internal ValueTask<RemoteStreamCommitResult<TState, TInput>> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        CancellationToken cancellationToken)
    {
        Task<RemoteStreamCommitResult<TState, TInput>> task;
        lock (_gate)
        {
            ThrowIfDisposed();
            task = _workLane.EnqueueAsync(token => ApplyRemoteBatchCoreAsync(batch, token), cancellationToken);
        }

        return new(task);
    }

    /// <summary>Completes the shared disposal proxy after owned cleanup finishes.</summary>
    /// <param name="completion">The shared disposal completion source.</param>
    /// <returns>The asynchronous completion task.</returns>
    private async Task CompleteDisposeAsync(TaskCompletionSource<bool> completion)
    {
        try
        {
            await DisposeCoreAsync().ConfigureAwait(false);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Disposes owned resources exactly once and exposes the same completion to repeated callers.</summary>
    /// <returns>The asynchronous disposal operation.</returns>
    private async Task DisposeCoreAsync()
    {
        lock (_gate)
        {
            _disposed = true;
        }

        Exception? failure = null;
        failure = await CaptureFailureAsync(
                () => _inputProducer.DisposeAsync().AsTask(),
                failure)
            .ConfigureAwait(false);
        failure = await CaptureFailureAsync(() => SetDesiredLifecycleState(false, allowDisposed: true), failure).ConfigureAwait(false);
        failure = await CaptureFailureAsync(() => _workLane.WhenIdleAsync(CancellationToken.None), failure).ConfigureAwait(false);
        _workLane.Dispose();
        _local.Dispose();
        _remote.Dispose();
        _syncStates.Dispose();
        _operationStates.Dispose();
        _faults.Dispose();
        _committedStateQueueSnapshots.Dispose();
        _participantRegistration.Dispose();
        if (failure is null)
        {
            return;
        }

        throw failure;
    }

    /// <summary>Continues lifecycle convergence after a previous lifecycle transition.</summary>
    /// <param name="previous">The previous lifecycle task.</param>
    /// <returns>The lifecycle convergence task.</returns>
    private async Task ContinueLifecycleAsync(Task previous)
    {
        try
        {
            await previous.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishLifecycleFailureOnce(exception);
        }

        await ApplyDesiredLifecycleStateAsync().ConfigureAwait(false);
    }

    /// <summary>Records the desired lifecycle state and returns the shared convergence task.</summary>
    /// <param name="started">Whether the stream should be started.</param>
    /// <param name="allowDisposed">Whether disposal is allowed to request convergence.</param>
    /// <param name="forceStop">Whether a stop request should park remote work even when the typed facade has not started.</param>
    /// <returns>The convergence task.</returns>
    private Task SetDesiredLifecycleState(bool started, bool allowDisposed = false, bool forceStop = false)
    {
        TaskCompletionSource<bool>? completion = null;
        Task? previous = null;
        Task task;
        lock (_gate)
        {
            if (!allowDisposed)
            {
                ThrowIfDisposed();
            }

            var stopRequired = RecordStopRequestLocked(forceStop);
            var changed = _desiredStarted != started;
            _desiredStarted = started;
            if (!ShouldCreateLifecycleConvergenceLocked(changed, stopRequired))
            {
                task = _lifecycleTask;
            }
            else
            {
                previous = _lifecycleTask;
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _lifecycleTask = completion.Task;
                task = _lifecycleTask;
            }
        }

        if (completion is not null && previous is not null)
        {
            _ = CompleteLifecycleAsync(previous, completion);
        }

        return task;
    }

    /// <summary>Records an explicit remote stop request while the stream gate is held.</summary>
    /// <param name="forceStop">Whether a remote stop should be forced.</param>
    /// <returns><see langword="true"/> when remote work still needs to be stopped.</returns>
    private bool RecordStopRequestLocked(bool forceStop)
    {
        if (!forceStop || _remoteStopped)
        {
            return false;
        }

        _stopRequested = true;
        return true;
    }

    /// <summary>Determines whether a new lifecycle convergence proxy is needed while the stream gate is held.</summary>
    /// <param name="changed">Whether the desired started state changed.</param>
    /// <param name="stopRequired">Whether a remote stop must be applied even without typed startup.</param>
    /// <returns><see langword="true"/> when a new convergence proxy should be created.</returns>
    private bool ShouldCreateLifecycleConvergenceLocked(bool changed, bool stopRequired)
    {
        if (changed)
        {
            return true;
        }

        if (!_lifecycleTask.IsCompleted)
        {
            return false;
        }

        return stopRequired || !IsCompletedSuccessfully(_lifecycleTask);
    }

    /// <summary>Completes a lifecycle convergence proxy after coordinator work finishes.</summary>
    /// <param name="previous">The previous lifecycle task.</param>
    /// <param name="completion">The proxy completion source.</param>
    /// <returns>The asynchronous completion task.</returns>
    private async Task CompleteLifecycleAsync(Task previous, TaskCompletionSource<bool> completion)
    {
        try
        {
            if (!IsCompletedSuccessfully(previous))
            {
                await ContinueLifecycleAsync(previous).ConfigureAwait(false);
            }
            else
            {
                await ApplyDesiredLifecycleStateAsync().ConfigureAwait(false);
            }

            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            PublishLifecycleFailureOnce(exception);
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Converges actual stream lifecycle to the most recent desired state.</summary>
    /// <returns>The convergence task.</returns>
    private async Task ApplyDesiredLifecycleStateAsync()
    {
        while (true)
        {
            bool shouldBeStarted;
            bool isStarted;
            bool shouldStop;
            lock (_gate)
            {
                shouldBeStarted = _desiredStarted;
                isStarted = _started;
                shouldStop = !shouldBeStarted && _stopRequested && !_remoteStopped;
            }

            if (shouldBeStarted == isStarted && !shouldStop)
            {
                return;
            }

            if (shouldBeStarted)
            {
                await _options.Coordinator.StartStreamAsync(StreamId, CancellationToken.None).ConfigureAwait(false);
                _ = await _workLane.EnqueueAsync(token => EnsureInitializedCoreAsync(token), CancellationToken.None).ConfigureAwait(false);
                lock (_gate)
                {
                    _started = true;
                    _remoteStopped = false;
                    _stopRequested = false;
                }

                continue;
            }

            await _workLane.WhenIdleAsync(CancellationToken.None).ConfigureAwait(false);
            await _options.Coordinator.StopStreamAsync(StreamId, CancellationToken.None).ConfigureAwait(false);
            lock (_gate)
            {
                _started = false;
                _remoteStopped = true;
                _stopRequested = false;
            }
        }
    }

    /// <summary>Runs one typed local publish inside the serialized stream lane.</summary>
    /// <param name="value">The caller input value.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    private async ValueTask<PublishReceipt> PublishCoreAsync(
        TInput value,
        RemotePublishOptions? options,
        CancellationToken cancellationToken)
    {
        ValidatePublishOptions(options);
        var committer = await EnsureInitializedCoreAsync(cancellationToken, allowDisposed: true).ConfigureAwait(false);
        var effectiveOptions = options ?? _options.Definition.Publish;
        var result = await committer.CommitAsync(value, CreatePolicy(effectiveOptions), effectiveOptions?.BaseVersion, cancellationToken)
            .ConfigureAwait(false);
        NotifyCommitReady(result);
        var committedPayload = await PublishLocalAsync(result.State, result.Receipt.OperationId, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, result.QueueSnapshot);
        }

        PublishOperationStatus(result.Receipt);
        return result.Receipt;
    }

    /// <summary>Runs one producer-captured serialized input inside the serialized stream lane.</summary>
    /// <param name="payload">The owned input payload.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    private ValueTask<PublishReceipt> PublishSerializedAsync(
        PayloadEnvelope payload,
        RemotePublishOptions? options,
        CancellationToken cancellationToken) =>
        new(PublishAdmittedAsync(
            token => PublishSerializedCoreAsync(payload, options, token),
            GetNotificationSize(payload),
            options,
            allowDisposed: true,
            cancellationToken));

    /// <summary>Commits one producer-captured serialized input using the initialized local committer.</summary>
    /// <param name="payload">The owned input payload.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    private async ValueTask<PublishReceipt> PublishSerializedCoreAsync(
        PayloadEnvelope payload,
        RemotePublishOptions? options,
        CancellationToken cancellationToken)
    {
        ValidatePublishOptions(options);
        var committer = await EnsureInitializedCoreAsync(cancellationToken, allowDisposed: true).ConfigureAwait(false);
        var effectiveOptions = options ?? _options.Definition.Publish;
        var result = await committer.CommitSerializedAsync(
                payload,
                CreatePolicy(effectiveOptions),
                effectiveOptions?.BaseVersion,
                cancellationToken)
            .ConfigureAwait(false);
        NotifyCommitReady(result);
        var committedPayload = await PublishLocalAsync(result.State, result.Receipt.OperationId, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, result.QueueSnapshot);
        }

        PublishOperationStatus(result.Receipt);
        return result.Receipt;
    }

    /// <summary>Runs one serialized local publish inside the serialized stream lane.</summary>
    /// <param name="operation">The serialized operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    private async ValueTask<PublishReceipt> CommitSerializedCoreAsync(
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        var result = await committer.CommitSerializedAsync(operation, cancellationToken).ConfigureAwait(false);
        NotifyCommitReady(result);
        var committedPayload = await PublishLocalAsync(result.State, result.Receipt.OperationId, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, result.QueueSnapshot);
        }

        PublishOperationStatus(result.Receipt);
        return result.Receipt;
    }

    /// <summary>Admits and runs one typed local publish outside the serialized lane when capacity waits are required.</summary>
    /// <param name="value">The caller input value.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task<PublishReceipt> PublishWithAdmissionAsync(
        TInput value,
        RemotePublishOptions? options,
        CancellationToken cancellationToken) =>
        PublishAdmittedAsync(
            token => PublishCoreAsync(value, options, token),
            _options.LocalAdmissionRetainedBytes,
            options,
            allowDisposed: false,
            cancellationToken);

    /// <summary>Tracks one admitted publish through serialization, capacity waits, and durable completion.</summary>
    /// <param name="publish">The typed or owned-payload commit operation.</param>
    /// <param name="retainedBytes">The retained input charge for admission and capacity waits.</param>
    /// <param name="options">The optional publish options.</param>
    /// <param name="allowDisposed">Whether a previously accepted input may drain during stream disposal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable publish receipt.</returns>
    private async Task<PublishReceipt> PublishAdmittedAsync(
        Func<CancellationToken, ValueTask<PublishReceipt>> publish,
        long retainedBytes,
        RemotePublishOptions? options,
        bool allowDisposed,
        CancellationToken cancellationToken)
    {
        ValidatePublishOptions(options);
        var admission = await _options.Coordinator.EnterLocalCommitAsync(StreamId, retainedBytes, cancellationToken).ConfigureAwait(false);
        try
        {
            while (true)
            {
                var generation = _options.Coordinator.GetCapacityReleaseGeneration(StreamId);
                try
                {
                    Task<PublishReceipt> task;
                    lock (_gate)
                    {
                        if (!allowDisposed)
                        {
                            ThrowIfDisposed();
                        }

                        task = _workLane.EnqueueAsync(publish, cancellationToken);
                    }

                    return await task.ConfigureAwait(false);
                }
                catch (QueueCapacityExceededException exception) when (ShouldWaitForCapacity(exception, options))
                {
                    await _options.Coordinator
                        .WaitForCapacityReleaseAsync(StreamId, generation, retainedBytes, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _options.Coordinator.CompleteLocalCommit(admission);
        }
    }

    /// <summary>Runs one typed remote apply inside the serialized stream lane.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable remote apply result.</returns>
    private async ValueTask<RemoteStreamCommitResult<TState, TInput>> ApplyRemoteBatchCoreAsync(
        RemoteEventBatch batch,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        var result = await committer.ApplyRemoteBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        PublishRemote(result);
        var committedPayload = await PublishLocalAsync(result.State, null, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, result.QueueSnapshot);
        }

        return result;
    }

    /// <summary>Runs one upload reconciliation inside the serialized stream lane.</summary>
    /// <param name="batch">The exact upload batch.</param>
    /// <param name="result">The remote upload result.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The reconciliation task.</returns>
    private async ValueTask<ParticipantQueueTransitionResult> ApplySyncResultCoreAsync(
        SyncBatch batch,
        RemoteSyncResult result,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        var state = await committer.ApplySyncResultAsync(batch, result, cancellationToken).ConfigureAwait(false);
        var queueSnapshot = committer.RecoveredQueueSnapshot;
        var committedPayload = await PublishLocalAsync(state, null, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, queueSnapshot);
        }

        await PublishOperationStatusesAsync(result).ConfigureAwait(false);
        return new(queueSnapshot);
    }

    /// <summary>Runs one local dead-letter reconciliation inside the serialized stream lane.</summary>
    /// <param name="leaseId">The active upload lease.</param>
    /// <param name="operationId">The operation to dead-letter.</param>
    /// <param name="reasonCode">The stable local reason code.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The dead-letter task.</returns>
    private async ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationCoreAsync(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        var state = await committer.DeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken).ConfigureAwait(false);
        var queueSnapshot = committer.RecoveredQueueSnapshot;
        var committedPayload = await PublishLocalAsync(state, null, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, queueSnapshot);
        }

        try
        {
            var status = await _options.Store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            if (status is not null)
            {
                _ = _operationStates.PublishEvent(status, GetOperationStatusNotificationSize(status));
            }
            else
            {
                PublishFault(
                    OperationStatusFaultCode,
                    "The durable operation status was unavailable after a dead-letter commit.",
                    operationId,
                    new InvalidOperationException("The durable operation status was unavailable."));
            }
        }
        catch (Exception exception)
        {
            PublishFault(
                OperationStatusFaultCode,
                "The durable operation status could not be read after a dead-letter commit.",
                operationId,
                exception);
        }

        return new(queueSnapshot);
    }

    /// <summary>Ensures durable identity and recovery have completed.</summary>
    /// <param name="cancellationToken">The cancellation token used by the first initializer.</param>
    /// <param name="allowDisposed">A value indicating whether previously admitted publications may initialize during disposal.</param>
    /// <returns>The initialized typed committer.</returns>
    private async ValueTask<LocalStreamCommitter<TState, TInput>> EnsureInitializedCoreAsync(
        CancellationToken cancellationToken,
        bool allowDisposed = false)
    {
        var task = GetOrCreateInitializeTask(allowDisposed);
        return await WaitForInitializationAsync(task, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Gets or creates the shared initialization task.</summary>
    /// <param name="allowDisposed">A value indicating whether previously admitted publications may initialize during disposal.</param>
    /// <returns>The initialization task.</returns>
    private Task<LocalStreamCommitter<TState, TInput>> GetOrCreateInitializeTask(bool allowDisposed)
    {
        TaskCompletionSource<LocalStreamCommitter<TState, TInput>>? completion = null;
        Task<LocalStreamCommitter<TState, TInput>> task;
        lock (_gate)
        {
            if (_initializeTask is null)
            {
                if (!allowDisposed)
                {
                    ThrowIfDisposed();
                }

                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _initializeTask = completion.Task;
            }

            task = _initializeTask;
        }

        if (completion is not null)
        {
            _ = CompleteInitializeAsync(completion);
        }

        return task;
    }

    /// <summary>Completes the shared initialization proxy.</summary>
    /// <param name="completion">The proxy completion source.</param>
    /// <returns>The asynchronous completion task.</returns>
    private async Task CompleteInitializeAsync(TaskCompletionSource<LocalStreamCommitter<TState, TInput>> completion)
    {
        try
        {
            var committer = await InitializeCoreAsync(CancellationToken.None).ConfigureAwait(false);
            _ = completion.TrySetResult(committer);
        }
        catch (Exception exception)
        {
            PublishLifecycleFailureOnce(exception);
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Publishes a sanitized lifecycle fault once for each distinct exception instance.</summary>
    /// <param name="exception">The lifecycle failure.</param>
    private void PublishLifecycleFailureOnce(Exception exception)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_lastLifecycleFault, exception))
            {
                return;
            }

            _lastLifecycleFault = exception;
        }

        PublishFault("OC.Stream.Lifecycle", "A previous stream lifecycle transition failed.", null, exception);
    }

    /// <summary>Resolves durable identity, constructs the committer, and recovers local state.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The initialized committer.</returns>
    private async Task<LocalStreamCommitter<TState, TInput>> InitializeCoreAsync(CancellationToken cancellationToken)
    {
        var preferredId = GetPreferredSubscriptionId(_options.Definition);
        var subscriptionId = await _options.Coordinator.EnsureSubscriptionIdAsync(StreamId, preferredId, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        var committer = new LocalStreamCommitter<TState, TInput>(new()
        {
            StreamId = StreamId,
            SubscriptionId = subscriptionId,
            ClientId = _options.ClientId,
            Contracts = new()
            {
                InputContractId = _options.Definition.InputContractId,
                InputSchemaVersion = _options.Definition.InputSchemaVersion,
                StateContractId = _options.Definition.StateContractId,
                StateSchemaVersion = _options.Definition.StateSchemaVersion,
                SnapshotFormatVersion = _options.Definition.SnapshotFormatVersion,
            },
            Dependencies = new()
            {
                Store = _options.Store,
                Serializer = _options.Serializer,
                Projection = _options.Definition.Projection,
                TimeProvider = _options.TimeProvider,
                OperationIdSource = _options.OperationIdSource,
            },
            MinimumPriority = _options.MinimumPriority,
            MaximumPriority = _options.MaximumPriority,
        });
        var state = await committer.RecoverAsync(cancellationToken).ConfigureAwait(false);
        _options.Coordinator.RecordRecoveredQueueAggregate(StreamId, committer.RecoveredQueueSnapshot);
        if (committer.RecoveredUploadHead is { } recoveredUploadHead)
        {
            _options.Coordinator.NotifyRecoveredLocalWorkReady(StreamId, recoveredUploadHead.Priority, recoveredUploadHead.NotBeforeUtc);
        }

        lock (_gate)
        {
            _subscriptionId = subscriptionId;
        }

        var committedPayload = await PublishLocalAsync(state, null, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, committer.RecoveredQueueSnapshot);
        }

        return committer;
    }

    /// <summary>Validates call-specific or definition-level publish options.</summary>
    /// <param name="options">The call-specific options.</param>
    /// <exception cref="InvalidOperationException">The effective publish options target a different stream.</exception>
    private void ValidatePublishOptions(RemotePublishOptions? options)
    {
        var effective = options ?? _options.Definition.Publish;
        if (effective is null)
        {
            return;
        }

        effective.Validate();
        if (effective.StreamId == StreamId)
        {
            return;
        }

        throw new InvalidOperationException("Publish options StreamId must match the stream.");
    }

    /// <summary>Determines whether a capacity failure should wait for a release notification.</summary>
    /// <param name="exception">The capacity exception.</param>
    /// <param name="options">The call-specific publish options.</param>
    /// <returns>Whether the publish should wait and retry.</returns>
    private bool ShouldWaitForCapacity(QueueCapacityExceededException exception, RemotePublishOptions? options)
    {
        var effective = options ?? _options.Definition.Publish;
        return exception.CanFitWhenEmpty && (effective is null || effective.AdmissionStrategy == BufferStrategy.Block);
    }

    /// <summary>Publishes an isolated local state snapshot and records its payload for future latest replay.</summary>
    /// <param name="state">The current committed local state.</param>
    /// <param name="operationId">The optional local operation identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned committed payload, or null when notification preparation fails.</returns>
    private async ValueTask<PayloadEnvelope?> PublishLocalAsync(
        LocalStreamCommitterState<TState> state,
        OperationId? operationId,
        CancellationToken cancellationToken)
    {
        PayloadEnvelope payload;
        try
        {
            payload = await GetCommittedStatePayloadAsync(state, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Stream.LocalSnapshot", "The stream local notification snapshot failed.", operationId, exception);
            return null;
        }

        var schedules = new List<Action>();
        var factory = CreateLocalSnapshotFactory(payload);
        var sizeBytes = GetNotificationSize(payload);
        lock (_gate)
        {
            _latestLocal = new(payload);
            _ = _local.PublishLatestDeferred(factory, sizeBytes, schedules);
        }

        RunNotificationSchedules(schedules);
        return payload;
    }

    /// <summary>Gets the durable payload backing a local notification snapshot.</summary>
    /// <param name="state">The current committed local state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The snapshot payload.</returns>
    private async ValueTask<PayloadEnvelope> GetCommittedStatePayloadAsync(
        LocalStreamCommitterState<TState> state,
        CancellationToken cancellationToken) =>
        state.MaterializedPayload is { } materialized
            ? materialized
            : await _options.Serializer
            .SerializeAsync(
                _options.Definition.StateContractId,
                _options.Definition.StateSchemaVersion,
                state.State,
                cancellationToken)
            .ConfigureAwait(false);

    /// <summary>Creates a factory that materializes a fresh local state instance from a committed payload.</summary>
    /// <param name="payload">The committed payload.</param>
    /// <returns>The local state snapshot factory.</returns>
    private Func<CancellationToken, ValueTask<TState>> CreateLocalSnapshotFactory(PayloadEnvelope payload) =>
        cancellationToken => DeserializeLocalSnapshotAsync(payload, cancellationToken);

    /// <summary>Deserializes one local notification snapshot.</summary>
    /// <param name="payload">The committed payload.</param>
    /// <param name="cancellationToken">The subscription cancellation token.</param>
    /// <returns>The isolated local state.</returns>
    /// <exception cref="InvalidOperationException">The serializer returns the wrong state type.</exception>
    private async ValueTask<TState> DeserializeLocalSnapshotAsync(PayloadEnvelope payload, CancellationToken cancellationToken)
    {
        var snapshot = await _options.LocalStateSnapshotFactory(payload, cancellationToken).ConfigureAwait(false);
        return snapshot is not null
            ? snapshot
            : throw new InvalidOperationException("The local notification snapshot materializer returned null.");
    }

    /// <summary>Subscribes to local state with atomic latest replay.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>The subscription handle.</returns>
    private IDisposable SubscribeLocal(IObserver<TState> observer)
    {
        var schedules = new List<Action>(1);
        IDisposable subscription;
        lock (_gate)
        {
            ThrowIfDisposed();
            subscription = _latestLocal is { } latest
                ? _local.SubscribeDeferred(
                    observer,
                    _options.NotificationOptions,
                    true,
                    CreateLocalSnapshotFactory(latest.Payload),
                    GetNotificationSize(latest.Payload),
                    schedules)
                : _local.Subscribe(observer, _options.NotificationOptions);
        }

        RunNotificationSchedules(schedules);
        return subscription;
    }

    /// <summary>Publishes committed remote messages without replay.</summary>
    /// <param name="result">The remote commit result.</param>
    private void PublishRemote(RemoteStreamCommitResult<TState, TInput> result)
    {
        var events = result.Batch.Events;
        for (var i = 0; i < events.Count; i++)
        {
            var remoteEvent = events[i];
            _ = _remote.PublishEvent(
                CreateRemoteSnapshotFactory(remoteEvent),
                GetRemoteNotificationSize(remoteEvent));
        }
    }

    /// <summary>Creates a factory that materializes a fresh remote message from a committed remote payload.</summary>
    /// <param name="remoteEvent">The committed remote event.</param>
    /// <returns>The remote message snapshot factory.</returns>
    private Func<CancellationToken, ValueTask<RemoteMessage<TInput>>> CreateRemoteSnapshotFactory(RemoteEvent remoteEvent)
    {
        var eventId = remoteEvent.EventId;
        var streamId = remoteEvent.StreamId;
        var serverCursor = remoteEvent.ServerCursor;
        var committedAtUtc = remoteEvent.CommittedAtUtc;
        var payload = remoteEvent.Payload;
        return async cancellationToken => new(
            eventId,
            streamId,
            serverCursor,
            committedAtUtc,
            await DeserializeRemoteSnapshotAsync(payload, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>Deserializes one remote input notification snapshot.</summary>
    /// <param name="payload">The committed remote input payload.</param>
    /// <param name="cancellationToken">The subscription cancellation token.</param>
    /// <returns>The isolated remote input.</returns>
    /// <exception cref="InvalidOperationException">The input materializer returned null.</exception>
    private async ValueTask<TInput> DeserializeRemoteSnapshotAsync(PayloadEnvelope payload, CancellationToken cancellationToken)
    {
        var snapshot = await _options.RemoteInputSnapshotFactory(payload, cancellationToken).ConfigureAwait(false);
        return snapshot is not null
            ? snapshot
            : throw new InvalidOperationException("The remote notification snapshot materializer returned null.");
    }

    /// <summary>Publishes the locally saved operation status.</summary>
    /// <param name="receipt">The durable publish receipt.</param>
    private void PublishOperationStatus(PublishReceipt receipt) =>
        _ = _operationStates.PublishEvent(
            new SyncOperationStatus(receipt.OperationId, StreamId, receipt.State, Attempt: 0, receipt.SavedAtUtc, ReasonCode: null),
            GetOperationStatusNotificationSize(StreamId, receipt));

    /// <summary>Publishes locally observed upload result statuses after durable reconciliation.</summary>
    /// <param name="result">The reconciled result.</param>
    /// <returns>The best-effort notification task.</returns>
    private async ValueTask PublishOperationStatusesAsync(RemoteSyncResult result)
    {
        foreach (var remote in result.Operations)
        {
            try
            {
                var status = await _options.Store.GetOperationStatusAsync(remote.OperationId, CancellationToken.None).ConfigureAwait(false);
                if (status is null)
                {
                    PublishFault(
                        OperationStatusFaultCode,
                        "The durable operation status was unavailable after upload reconciliation.",
                        remote.OperationId,
                        new InvalidOperationException("The durable operation status was unavailable."));
                    continue;
                }

                _ = _operationStates.PublishEvent(status, GetOperationStatusNotificationSize(status));
            }
            catch (Exception exception)
            {
                PublishFault(
                    OperationStatusFaultCode,
                    "The durable operation status could not be read after upload reconciliation.",
                    remote.OperationId,
                    exception);
            }
        }
    }

    /// <summary>Notifies the coordinator after a durable local commit.</summary>
    /// <param name="result">The local commit result.</param>
    private void NotifyCommitReady(LocalStreamCommitResult<TState, TInput> result)
    {
        try
        {
            _options.Coordinator.RecordSavedLocalCommit(
                StreamId,
                result.Operation,
                result.QueueSnapshot,
                result.Receipt);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Stream.CommitReady", "The stream commit-ready notification failed.", result.Receipt.OperationId, exception);
        }
    }

    /// <summary>Reports observer callback faults through the stream fault observable.</summary>
    /// <param name="exception">The observer exception.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReportObserverFault(Exception exception) =>
        PublishFault("OC.Stream.Observer", "A stream observer callback failed.", null, exception);

    /// <summary>Publishes a stream fault notification.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The diagnostic message.</param>
    /// <param name="operationId">The optional operation identity.</param>
    /// <param name="exception">The local exception.</param>
    private void PublishFault(string code, string message, OperationId? operationId, Exception exception)
    {
        var diagnostic = CreateDiagnosticException(exception);
        var fault = new OccasionallyConnectedFault(
            code,
            message,
            _options.TimeProvider.GetUtcNow(),
            StreamId,
            operationId,
            diagnostic)
        { Category = FaultCategory.InternalInvariant, Severity = FaultSeverity.Error, IsTransient = false };
        _ = _faults.PublishEvent(fault, GetFaultNotificationSize(fault, diagnostic));
    }

    /// <summary>Creates the stream-owned input producer when observer input is configured.</summary>
    /// <returns>The input producer used by the public observer facade.</returns>
    private IOccasionallyConnectedInputProducer<TInput> CreateInputProducer()
    {
        var definition = _options.Definition;
        return definition.Input is not { } admission || definition.InputCapture is not { } capture
            ? new DisabledInputProducer()
            : new OccasionallyConnectedInputProducer<TInput>(new()
            {
                StreamId = StreamId,
                Admission = admission,
                Capture = capture,
                PublishAsync = PublishSerializedAsync,
                PublishFault = PublishFault,
                PublishOptions = definition.Publish,
            });
    }

    /// <summary>Throws when the stream has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The stream has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Observable wrapper for latest-replaying local state.</summary>
    /// <param name="owner">The owning stream.</param>
    private sealed class LocalObservable(OccasionallyConnectedStream<TState, TInput> owner) : IObservable<TState>
    {
        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<TState> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            return owner.SubscribeLocal(observer);
        }
    }

    /// <summary>Observable wrapper for dispatcher-backed event streams.</summary>
    /// <typeparam name="T">The notification value type.</typeparam>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <param name="options">The subscription options.</param>
    private sealed class DispatcherObservable<T>(
        ObserverNotificationDispatcher<T> dispatcher,
        ObserverNotificationSubscriptionOptions options) : IObservable<T>
    {
        /// <inheritdoc />
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            return dispatcher.Subscribe(observer, options);
        }
    }

    /// <summary>Provides an inert observer when a stream has no observer input bridge.</summary>
    private sealed class DisabledInputProducer : IOccasionallyConnectedInputProducer<TInput>
    {
        /// <inheritdoc />
        public IObserver<TInput> Observer { get; } = new DisabledInputObserver();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;

        /// <summary>Ignores observer signals for streams without observer input configuration.</summary>
        private sealed class DisabledInputObserver : IObserver<TInput>
        {
            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted()
            {
            }

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => ArgumentExceptionHelper.ThrowIfNull(error);

            /// <inheritdoc />
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(TInput value)
            {
            }
        }
    }

    /// <summary>Stores the committed local payload backing the latest replay.</summary>
    /// <param name="Payload">The committed local state payload.</param>
    private sealed record LatestLocal(PayloadEnvelope Payload);
}
