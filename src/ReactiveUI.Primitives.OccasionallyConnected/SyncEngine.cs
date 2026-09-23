// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Internal synchronization engine for shared lifecycle, admission, and stream participant dispatch.</summary>
internal sealed partial class SyncEngine : ISyncEngine, IOccasionallyConnectedStreamCoordinator
{
    /// <summary>The minimum retained byte charge for a producer whose exact size is unknown.</summary>
    internal const long UnknownProducerRetainedBytes = 1;

    /// <summary>Protects shared lifecycle, registration, and capacity waiters.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores immutable engine options.</summary>
    private readonly SyncEngineOptions _options;

    /// <summary>Coordinates idempotent global start and stop transitions.</summary>
    private readonly LifecycleTransitionCoordinator _lifecycle;

    /// <summary>Selects ready streams for bounded upload attempts.</summary>
    private readonly FairStreamScheduler _scheduler;

    /// <summary>Stores registered stream participants by stream identity.</summary>
    private readonly Dictionary<StreamId, ParticipantRegistration> _participants = [];

    /// <summary>Stores capacity-release wait state for registered streams.</summary>
    private readonly Dictionary<StreamId, CapacitySignal> _capacitySignals = [];

    /// <summary>Bounds retained local publication inputs across all registered streams.</summary>
    private readonly CapacityBudget _localCommitBudget;

    /// <summary>Bounds capacity waiters across all registered streams.</summary>
    private readonly CapacityBudget _capacityWaiterBudget;

    /// <summary>Stores bounded queue diagnostic aggregates by registered stream.</summary>
    private readonly Dictionary<StreamId, QueueDiagnosticSnapshot> _queueDiagnosticSnapshots = [];

    /// <summary>Tracks streams currently represented by a scheduler head.</summary>
    private readonly HashSet<StreamId> _scheduledStreams = [];

    /// <summary>Tracks streams that need another scheduler head after an active attempt completes.</summary>
    private readonly Dictionary<StreamId, UploadReschedule> _rescheduleStreams = [];

    /// <summary>Stores bounded ready-head timing metadata by stream.</summary>
    private readonly Dictionary<StreamId, UploadHead> _uploadHeads = [];

    /// <summary>Stores upload wakes committed while the engine is stopped or stopping.</summary>
    private readonly Dictionary<StreamId, UploadHead> _deferredUploadHeads = [];

    /// <summary>Stores currently running upload attempt tasks for observed bounded draining.</summary>
    private readonly HashSet<Task> _uploadAttemptTasks = [];

    /// <summary>Publishes synchronization lifecycle states.</summary>
    private readonly BoundedObserverRegistry<SyncState> _syncStates;

    /// <summary>Publishes operation lifecycle states.</summary>
    private readonly BoundedObserverRegistry<SyncOperationStatus> _operationStates;

    /// <summary>Publishes sanitized engine faults.</summary>
    private readonly BoundedObserverRegistry<OccasionallyConnectedFault> _faults;

    /// <summary>Records engine metrics.</summary>
    private readonly OccasionallyConnectedMetrics _metrics;

    /// <summary>Records engine activities.</summary>
    private readonly OccasionallyConnectedActivities _activities;

    /// <summary>Stores remote receive pump tasks for the active session.</summary>
    private readonly List<Task> _receivePumpTasks = [];

    /// <summary>Stores composite receive stop tasks so abandoned caller waits remain observed.</summary>
    private readonly List<Task> _receiveStopTasks = [];

    /// <summary>Tracks streams with a receive pump in the active session.</summary>
    private readonly HashSet<StreamId> _receivePumpStreams = [];

    /// <summary>Stores retired shared-session generations awaiting lease release.</summary>
    private readonly List<SharedSessionLeaseState> _retiredSessionLeases = [];

    /// <summary>Stores the last engine lifecycle status reported to diagnostics.</summary>
    private SyncLifecycleStatus _diagnosticLifecycleStatus;

    /// <summary>Stores whether the active engine lifecycle has a usable network path.</summary>
    private bool _diagnosticNetworkAvailable;

    /// <summary>Orders stream diagnostic notifications captured by concurrent engine paths.</summary>
    private long _diagnosticRevision;

    /// <summary>Stores the latest global state awaiting observer delivery.</summary>
    private SyncState? _pendingGlobalSyncState;

    /// <summary>Rejects delayed global states superseded by newer captured revisions.</summary>
    private long _latestGlobalSyncRevision;

    /// <summary>Tracks the single global diagnostic delivery worker.</summary>
    private bool _globalSyncDeliveryActive;

    /// <summary>Tracks global engine admission state.</summary>
    private EngineAdmissionState _admissionState;

    /// <summary>Stores the shared store initialization task.</summary>
    private Task? _initializeStoreTask;

    /// <summary>Stores the active transport session while the engine is running.</summary>
    private IRemoteTransportSession? _session;

    /// <summary>Tracks the active shared transport session generation.</summary>
    private long _sessionGeneration;

    /// <summary>Stores lease accounting for the active shared transport session.</summary>
    private SharedSessionLeaseState? _sharedSessionLease;

    /// <summary>Stores the current shared-session renewal task.</summary>
    private Task<SessionRenewalResult>? _sessionRenewalTask;

    /// <summary>Stores the shared-session generation currently being renewed.</summary>
    private long _sessionRenewalGeneration;

    /// <summary>Tracks whether the current continuous failure episode already used its renewal.</summary>
    private bool _remoteSessionRenewalUsedSinceProgress;

    /// <summary>Stores the engine-owned cancellation source for upload pump work.</summary>
    private CancellationTokenSource? _uploadCancellation;

    /// <summary>Stores the single upload pump task while the engine is running.</summary>
    private Task? _uploadPumpTask;

    /// <summary>Signals the upload pump when stream work becomes schedulable.</summary>
    private TaskCompletionSource<bool> _uploadSignal = CreateCompletion();

    /// <summary>Completes when currently scheduled and active upload work has drained.</summary>
    private TaskCompletionSource<bool>? _syncCycleWaiter;

    /// <summary>Completes when active upload attempt tasks have drained after pump cancellation.</summary>
    private TaskCompletionSource<bool>? _uploadDrainWaiter;

    /// <summary>Stores the shared disposal task after the first dispose request.</summary>
    private Task? _disposeTask;

    /// <summary>Stores the shared accepted stop task while stop cleanup is running.</summary>
    private Task? _stopTask;

    /// <summary>Completes when the active accepted stop has registered its lifecycle stop intent.</summary>
    private Task? _stopIntentRegisteredTask;

    /// <summary>Completes when all admitted local commits have drained.</summary>
    private TaskCompletionSource<bool>? _drainWaiter;

    /// <summary>Tracks admitted local commits that have not completed.</summary>
    private int _activeLocalCommits;

    /// <summary>Tracks in-flight upload attempts across streams.</summary>
    private int _activeUploadAttempts;

    /// <summary>Tracks whether the upload pump has a pending signal.</summary>
    private bool _uploadSignaled;

    /// <summary>Tracks whether disposal has started.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SyncEngine"/> class.</summary>
    /// <param name="options">The engine options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is malformed.</exception>
    internal SyncEngine(SyncEngineOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _localCommitBudget = new(options.Options.Outbox.MaximumBlockedPublishers, options.Options.Outbox.MaxBytes);
        _capacityWaiterBudget = new(options.Options.Outbox.MaximumBlockedPublishers, options.Options.Outbox.MaxBytes);
        _syncStates = new(options.MaxDiagnosticSubscriptions, options.NotificationScheduler, latestState: true, GetGlobalSyncStateSize, ReportGlobalSyncDeliveryFault);
        _operationStates = new(options.MaxDiagnosticSubscriptions, options.NotificationScheduler, latestState: false, GetGlobalOperationStateSize, ReportGlobalSyncDeliveryFault);
        _faults = new(options.MaxDiagnosticSubscriptions, options.NotificationScheduler, latestState: false, GetGlobalFaultSize);
        _metrics = new(options.Options.Diagnostics.Enabled);
        _activities = new(options.Options.Diagnostics.Enabled, options.Options.Diagnostics.ActivitySamplingRatio);
        _scheduler = new(new(options.MaxRegisteredStreams, options.MaxSchedulerDescriptorBytes, options.Options.MinimumPriority, options.Options.MaximumPriority), options.TimeProvider);
        _snapshotRecoveryAdmissionQueue = new(_gate, options.Options.MaxConcurrentStreams);
        _lifecycle = new(StartCoreAsync, StopCoreAsync);
    }

    /// <summary>Engine lifecycle states that control local admission.</summary>
    private enum EngineAdmissionState
    {
        /// <summary>The engine has not been started or stopped and allows offline local commits.</summary>
        Created = 0,

        /// <summary>The engine is opening shared resources.</summary>
        Starting = 1,

        /// <summary>The engine is running and admits local commits.</summary>
        Running = 2,

        /// <summary>The engine is stopping and rejects new local commits.</summary>
        Stopping = 3,

        /// <summary>The engine is stopped and rejects new local commits until restarted.</summary>
        Stopped = 4,

        /// <summary>The engine is disposed.</summary>
        Disposed = 5,
    }

    /// <inheritdoc/>
    public IObservable<SyncState> SyncStates => _syncStates;

    /// <inheritdoc/>
    public IObservable<SyncOperationStatus> OperationStates => _operationStates;

    /// <inheritdoc/>
    public IObservable<OccasionallyConnectedFault> Faults => _faults;

    /// <inheritdoc/>
    public async ValueTask<PublishReceipt> EnqueueOperationAsync(
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        var retainedBytes = GetOperationRetainedBytes(operation);
        var admission = await EnterLocalCommitAsync(operation.StreamId, retainedBytes, cancellationToken).ConfigureAwait(false);
        try
        {
            await EnsureStoreInitializedAsync(cancellationToken).ConfigureAwait(false);

            while (true)
            {
                var generation = GetCapacityReleaseGeneration(operation.StreamId);
                var participant = GetParticipant(operation.StreamId);
                try
                {
                    var startedTimestamp = GetDiagnosticTimestamp();
                    using var activity = StartDiagnosticActivity(OccasionallyConnectedActivityName.StoreCommit);
                    var receipt = await participant.CommitSerializedAsync(operation, cancellationToken).ConfigureAwait(false);
                    RecordStoreCommitDuration(startedTimestamp);
                    if (participant is not IReportsSavedLocalCommitDiagnostics reporter || !reporter.ReportsSavedLocalCommitTo(this))
                    {
                        RecordOperationPublished();
                        RecordQueueDelta(operation.StreamId, 1, retainedBytes);
                        _operationStates.Publish(new(
                            receipt.OperationId,
                            operation.StreamId,
                            receipt.State,
                            Attempt: 0,
                            receipt.SavedAtUtc,
                            ReasonCode: null));
                        NotifyAdmittedLocalCommitReady(operation.StreamId, operation);
                    }

                    return receipt;
                }
                catch (QueueCapacityExceededException exception) when (exception.CanFitWhenEmpty)
                {
                    await WaitForCapacityReleaseAsync(operation.StreamId, generation, retainedBytes, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            CompleteLocalCommit(admission);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
        OperationId operationId,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await EnsureStoreInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await _options.Store.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        Task? stopIntentRegisteredTask;
        lock (_gate)
        {
            var stopTask = _stopTask;
            stopIntentRegisteredTask = stopTask is not null && !stopTask.IsCompleted ? _stopIntentRegisteredTask : null;
        }

        if (stopIntentRegisteredTask is not null)
        {
            await stopIntentRegisteredTask.ConfigureAwait(false);
        }

        await _lifecycle.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        Task drainTask;
        Task stopTask;
        (TaskCompletionSource<bool> Completion, TaskCompletionSource<bool> IntentRegistered)? acceptedStop = null;
        CapacityWaiter[] waiters;
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            _admissionState = EngineAdmissionState.Stopping;
            waiters = TakeAllCapacityWaitersLocked();
            drainTask = GetDrainTaskLocked();
            if (_stopTask is null || _stopTask.IsCompleted)
            {
                acceptedStop = (CreateCompletion(), CreateCompletion());
                _stopTask = acceptedStop.Value.Completion.Task;
                _stopIntentRegisteredTask = acceptedStop.Value.IntentRegistered.Task;
            }

            stopTask = _stopTask;
        }

        ReleaseWaiters(waiters, new InvalidOperationException("The synchronization engine stopped before outbox capacity became available."));
        if (acceptedStop is { } accepted)
        {
            _ = RunAcceptedStopAsync(drainTask, accepted.Completion, accepted.IntentRegistered);
        }

        await stopTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask TriggerSyncAsync(CancellationToken cancellationToken) => new(TriggerSyncCoreAsync(cancellationToken));

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource<bool>? completion = null;
        CapacityWaiter[] waiters = [];
        Task task;
        lock (_gate)
        {
            if (_disposeTask is null)
            {
                _disposed = true;
                _admissionState = EngineAdmissionState.Disposed;
                waiters = TakeAllCapacityWaitersLocked();
                completion = CreateCompletion();
                _disposeTask = completion.Task;
            }

            task = _disposeTask;
        }

        ReleaseWaiters(waiters, new ObjectDisposedException(nameof(SyncEngine)));
        if (completion is not null)
        {
            _ = RunDisposeAsync(completion);
        }

        return new(task);
    }

    /// <inheritdoc/>
    public IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant)
    {
        ArgumentExceptionHelper.ThrowIfNull(participant);

        lock (_gate)
        {
            ThrowIfDisposedLocked();
            if (_participants.Count >= _options.MaxRegisteredStreams)
            {
                throw new InvalidOperationException("The synchronization engine registered stream limit has been reached.");
            }

            if (_participants.ContainsKey(participant.StreamId))
            {
                throw new InvalidOperationException("A synchronization participant is already registered for the stream.");
            }

            var registration = new ParticipantRegistration(this, participant);
            _participants.Add(participant.StreamId, registration);
            _capacitySignals.Add(participant.StreamId, new(_capacityWaiterBudget));
            _scheduler.Register(new(participant.StreamId, Weight: 1));
            _ = StartReceivePumpForRegistrationLocked(registration);
            return registration;
        }
    }

    /// <inheritdoc/>
    public async ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken)
    {
        await EnsureStoreInitializedAsync(cancellationToken).ConfigureAwait(false);
        return await _options.Store.GetOrCreateSubscriptionIdAsync(streamId, preferredId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(StreamId streamId, long retainedBytes, CancellationToken cancellationToken)
    {
        ValidateRetainedBytes(retainedBytes);
        LocalCommitAdmission? admission = null;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            cancellationToken.ThrowIfCancellationRequested();
            var signal = GetCapacitySignalLocked(streamId);
            if (_admissionState is EngineAdmissionState.Created or EngineAdmissionState.Starting or EngineAdmissionState.Running)
            {
                _localCommitBudget.Reserve(retainedBytes);
                _activeLocalCommits++;
                admission = new(streamId, retainedBytes, signal.Id);
            }
        }

        return admission.HasValue
            ? new(admission.GetValueOrDefault())
            : throw new InvalidOperationException("The synchronization engine is not admitting new durable work.");
    }

    /// <inheritdoc/>
    public void CompleteLocalCommit(LocalCommitAdmission admission)
    {
        ValidateRetainedBytes(admission.RetainedBytes);
        TaskCompletionSource<bool>? drainWaiter = null;
        CapacityWaiter[] waiters;
        lock (_gate)
        {
            if (_activeLocalCommits <= 0)
            {
                throw new InvalidOperationException("No local commit admission is active.");
            }

            _activeLocalCommits--;
            _localCommitBudget.Release(admission.RetainedBytes);
            waiters = TakeAllCapacityWaitersLocked();

            if (_activeLocalCommits == 0)
            {
                drainWaiter = _drainWaiter;
                _drainWaiter = null;
            }
        }

        ReleaseWaiters(waiters);
        _ = drainWaiter?.TrySetResult(true);
    }

    /// <inheritdoc/>
    public long GetCapacityReleaseGeneration(StreamId streamId)
    {
        lock (_gate)
        {
            return GetCapacitySignalLocked(streamId).Generation;
        }
    }

    /// <inheritdoc/>
    public ValueTask WaitForCapacityReleaseAsync(
        StreamId streamId,
        long observedGeneration,
        long retainedBytes,
        CancellationToken cancellationToken)
    {
        ValidateRetainedBytes(retainedBytes);
        CapacityWaiter? waiter = null;
        Task? waitTask;

        lock (_gate)
        {
            ThrowIfDisposedLocked();
            cancellationToken.ThrowIfCancellationRequested();
            if (_admissionState is EngineAdmissionState.Stopping or EngineAdmissionState.Stopped)
            {
                throw new InvalidOperationException("The synchronization engine stopped before outbox capacity became available.");
            }

            var signal = GetCapacitySignalLocked(streamId);
            if (signal.Generation != observedGeneration)
            {
                return default;
            }

            signal.ReserveWaiter(retainedBytes);
            waiter = new(this, signal, retainedBytes, cancellationToken);
            waiter.Node = signal.Waiters.AddLast(waiter);
            waitTask = waiter.Task;
        }

        waiter.RegisterCancellation();
        return new(waitTask);
    }

    /// <inheritdoc/>
    public void NotifyCapacityReleased(StreamId streamId)
    {
        // Durable work can finish after its stream unregisters. Its capacity belongs to every live stream.
        _ = streamId;
        CapacityWaiter[] waiters;
        lock (_gate)
        {
            waiters = TakeAllCapacityWaitersLocked();
        }

        ReleaseWaiters(waiters);
    }

    /// <inheritdoc/>
    public ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken)
    {
        StartStream(streamId, cancellationToken);
        return default;
    }

    /// <inheritdoc/>
    public ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken)
    {
        var receiveTask = StopStream(streamId, cancellationToken);
        if (receiveTask is null)
        {
            return default;
        }

        if (receiveTask.IsCompleted)
        {
            return receiveTask.Status == TaskStatus.RanToCompletion ? default : new(receiveTask);
        }

        return new(receiveTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken));
    }

    /// <inheritdoc/>
    public void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            _ = GetParticipantLocked(streamId);
            ScheduleOrDeferStreamLocked(streamId, operation.Policy.Priority);
        }
    }

    /// <summary>Requests a bounded synchronization cycle and waits for the cycle's accepted work to drain.</summary>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The trigger task.</returns>
    /// <exception cref="InvalidOperationException">The engine is not running.</exception>
    private async Task TriggerSyncCoreAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Task cycleTask;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            if (_admissionState != EngineAdmissionState.Running)
            {
                throw new InvalidOperationException("The synchronization engine must be running before synchronization can be triggered.");
            }

            var nowUtc = _options.TimeProvider.GetUtcNow();
            foreach (var streamId in _participants.Keys)
            {
                if (!IsStreamRemoteActiveLocked(streamId))
                {
                    continue;
                }

                ScheduleStreamLocked(
                    streamId,
                    priority: 0,
                    nowUtc,
                    nowUtc,
                    maximumOperations: 0,
                    deadLetterOversizedHead: false,
                    forceReady: true);
            }

            cycleTask = GetSyncCycleTaskLocked();
        }

        await cycleTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs the accepted stop intent independently from any single caller's cancellation token.</summary>
    /// <param name="drainTask">The current local commit drain task.</param>
    /// <param name="completion">The shared accepted stop completion.</param>
    /// <param name="stopIntentRegistered">The signal completed after the lifecycle stop intent is registered.</param>
    /// <returns>The accepted stop driver task.</returns>
    private async Task RunAcceptedStopAsync(
        Task drainTask,
        TaskCompletionSource<bool> completion,
        TaskCompletionSource<bool> stopIntentRegistered)
    {
        try
        {
            var stopTask = _lifecycle.StopAsync(CancellationToken.None);
            var startupCancellation = CancelStartupAsync();
            _ = stopIntentRegistered.TrySetResult(true);
            Exception? failure = null;
            failure = await CaptureCleanupFailureAsync(failure, () => new(startupCancellation)).ConfigureAwait(false);
            failure = await CaptureCleanupFailureAsync(failure, () => new(drainTask)).ConfigureAwait(false);
            failure = await CaptureCleanupFailureAsync(failure, () => new(stopTask)).ConfigureAwait(false);
            lock (_gate)
            {
                if (_admissionState == EngineAdmissionState.Stopping)
                {
                    _admissionState = EngineAdmissionState.Stopped;
                }
            }

            ThrowCaptured(failure);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = stopIntentRegistered.TrySetException(exception);
            _ = stopIntentRegistered.Task.Exception;
            _ = completion.TrySetException(exception);
            _ = completion.Task.Exception;
        }
    }

    /// <summary>Gets or creates a synchronization cycle wait task while the engine lock is held.</summary>
    /// <returns>The cycle task.</returns>
    private Task GetSyncCycleTaskLocked()
    {
        if (_scheduledStreams.Count == 0 && _activeUploadAttempts == 0)
        {
            return Task.CompletedTask;
        }

        _syncCycleWaiter ??= CreateCompletion();
        return _syncCycleWaiter.Task;
    }

    /// <summary>Schedules a stream for upload or defers it until the next start.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="priority">The head priority.</param>
    /// <param name="notBeforeUtc">The optional UTC time before which the head is known not to be leaseable.</param>
    private void ScheduleOrDeferStreamLocked(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc = null)
    {
        var now = _options.TimeProvider.GetUtcNow();
        var normalDueUtc = now + _options.Options.Batching.MaximumDwellTime;
        var dueUtc = normalDueUtc;
        var hasFutureConstraint = false;
        if (notBeforeUtc is { } constrained && constrained > now)
        {
            hasFutureConstraint = true;
            if (constrained > normalDueUtc)
            {
                dueUtc = constrained;
            }
        }

        var requested = new UploadReschedule(
            priority,
            now,
            dueUtc,
            MaximumOperations: 0,
            DeadLetterOversizedHead: false,
            ForceReady: false) { IsRetryBackoff = hasFutureConstraint };
        if (_admissionState is EngineAdmissionState.Stopping or EngineAdmissionState.Stopped
            || !IsStreamRemoteActiveLocked(streamId))
        {
            DeferStreamScheduleLocked(streamId, requested);
            return;
        }

        ScheduleStreamLocked(streamId, requested);
    }

    /// <summary>Defers an upload wake until the engine is started again.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The deferred scheduler head request.</param>
    private void DeferStreamScheduleLocked(StreamId streamId, UploadReschedule requested)
    {
        if (_deferredUploadHeads.TryGetValue(streamId, out var existing))
        {
            requested = MergeUploadReschedule(ToReschedule(existing), requested);
        }

        _deferredUploadHeads[streamId] = new(
            requested.Priority,
            requested.ReadySinceUtc,
            requested.DueUtc,
            requested.MaximumOperations,
            requested.DeadLetterOversizedHead,
            requested.ForceReady,
            Inflight: false) { IsRetryBackoff = requested.IsRetryBackoff };
    }

    /// <summary>Schedules all stopped-time upload wakes after a successful start.</summary>
    private void ScheduleDeferredUploadHeadsLocked()
    {
        var deferred = new KeyValuePair<StreamId, UploadHead>[_deferredUploadHeads.Count];
        var index = 0;
        foreach (var item in _deferredUploadHeads)
        {
            deferred[index] = item;
            index++;
        }

        _deferredUploadHeads.Clear();
        for (var i = 0; i < deferred.Length; i++)
        {
            var head = deferred[i].Value;
            if (IsStreamRemoteActiveLocked(deferred[i].Key))
            {
                ScheduleStreamLocked(deferred[i].Key, ToReschedule(head));
            }
            else
            {
                _deferredUploadHeads[deferred[i].Key] = head;
            }
        }
    }

    /// <summary>Schedules a deferred upload head for a resumed stream while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    private void ScheduleDeferredUploadHeadLocked(StreamId streamId)
    {
        if (_admissionState != EngineAdmissionState.Running
            || !_deferredUploadHeads.TryGetValue(streamId, out var head))
        {
            return;
        }

        _ = _deferredUploadHeads.Remove(streamId);
        ScheduleStreamLocked(streamId, ToReschedule(head));
    }

    /// <summary>Parks pending upload scheduler state for one stopped stream while preserving local admission.</summary>
    /// <param name="streamId">The stream identity.</param>
    private void ParkStreamUploadLocked(StreamId streamId)
    {
        if (_uploadHeads.TryGetValue(streamId, out var head) && !head.Inflight && _scheduler.RemovePendingHead(streamId))
        {
            DeferStreamScheduleLocked(streamId, ToReschedule(head));
            _ = _scheduledStreams.Remove(streamId);
            _ = _uploadHeads.Remove(streamId);
        }

        if (_snapshotRecoveryUploadHeads.TryGetValue(streamId, out var snapshotHead))
        {
            _ = _snapshotRecoveryUploadHeads.Remove(streamId);
            DeferStreamScheduleLocked(streamId, ToReschedule(snapshotHead));
        }

        if (_rescheduleStreams.TryGetValue(streamId, out var pending))
        {
            _ = _rescheduleStreams.Remove(streamId);
            DeferStreamScheduleLocked(streamId, pending);
        }

        TryCompleteSyncCycleLocked();
        SignalUploadPumpLocked();
    }

    /// <summary>Schedules a stream while preserving existing upload-head metadata.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="priority">The head priority.</param>
    /// <param name="readySinceUtc">The UTC timestamp the head first became eligible to batch.</param>
    /// <param name="dueUtc">The due time for the scheduled head.</param>
    /// <param name="maximumOperations">The adaptive maximum operation count, or zero for the negotiated default.</param>
    /// <param name="deadLetterOversizedHead">Whether the next single-operation lease must be dead-lettered before upload.</param>
    /// <param name="forceReady">Whether the head should bypass dwell planning.</param>
    private void ScheduleStreamLocked(
        StreamId streamId,
        int priority,
        DateTimeOffset readySinceUtc,
        DateTimeOffset dueUtc,
        int maximumOperations,
        bool deadLetterOversizedHead,
        bool forceReady = false)
    {
        var requested = new UploadReschedule(priority, readySinceUtc, dueUtc, maximumOperations, deadLetterOversizedHead, forceReady);
        ScheduleStreamLocked(streamId, requested);
    }

    /// <summary>Schedules a stream while preserving existing upload-head metadata.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The scheduler head request.</param>
    private void ScheduleStreamLocked(StreamId streamId, UploadReschedule requested)
    {
        if (_snapshotRecoveryStreams.Contains(streamId))
        {
            ParkSnapshotRecoveryUploadLocked(streamId, requested);
            return;
        }

        if (_scheduledStreams.Contains(streamId))
        {
            ScheduleExistingStreamLocked(streamId, requested);
            return;
        }

        ScheduleNewStreamLocked(streamId, requested);
    }

    /// <summary>Schedules a new stream head while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The scheduler head request.</param>
    private void ScheduleNewStreamLocked(StreamId streamId, UploadReschedule requested)
    {
        _scheduler.Ready(streamId, requested.Priority, requested.DueUtc);
        _ = _scheduledStreams.Add(streamId);
        _uploadHeads[streamId] = new(
            requested.Priority,
            requested.ReadySinceUtc,
            requested.DueUtc,
            requested.MaximumOperations,
            requested.DeadLetterOversizedHead,
            requested.ForceReady,
            Inflight: false) { IsRetryBackoff = requested.IsRetryBackoff };
        _syncCycleWaiter ??= CreateCompletion();
        SignalUploadPumpLocked();
    }

    /// <summary>Merges a request into an already scheduled stream head.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The scheduler head request.</param>
    private void ScheduleExistingStreamLocked(StreamId streamId, UploadReschedule requested)
    {
        if (_rescheduleStreams.TryGetValue(streamId, out var pending))
        {
            requested = MergeUploadReschedule(pending, requested);
            _ = _rescheduleStreams.Remove(streamId);
        }

        if (_uploadHeads.TryGetValue(streamId, out var head) && !head.Inflight && _scheduler.RemovePendingHead(streamId))
        {
            var merged = MergeUploadReschedule(ToReschedule(head), requested);
            _scheduler.Ready(streamId, merged.Priority, merged.DueUtc);
            _uploadHeads[streamId] = new(
                merged.Priority,
                merged.ReadySinceUtc,
                merged.DueUtc,
                merged.MaximumOperations,
                merged.DeadLetterOversizedHead,
                merged.ForceReady,
                Inflight: false) { IsRetryBackoff = merged.IsRetryBackoff };
            _syncCycleWaiter ??= CreateCompletion();
            SignalUploadPumpLocked();
            return;
        }

        _rescheduleStreams[streamId] = requested;
    }

    /// <summary>Signals the upload pump while the engine lock is held.</summary>
    private void SignalUploadPumpLocked()
    {
        _uploadSignaled = true;
        _ = _uploadSignal.TrySetResult(true);
    }

    /// <summary>Takes the current upload signal task while the engine lock is held.</summary>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The signal task.</returns>
    private Task TakeUploadSignalTaskLocked(CancellationToken cancellationToken)
    {
        if (_uploadSignaled)
        {
            _uploadSignaled = false;
            _uploadSignal = CreateCompletion();
            return Task.CompletedTask;
        }

        var delay = GetNextUploadDelayLocked();
        return delay.HasValue
            ? WaitForUploadSignalOrDelayAsync(_uploadSignal.Task, delay.GetValueOrDefault(), cancellationToken)
            : _uploadSignal.Task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    /// <summary>Calculates the delay until the next non-inflight scheduled upload head becomes due.</summary>
    /// <returns>The next delay, or <see langword="null"/> when no delayed head is pending.</returns>
    private TimeSpan? GetNextUploadDelayLocked()
    {
        DateTimeOffset? nextDueUtc = null;
        foreach (var head in _uploadHeads.Values)
        {
            if (head.Inflight)
            {
                continue;
            }

            nextDueUtc = nextDueUtc is null || head.DueUtc < nextDueUtc.GetValueOrDefault() ? head.DueUtc : nextDueUtc;
        }

        if (nextDueUtc is null)
        {
            return null;
        }

        var delay = nextDueUtc.GetValueOrDefault() - _options.TimeProvider.GetUtcNow();
        return delay <= TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    /// <summary>Waits for either a pump signal or the next due upload deadline.</summary>
    /// <param name="signalTask">The current pump signal task.</param>
    /// <param name="delay">The delay until the next due head.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The wait task.</returns>
    private async Task WaitForUploadSignalOrDelayAsync(
        Task signalTask,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        var delayCompletion = CreateCompletion();
        await using var timer = _options.TimeProvider.CreateTimer(
            static state =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                _ = ((TaskCompletionSource<bool>)state).TrySetResult(true);
            },
            delayCompletion,
            delay,
            Timeout.InfiniteTimeSpan);
        await using var registration = UnsafeRegisterDelayCancellation(delayCompletion, cancellationToken);
        var completed = await Task.WhenAny(signalTask, delayCompletion.Task).ConfigureAwait(false);
        await completed.ConfigureAwait(false);
    }

    /// <summary>Completes the current trigger cycle if the pump is idle while the engine lock is held.</summary>
    private void TryCompleteSyncCycleLocked()
    {
        if (_scheduledStreams.Count != 0 || _activeUploadAttempts != 0)
        {
            return;
        }

        var waiter = _syncCycleWaiter;
        _syncCycleWaiter = null;
        _ = waiter?.TrySetResult(true);
    }

    /// <summary>Completes the shared disposal task after all cleanup stages have run.</summary>
    /// <param name="completion">The shared disposal completion.</param>
    /// <returns>The disposal driver task.</returns>
    private async Task RunDisposeAsync(TaskCompletionSource<bool> completion)
    {
        try
        {
            await DisposeCoreAsync().ConfigureAwait(false);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
            _ = completion.Task.Exception;
        }
    }

    /// <summary>Disposes owned lifecycle and dependencies.</summary>
    /// <returns>The disposal task.</returns>
    private async Task DisposeCoreAsync()
    {
        Exception? failure = null;
        failure = await CaptureCleanupFailureAsync(failure, () => new(CancelStartupAsync())).ConfigureAwait(false);
        failure = await CaptureLifecycleDisposeFailureAsync(failure).ConfigureAwait(false);
        _uploadCancellation?.Dispose();
        _uploadCancellation = null;
        failure = await CaptureCleanupFailureAsync(failure, () => new(WaitForDrainAsync())).ConfigureAwait(false);
        failure = DisposeRegisteredReceiveCancellations(failure);
        var receiveStopTasks = TakeReceiveStopTasks();
        if (receiveStopTasks.Length != 0)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(Task.WhenAll(receiveStopTasks))).ConfigureAwait(false);
        }

        try
        {
            _activities.Dispose();
            _metrics.Dispose();
            _syncStates.Dispose();
            _operationStates.Dispose();
            _faults.Dispose();
        }
        catch (Exception exception)
        {
            failure ??= exception;
        }

        var session = TakeSession();
        if (session is not null)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => session.DisposeAsync()).ConfigureAwait(false);
        }

        var retiredSessionLeases = TakeRetiredSessionLeases();
        for (var i = 0; i < retiredSessionLeases.Length; i++)
        {
            var retiredSession = retiredSessionLeases[i].Session;
            failure = await CaptureCleanupFailureAsync(failure, () => retiredSession.DisposeAsync()).ConfigureAwait(false);
        }

        if (_options.TransportOwnership == SyncEngineDependencyOwnership.Owned)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => _options.Transport.DisposeAsync()).ConfigureAwait(false);
        }

        if (_options.StoreOwnership == SyncEngineDependencyOwnership.Owned)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => _options.Store.DisposeAsync()).ConfigureAwait(false);
        }

        ThrowCaptured(failure);
    }

    /// <summary>Disposes receive cancellation sources for still-registered participants during engine disposal.</summary>
    /// <param name="failure">The current first cleanup failure.</param>
    /// <returns>The preserved first cleanup failure.</returns>
    private Exception? DisposeRegisteredReceiveCancellations(Exception? failure)
    {
        ParticipantRegistration[] registrations;
        lock (_gate)
        {
            registrations = [.. _participants.Values];
        }

        for (var i = 0; i < registrations.Length; i++)
        {
            var registration = registrations[i];
            var cancellationFailure = registration.CancelReceive();
            if (cancellationFailure is not null)
            {
                PublishReceivePumpFault(registration.Participant, cancellationFailure);
                failure ??= cancellationFailure;
            }

            try
            {
                registration.DisposeReceiveCancellation();
            }
            catch (Exception exception)
            {
                failure ??= exception;
            }
        }

        return failure;
    }

    /// <summary>Takes stop-time resources and clears pending work while the engine lock is held.</summary>
    /// <returns>The resources to clean up outside the engine lock.</returns>
    private StopResources TakeStopResources()
    {
        lock (_gate)
        {
            if (_admissionState != EngineAdmissionState.Disposed)
            {
                _admissionState = EngineAdmissionState.Stopping;
            }

            _sharedSessionLease?.MarkStopOwned();
            for (var i = 0; i < _retiredSessionLeases.Count; i++)
            {
                _retiredSessionLeases[i].MarkStopOwned();
            }

            var resources = new StopResources(
                _session,
                _uploadCancellation,
                _uploadPumpTask,
                _sessionRenewalTask,
                [.. _receivePumpTasks],
                [.. _receiveStopTasks],
                [.. _retiredSessionLeases]);
            _session = null;
            _sharedSessionLease = null;
            _uploadCancellation = null;
            _uploadPumpTask = null;
            _sessionRenewalTask = null;
            _sessionRenewalGeneration = 0;
            _remoteSessionRenewalUsedSinceProgress = false;
            _retiredSessionLeases.Clear();
            _receivePumpTasks.Clear();
            _receiveStopTasks.Clear();
            _receivePumpStreams.Clear();
            ClearPendingUploadSchedulesLocked();
            SignalUploadPumpLocked();
            return resources;
        }
    }

    /// <summary>Clears scheduler state for pending upload heads while preserving inflight ownership.</summary>
    private void ClearPendingUploadSchedulesLocked()
    {
        var scheduledStreams = new StreamId[_scheduledStreams.Count];
        _scheduledStreams.CopyTo(scheduledStreams);
        foreach (var streamId in scheduledStreams)
        {
            if (_uploadHeads.TryGetValue(streamId, out var head) && head.Inflight)
            {
                continue;
            }

            if (_scheduler.RemovePendingHead(streamId) && _admissionState != EngineAdmissionState.Disposed)
            {
                DeferStreamScheduleLocked(streamId, ToReschedule(head));
            }

            _ = _scheduledStreams.Remove(streamId);
            _ = _uploadHeads.Remove(streamId);
            _ = _rescheduleStreams.Remove(streamId);
        }
    }

    /// <summary>Waits for active local admissions to drain.</summary>
    /// <returns>The drain wait task.</returns>
    private async Task WaitForDrainAsync()
    {
        Task drainTask;
        lock (_gate)
        {
            drainTask = GetDrainTaskLocked();
        }

        await drainTask.ConfigureAwait(false);
    }

    /// <summary>Gets the active local admission drain task while the engine lock is held.</summary>
    /// <returns>The drain task.</returns>
    private Task GetDrainTaskLocked()
    {
        if (_activeLocalCommits == 0)
        {
            return Task.CompletedTask;
        }

        _drainWaiter ??= CreateCompletion();
        return _drainWaiter.Task;
    }

    /// <summary>Takes the active session for disposal.</summary>
    /// <returns>The active session, if any.</returns>
    private IRemoteTransportSession? TakeSession()
    {
        lock (_gate)
        {
            var session = _session;
            _sharedSessionLease?.MarkStopOwned();
            _session = null;
            _sharedSessionLease = null;
            return session;
        }
    }

    /// <summary>Takes retired shared sessions for disposal.</summary>
    /// <returns>The retired shared-session lease states.</returns>
    private SharedSessionLeaseState[] TakeRetiredSessionLeases()
    {
        lock (_gate)
        {
            for (var i = 0; i < _retiredSessionLeases.Count; i++)
            {
                _retiredSessionLeases[i].MarkStopOwned();
            }

            var retiredSessionLeases = _retiredSessionLeases.ToArray();
            _retiredSessionLeases.Clear();
            return retiredSessionLeases;
        }
    }

    /// <summary>Takes receive stop tasks while the engine lock is held.</summary>
    /// <returns>The active receive stop tasks.</returns>
    private Task[] TakeReceiveStopTasks()
    {
        lock (_gate)
        {
            var tasks = _receiveStopTasks.ToArray();
            _receiveStopTasks.Clear();
            return tasks;
        }
    }

    /// <summary>Stops shared transport work and closes new durable admission.</summary>
    /// <returns>The stop task.</returns>
    private async ValueTask StopCoreAsync()
    {
        await WaitForDrainAsync().ConfigureAwait(false);
        var resources = TakeStopResources();
        Exception? failure = null;
        if (resources.UploadCancellation is not null)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(resources.UploadCancellation.CancelAsync())).ConfigureAwait(false);
            resources.UploadCancellation.Dispose();
        }

        if (resources.UploadPumpTask is not null)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(resources.UploadPumpTask)).ConfigureAwait(false);
        }

        if (resources.SessionRenewalTask is not null)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(resources.SessionRenewalTask)).ConfigureAwait(false);
        }

        if (resources.ReceivePumpTasks.Length != 0)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(Task.WhenAll(resources.ReceivePumpTasks))).ConfigureAwait(false);
        }

        if (resources.ReceiveStopTasks.Length != 0)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => new(Task.WhenAll(resources.ReceiveStopTasks))).ConfigureAwait(false);
        }

        if (resources.Session is not null)
        {
            failure = await CaptureCleanupFailureAsync(failure, () => resources.Session.DisposeAsync()).ConfigureAwait(false);
        }

        for (var i = 0; i < resources.RetiredSessionLeases.Length; i++)
        {
            var retiredSession = resources.RetiredSessionLeases[i].Session;
            failure = await CaptureCleanupFailureAsync(failure, () => retiredSession.DisposeAsync()).ConfigureAwait(false);
        }

        lock (_gate)
        {
            if (_admissionState != EngineAdmissionState.Disposed)
            {
                _admissionState = EngineAdmissionState.Stopped;
            }

            TryCompleteSyncCycleLocked();
        }

        failure = await CaptureCleanupFailureAsync(
                failure,
                () =>
                {
                    PublishLifecycleState(SyncLifecycleStatus.Stopped, networkAvailable: false);
                    return default;
                })
            .ConfigureAwait(false);
        ThrowCaptured(failure);
    }

    /// <summary>Publishes a lifecycle state transition.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="networkAvailable">Whether network transport is available.</param>
    private void PublishLifecycleState(SyncLifecycleStatus status, bool networkAvailable)
    {
        SyncState aggregate;
        (IOccasionallyConnectedStreamDiagnosticsSink Sink, SyncState State)[] streams;
        long revision;
        lock (_gate)
        {
            _diagnosticLifecycleStatus = status;
            _diagnosticNetworkAvailable = networkAvailable;
            revision = ++_diagnosticRevision;
            aggregate = CreateAggregateSyncStateLocked();
            streams = CaptureStreamSyncStatesLocked();
        }

        PublishGlobalSyncState(aggregate, revision);
        for (var i = 0; i < streams.Length; i++)
        {
            PublishStreamDiagnostic(streams[i].Sink, streams[i].State, revision);
        }

        RecordConnectionStateChange();
    }

    /// <summary>Ensures the local store has been initialized once.</summary>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The shared initialization task.</returns>
    private Task EnsureStoreInitializedAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool>? completion = null;
        Task task;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            if (_initializeStoreTask is null)
            {
                completion = CreateCompletion();
                _initializeStoreTask = completion.Task;
            }

            task = _initializeStoreTask;
        }

        if (completion is not null)
        {
            _ = CompleteStoreInitializationAsync(completion);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return task.IsCompleted || !cancellationToken.CanBeCanceled
            ? task
            : task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    /// <summary>Runs the shared local store initialization outside the engine gate.</summary>
    /// <param name="completion">The initialization completion source.</param>
    /// <returns>The initialization driver task.</returns>
    private async Task CompleteStoreInitializationAsync(TaskCompletionSource<bool> completion)
    {
        try
        {
            await _options.Store.InitializeAsync(_options.StoreInitialization, CancellationToken.None).ConfigureAwait(false);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
            _ = completion.Task.Exception;
        }
    }

    /// <summary>Gets a participant outside the engine lock.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The participant.</returns>
    private IOccasionallyConnectedStreamParticipant GetParticipant(StreamId streamId)
    {
        lock (_gate)
        {
            return GetParticipantLocked(streamId).Participant;
        }
    }

    /// <summary>Gets a participant registration while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The participant registration.</returns>
    /// <exception cref="InvalidOperationException">The stream is not registered.</exception>
    private ParticipantRegistration GetParticipantLocked(StreamId streamId)
    {
        if (_participants.TryGetValue(streamId, out var participant))
        {
            return participant;
        }

        throw new InvalidOperationException("The stream is not registered with the synchronization engine.");
    }

    /// <summary>Gets a capacity signal while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The capacity signal.</returns>
    /// <exception cref="InvalidOperationException">The stream is not registered.</exception>
    private CapacitySignal GetCapacitySignalLocked(StreamId streamId) =>
        _capacitySignals.TryGetValue(streamId, out var signal)
            ? signal
            : throw new InvalidOperationException("The stream is not registered with the synchronization engine.");

    /// <summary>Takes all registered capacity waiters while the engine lock is held.</summary>
    /// <returns>The waiters to release.</returns>
    private CapacityWaiter[] TakeAllCapacityWaitersLocked()
    {
        var waiters = new List<CapacityWaiter>();
        foreach (var signal in _capacitySignals.Values)
        {
            signal.Generation++;
            waiters.AddRange(signal.TakeWaiters());
        }

        return [.. waiters];
    }

    /// <summary>Stores resources captured when stopping the engine.</summary>
    /// <param name="Session">The active transport session.</param>
    /// <param name="UploadCancellation">The upload pump cancellation source.</param>
    /// <param name="UploadPumpTask">The active upload pump task.</param>
    /// <param name="SessionRenewalTask">The active session renewal completion task.</param>
    /// <param name="ReceivePumpTasks">The active receive pump tasks.</param>
    /// <param name="ReceiveStopTasks">The active receive stop tasks.</param>
    /// <param name="RetiredSessionLeases">The retired shared sessions awaiting stop-time disposal.</param>
    private readonly record struct StopResources(
        IRemoteTransportSession? Session,
        CancellationTokenSource? UploadCancellation,
        Task? UploadPumpTask,
        Task<SessionRenewalResult>? SessionRenewalTask,
        Task[] ReceivePumpTasks,
        Task[] ReceiveStopTasks,
        SharedSessionLeaseState[] RetiredSessionLeases);
}
