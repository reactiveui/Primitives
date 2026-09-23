// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Test doubles for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The test client identifier.</summary>
    private const string EngineClientId = "client";

    /// <summary>The store initialization name used by SyncEngine fixtures.</summary>
    private const string StoreInitializationName = "sync-engine-tests";

    /// <summary>The default server idempotency retention in minutes.</summary>
    private const int DefaultServerIdempotencyRetentionMinutes = 5;

    /// <summary>The store capabilities modeled by positive recording upload fixtures.</summary>
    private const LocalStoreCapabilities RecordingStoreUploadCapabilities =
        LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply
        | LocalStoreCapabilities.DurableInbox
        | LocalStoreCapabilities.LeasedOutbox
        | LocalStoreCapabilities.DurableLocalCommit
        | LocalStoreCapabilities.ClientIdentityBinding;

    /// <summary>The transport capabilities modeled by positive recording upload fixtures.</summary>
    private const RemoteTransportCapabilities RecordingTransportUploadCapabilities =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>The future lease duration used by generic SyncEngine upload fixtures.</summary>
    private static readonly TimeSpan RecordingLeaseDuration = TimeSpan.FromMinutes(5);

    /// <summary>Creates a test engine.</summary>
    /// <param name="store">The optional store.</param>
    /// <param name="transport">The optional transport.</param>
    /// <param name="options">The optional runtime options.</param>
    /// <param name="maxRegisteredStreams">The stream registration cap.</param>
    /// <param name="maxDiagnosticSubscriptions">The diagnostic subscription cap.</param>
    /// <param name="timeProvider">The optional test time provider.</param>
    /// <param name="notificationScheduler">The optional diagnostic callback scheduler.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateEngine(
        ILocalStoreAdapter? store = null,
        RecordingTransport? transport = null,
        OccasionallyConnectedOptions? options = null,
        int maxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
        int maxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        TimeProvider? timeProvider = null,
        IObserverNotificationScheduler? notificationScheduler = null) =>
        new(new()
        {
            Store = store ?? new RecordingStore(),
            Transport = transport ?? new(),
            StoreOwnership = SyncEngineDependencyOwnership.Owned,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = options ?? OccasionallyConnectedOptions.Default,
            StoreInitialization = new(StoreInitializationName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = timeProvider ?? TimeProvider.System,
            NotificationScheduler = notificationScheduler ?? ThreadPoolObserverNotificationScheduler.Instance,
            MaxRegisteredStreams = maxRegisteredStreams,
            MaxDiagnosticSubscriptions = maxDiagnosticSubscriptions,
        });

    /// <summary>Creates a test engine with a composed transport adapter.</summary>
    /// <param name="transport">The composed transport adapter.</param>
    /// <param name="options">The optional runtime options.</param>
    /// <param name="timeProvider">The optional test time provider.</param>
    /// <param name="maxRegisteredStreams">The stream registration cap.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateEngineWithTransportAdapter(
        IRemoteTransportAdapter transport,
        OccasionallyConnectedOptions? options = null,
        TimeProvider? timeProvider = null,
        int maxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams) =>
        new(new()
        {
            Store = new RecordingStore(),
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Owned,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = options ?? OccasionallyConnectedOptions.Default,
            StoreInitialization = new(StoreInitializationName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = timeProvider ?? TimeProvider.System,
            MaxRegisteredStreams = maxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });

    /// <summary>Creates a test engine with explicit dependency ownership.</summary>
    /// <param name="store">The optional store.</param>
    /// <param name="transport">The optional transport.</param>
    /// <param name="storeOwnership">Whether the engine owns the supplied store.</param>
    /// <param name="transportOwnership">Whether the engine owns the supplied transport.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateEngineWithOwnership(
        ILocalStoreAdapter? store,
        RecordingTransport? transport,
        SyncEngineDependencyOwnership storeOwnership,
        SyncEngineDependencyOwnership transportOwnership) =>
        new(new()
        {
            Store = store ?? new RecordingStore(),
            Transport = transport ?? new(),
            StoreOwnership = storeOwnership,
            TransportOwnership = transportOwnership,
            Options = OccasionallyConnectedOptions.Default,
            StoreInitialization = new(StoreInitializationName, RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = TimeProvider.System,
            MaxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });

    /// <summary>Creates a raw serialized operation.</summary>
    /// <param name="streamId">The optional stream identity.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="operationId">The optional operation identity.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        StreamId? streamId = null,
        long sequence = FirstSequence,
        byte[]? payload = null,
        OperationId? operationId = null) =>
        new()
        {
            OperationId = operationId ?? Operation,
            StreamId = streamId ?? Stream,
            ClientSequence = sequence,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = new("counter-input", 1, "application/json", payload ?? TestPayload, "hash"),
        };

    /// <summary>Creates options with specific batch count.</summary>
    /// <param name="maximumOperations">The maximum operation count.</param>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateBatchOptions(int maximumOperations) =>
        OccasionallyConnectedOptions.Default with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with
            {
                MaximumOperations = maximumOperations,
                MaximumBytes = PreparedUploadBytes,
            },
        };

    /// <summary>Creates a prepared session with explicit batch-push capability.</summary>
    /// <param name="maximumOperations">The maximum operation count.</param>
    /// <returns>The prepared session.</returns>
    private static PreparedSession CreateBatchSession(int maximumOperations) =>
        new(maximumOperations, PreparedUploadBytes) { NegotiatedCapabilities = CreateBatchPushCapabilities(maximumOperations, PreparedUploadBytes) };

    /// <summary>Creates a recording upload store with one queued lease.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="capabilities">The optional modeled store capabilities.</param>
    /// <param name="timeProvider">The optional clock used to issue the fixture lease expiry.</param>
    /// <returns>The recording store.</returns>
    private static RecordingStore CreateUploadStore(
        IReadOnlyList<SyncOperation> operations,
        LocalStoreCapabilities? capabilities = null,
        TimeProvider? timeProvider = null)
    {
        var store = new RecordingStore { Capabilities = capabilities ?? RecordingStoreUploadCapabilities };
        store.Leases.Enqueue(CreateLease(operations, timeProvider));
        foreach (var operation in operations)
        {
            store.Statuses[operation.OperationId] = new(
                operation.OperationId,
                operation.StreamId,
                SyncOperationState.QueuedForUpload,
                Attempt: 0,
                DateTimeOffset.UnixEpoch,
                ReasonCode: null);
        }

        return store;
    }

    /// <summary>Creates a recording upload store with one lease for each operation.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="timeProvider">The optional clock used to issue fixture lease expiry.</param>
    /// <returns>The recording store.</returns>
    private static RecordingStore CreateUploadStoreWithSingleOperationLeases(
        IReadOnlyList<SyncOperation> operations,
        TimeProvider? timeProvider = null)
    {
        var store = new RecordingStore { Capabilities = RecordingStoreUploadCapabilities };
        foreach (var operation in operations)
        {
            store.Leases.Enqueue(CreateLease([operation], timeProvider));
            store.Statuses[operation.OperationId] = new(
                operation.OperationId,
                operation.StreamId,
                SyncOperationState.QueuedForUpload,
                Attempt: 0,
                DateTimeOffset.UnixEpoch,
                ReasonCode: null);
        }

        return store;
    }

    /// <summary>Creates a participant that records successful upload reconciliation in the store.</summary>
    /// <param name="store">The store receiving synchronized statuses.</param>
    /// <param name="failStatusQueriesAfterReconcile">Whether later status queries fail after durable reconciliation.</param>
    /// <param name="streamId">The optional stream identifier.</param>
    /// <param name="commitEntered">The optional signal raised when participant commit starts.</param>
    /// <param name="releaseCommit">The optional signal that releases a blocked participant commit.</param>
    /// <returns>The recording participant.</returns>
    private static RecordingParticipant CreateUploadParticipant(
        RecordingStore store,
        bool failStatusQueriesAfterReconcile = false,
        StreamId? streamId = null,
        TaskCompletionSource? commitEntered = null,
        TaskCompletionSource? releaseCommit = null) =>
        new()
        {
            StreamId = streamId ?? Stream,
            CommitEntered = commitEntered,
            ReleaseCommit = releaseCommit,
            OnApplySyncResult = (batch, result) =>
            {
                foreach (var operationResult in result.Operations)
                {
                    var operation = batch.Operations.First(candidate => candidate.OperationId == operationResult.OperationId);
                    store.Statuses[operation.OperationId] = new(
                        operation.OperationId,
                        operation.StreamId,
                        SyncOperationState.Synchronized,
                        Attempt: 1,
                        DateTimeOffset.UnixEpoch,
                        ReasonCode: null);
                }

                if (!failStatusQueriesAfterReconcile)
                {
                    return;
                }

                store.StatusQueryException = new InvalidOperationException("status failed");
            },
        };

    /// <summary>Creates one test outbox lease.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="timeProvider">The clock used to issue the lease expiry.</param>
    /// <returns>The lease.</returns>
    private static LeasedOperationBatch CreateLease(IReadOnlyList<SyncOperation> operations, TimeProvider? timeProvider = null) =>
        new(Guid.NewGuid(), (timeProvider ?? TimeProvider.System).GetUtcNow() + RecordingLeaseDuration, operations);

    /// <summary>Waits until a condition becomes true.</summary>
    /// <param name="condition">The condition to poll.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The condition does not become true before the guard timeout.</exception>
    private static async Task WaitForConditionAsync(Func<bool> condition)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (TimeProvider.System.GetUtcNow() < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(PollMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("The expected condition was not met.");
    }

    /// <summary>Creates a typed transport failure for retry classification tests.</summary>
    /// <param name="kind">The failure kind.</param>
    /// <param name="retryAfter">The optional retry-after lower bound.</param>
    /// <param name="credentialsVersion">The optional credential version.</param>
    /// <returns>The typed transport failure exception.</returns>
    private static TypedTransportFailureException CreateTransportFailure(
        RetryFailureKind kind,
        TimeSpan? retryAfter = null,
        string? credentialsVersion = null) =>
        new(new RetryFailure(kind, retryAfter, credentialsVersion));

    /// <summary>Typed Core transport failure used by Runtime tests without taking an HTTP package reference.</summary>
    private sealed class TypedTransportFailureException : Exception, IRemoteTransportFailure
    {
        /// <summary>The default typed transport failure message.</summary>
        private const string FailureMessage = "typed transport failure";

        /// <summary>Initializes a new instance of the <see cref="TypedTransportFailureException"/> class.</summary>
        public TypedTransportFailureException()
            : this(new(RetryFailureKind.ValidationRejected), FailureMessage, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TypedTransportFailureException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        public TypedTransportFailureException(string? message)
            : this(new(RetryFailureKind.ValidationRejected), message, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TypedTransportFailureException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        /// <param name="innerException">The inner exception.</param>
        public TypedTransportFailureException(string? message, Exception? innerException)
            : this(new(RetryFailureKind.ValidationRejected), message, innerException)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TypedTransportFailureException"/> class.</summary>
        /// <param name="retryFailure">The retry failure classification supplied by the transport.</param>
        public TypedTransportFailureException(RetryFailure retryFailure)
            : this(retryFailure, FailureMessage, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TypedTransportFailureException"/> class.</summary>
        /// <param name="retryFailure">The retry failure classification supplied by the transport.</param>
        /// <param name="message">The failure message.</param>
        /// <param name="innerException">The inner exception.</param>
        private TypedTransportFailureException(RetryFailure retryFailure, string? message, Exception? innerException)
            : base(message, innerException)
        {
            RetryFailure = retryFailure;
        }

        /// <inheritdoc/>
        public RetryFailure RetryFailure { get; }
    }

    /// <summary>Records participant commits.</summary>
    private sealed class RecordingParticipant : IOccasionallyConnectedStreamParticipant
    {
        /// <inheritdoc/>
        public StreamId StreamId { get; init; } = Stream;

        /// <summary>Gets the committed operation.</summary>
        public SyncOperation? CommittedOperation { get; private set; }

        /// <summary>Gets or sets the remaining capacity failures.</summary>
        public int CapacityFailures { get; init; }

        /// <summary>Gets the number of commit attempts.</summary>
        public int CommitAttempts { get; private set; }

        /// <summary>Gets or sets the optional signal set when a commit begins.</summary>
        public TaskCompletionSource? CommitEntered { get; init; }

        /// <summary>Gets or sets the optional signal that releases an active commit.</summary>
        public TaskCompletionSource? ReleaseCommit { get; init; }

        /// <summary>Gets or sets the optional callback invoked during a commit.</summary>
        public Action? OnCommit { get; init; }

        /// <summary>Gets the signal set after a capacity failure.</summary>
        public TaskCompletionSource CapacityFailureObserved { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the number of dead-letter calls.</summary>
        public int DeadLetterCalls { get; private set; }

        /// <summary>Gets the last dead-lettered operation identity.</summary>
        public OperationId? LastDeadLetterOperationId { get; private set; }

        /// <summary>Gets or sets the optional reconciliation callback.</summary>
        public Action<SyncBatch, RemoteSyncResult>? OnApplySyncResult { get; init; }

        /// <summary>Gets or sets the optional asynchronous reconciliation callback.</summary>
        public Func<SyncBatch, RemoteSyncResult, CancellationToken, ValueTask<ParticipantQueueTransitionResult>>? OnApplySyncResultAsync { get; init; }

        /// <summary>Gets or sets the remote receive subscription metadata.</summary>
        public ReceiveStreamSubscription? ReceiveSubscription { get; init; }

        /// <summary>Gets the remote receive batches applied through the participant.</summary>
        public List<RemoteEventBatch> AppliedRemoteBatches { get; } = [];

        /// <summary>Gets or sets the optional signal set when remote apply begins.</summary>
        public TaskCompletionSource? RemoteApplyEntered { get; init; }

        /// <summary>Gets or sets the optional signal that releases remote apply.</summary>
        public TaskCompletionSource? ReleaseRemoteApply { get; init; }

        /// <summary>Gets the number of remote apply calls.</summary>
        public int RemoteApplyCalls { get; private set; }

        /// <summary>Gets or sets the optional remote apply failure.</summary>
        public Exception? RemoteApplyException { get; init; }

        /// <summary>Gets or sets the optional durable cursor advancement classifier.</summary>
        public Func<RemoteEventBatch, bool>? RemoteApplyCursorAdvanced { get; init; }

        /// <inheritdoc/>
        public ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(ReceiveSubscription);
        }

        /// <inheritdoc/>
        public async ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CommitAttempts++;
            if (CommitAttempts <= CapacityFailures)
            {
                _ = CapacityFailureObserved.TrySetResult();
                throw new QueueCapacityExceededException("The participant queue is full.", true);
            }

            _ = CommitEntered?.TrySetResult();
            if (ReleaseCommit is not null)
            {
                await ReleaseCommit.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            CommittedOperation = operation;
            OnCommit?.Invoke();
            return new(
                operation.OperationId,
                operation.ClientSequence,
                SyncOperationState.SavedLocally,
                DateTimeOffset.UnixEpoch);
        }

        /// <inheritdoc/>
        public async ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(
            SyncBatch batch,
            RemoteSyncResult result,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (OnApplySyncResultAsync is not null)
            {
                return await OnApplySyncResultAsync(batch, result, cancellationToken).ConfigureAwait(false);
            }

            OnApplySyncResult?.Invoke(batch, result);
            return ParticipantQueueTransitionResult.None;
        }

        /// <inheritdoc/>
        public async ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AppliedRemoteBatches.Add(batch);
            RemoteApplyCalls++;
            _ = RemoteApplyEntered?.TrySetResult();
            if (ReleaseRemoteApply is not null)
            {
                await ReleaseRemoteApply.Task.ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (RemoteApplyException is not null)
            {
                throw RemoteApplyException;
            }

            var cursorAdvanced = RemoteApplyCursorAdvanced?.Invoke(batch) ?? (batch.Events.Count > 0);
            return new(new(batch.NextCursor, batch.Events.Count, 0), null, cursorAdvanced);
        }

        /// <inheritdoc/>
        public ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            CancellationToken cancellationToken)
        {
            _ = leaseId;
            _ = reasonCode;
            cancellationToken.ThrowIfCancellationRequested();
            DeadLetterCalls++;
            LastDeadLetterOperationId = operationId;
            return new(ParticipantQueueTransitionResult.None);
        }
    }
}
