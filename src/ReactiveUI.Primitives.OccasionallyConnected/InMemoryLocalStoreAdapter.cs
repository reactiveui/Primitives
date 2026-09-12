// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores occasionally connected stream state in this process.</summary>
/// <remarks>The adapter is ephemeral and retains data only for the lifetime of this instance.</remarks>
[DebuggerDisplay("Streams = {_streams.Count}, Operations = {_operations.Count}")]
internal sealed partial class InMemoryLocalStoreAdapter : ILocalStoreAdapter
{
    /// <summary>The default maximum retained operation and snapshot records.</summary>
    private const int DefaultMaximumRecordCount = 10_000;

    /// <summary>The default maximum retained operation and snapshot bytes.</summary>
    private const long DefaultMaximumEncodedBytes = 64L * 1024L * 1024L;

    /// <summary>The reason code returned when at-most-once has already recorded an attempt.</summary>
    private const string AtMostOnceAttemptRecordedReason = "OC.AtMostOnceAttemptAlreadyRecorded";

    /// <summary>The instance gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The stream identities in this instance.</summary>
    private readonly Dictionary<StreamId, StreamRecord> _streams = [];

    /// <summary>The operation records in this instance.</summary>
    private readonly Dictionary<OperationId, OperationRecord> _operations = [];

    /// <summary>The lease records in this instance.</summary>
    private readonly Dictionary<Guid, LeaseRecord> _leases = [];

    /// <summary>The inbox deduplication entries in this instance.</summary>
    private readonly HashSet<InboxKey> _inbox = [];

    /// <summary>The time provider used for local timestamps.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The validated retention policy.</summary>
    private readonly RetentionOptions _retentionOptions;

    /// <summary>The maximum retained operation and snapshot record count.</summary>
    private readonly int _maximumRecordCount;

    /// <summary>The maximum retained operation and snapshot payload bytes.</summary>
    private readonly long _maximumEncodedBytes;

    /// <summary>The initialized store identity.</summary>
    private string? _storeIdentity;

    /// <summary>The retained operation and snapshot payload bytes.</summary>
    private long _encodedBytes;

    /// <summary>The retained operation and snapshot record count.</summary>
    private int _recordCount;

    /// <summary>A value indicating whether the instance is disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="InMemoryLocalStoreAdapter"/> class.</summary>
    public InMemoryLocalStoreAdapter()
        : this(TimeProvider.System, DefaultMaximumRecordCount, DefaultMaximumEncodedBytes, new RetentionOptions())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="InMemoryLocalStoreAdapter"/> class.</summary>
    /// <param name="maximumRecordCount">The maximum retained operation and snapshot records.</param>
    /// <param name="maximumEncodedBytes">The maximum retained operation and snapshot payload bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException">A capacity is not positive.</exception>
    public InMemoryLocalStoreAdapter(int maximumRecordCount, long maximumEncodedBytes)
        : this(TimeProvider.System, maximumRecordCount, maximumEncodedBytes, new RetentionOptions())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="InMemoryLocalStoreAdapter"/> class.</summary>
    /// <param name="timeProvider">The time provider used for local timestamps.</param>
    /// <param name="maximumRecordCount">The maximum retained operation and snapshot records.</param>
    /// <param name="maximumEncodedBytes">The maximum retained operation and snapshot payload bytes.</param>
    /// <param name="retentionOptions">The validated retention options reserved for separate retention categories.</param>
    /// <exception cref="ArgumentNullException">A dependency is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A capacity or retention interval is not positive.</exception>
    internal InMemoryLocalStoreAdapter(
        TimeProvider timeProvider,
        int maximumRecordCount,
        long maximumEncodedBytes,
        RetentionOptions retentionOptions)
    {
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        ArgumentExceptionHelper.ThrowIfNull(retentionOptions);
        if (maximumRecordCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRecordCount), maximumRecordCount, "Maximum record count must be positive.");
        }

        if (maximumEncodedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumEncodedBytes), maximumEncodedBytes, "Maximum encoded bytes must be positive.");
        }

        retentionOptions.Validate();
        _timeProvider = timeProvider;
        _retentionOptions = retentionOptions;
        _maximumRecordCount = maximumRecordCount;
        _maximumEncodedBytes = maximumEncodedBytes;
    }

    /// <inheritdoc/>
    public LocalStoreCapabilities Capabilities { get; } =
        LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply
        | LocalStoreCapabilities.LeasedOutbox;

    /// <inheritdoc/>
    public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        InMemoryLocalStoreAdapterValidation.ValidateStoreIdentity(initialization.StoreIdentity, nameof(initialization));
        if (initialization.RequiredSchemaVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialization), initialization.RequiredSchemaVersion, "Required schema version must be positive.");
        }

        if (initialization.RequiredSchemaVersion > 1)
        {
            throw new NotSupportedException("The in-memory local store supports schema version one.");
        }

        if (initialization.RequireAuthenticatedEncryptionAtRest)
        {
            throw new NotSupportedException("The in-memory local store does not support authenticated encryption at rest.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            if (_storeIdentity is not null && !string.Equals(_storeIdentity, initialization.StoreIdentity, StringComparison.Ordinal))
            {
                throw new InvalidOperationException("The in-memory local store has already been initialized for another store identity.");
            }

            if (_storeIdentity is null)
            {
                EnsureCapacityFor(StoreIdentityCapacity(initialization.StoreIdentity));
                ApplyCapacity(StoreIdentityCapacity(initialization.StoreIdentity));
            }

            _storeIdentity = initialization.StoreIdentity;
        }

        return default;
    }

    /// <inheritdoc/>
    public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateStreamId(streamId, nameof(streamId));
        if (preferredId.HasValue && preferredId.Value.Value == Guid.Empty)
        {
            throw new ArgumentException("Preferred subscription id must be non-empty.", nameof(preferredId));
        }

        cancellationToken.ThrowIfCancellationRequested();
        SubscriptionId result;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            if (_streams.TryGetValue(streamId, out var existing))
            {
                if (preferredId.HasValue && existing.SubscriptionId != preferredId.Value)
                {
                    throw new InvalidOperationException("The preferred subscription identity does not match the stored identity.");
                }

                result = existing.SubscriptionId;
            }
            else
            {
                result = preferredId ?? SubscriptionId.New();
                var stream = new StreamRecord(result);
                var capacity = StreamRecordCapacity(streamId, stream);
                EnsureCapacityFor(capacity);
                _streams.Add(streamId, stream);
                ApplyCapacity(capacity);
            }
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<RecoveredStream> RecoverStreamAsync(
        StreamId streamId,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateRecoveryInput(streamId, subscriptionId);
        cancellationToken.ThrowIfCancellationRequested();
        RecoveredStream result;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var stream = GetStream(streamId);
            if (stream.SubscriptionId != subscriptionId)
            {
                throw new InvalidOperationException("The recovered subscription identity does not match the requested identity.");
            }

            List<SyncOperation> pending = [];
            foreach (var pair in _operations)
            {
                AddRecoveredOperation(streamId, pair.Value, pending);
            }

            pending.Sort(CompareOperationSequence);
            result = new(
                stream.SubscriptionId,
                stream.ServerCursor,
                stream.Snapshot,
                pending,
                [],
                stream.NextClientSequence);
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateCommitInput(operation, snapshotMutation);
        cancellationToken.ThrowIfCancellationRequested();
        LocalCommitResult result;
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            if (_operations.TryGetValue(operation.OperationId, out var duplicate))
            {
                result = GetDuplicateReceipt(duplicate, operation, snapshotMutation);
            }
            else
            {
                var stream = GetStream(operation.StreamId);
                ValidateCommitVersion(stream, operation, snapshotMutation);
                var committedAtUtc = nowUtc;
                var nextRevision = checked(snapshotMutation.ExpectedRevision + 1);
                var nextClientSequence = checked(operation.ClientSequence + 1);
                var nextSnapshot = new LocalSnapshot(
                    operation.StreamId,
                    snapshotMutation.FormatVersion,
                    stream.ServerCursor,
                    snapshotMutation.State,
                    nextRevision,
                    committedAtUtc);
                result = new(operation.OperationId, operation.ClientSequence, nextRevision, committedAtUtc);
                var record = new OperationRecord(operation, snapshotMutation, result, CreateStatus(operation, SyncOperationState.SavedLocally, 0, committedAtUtc, null));
                var capacity = AddCapacity(
                    OperationRecordCapacity(record),
                    CapacityDifference(LocalSnapshotCapacity(stream.Snapshot), LocalSnapshotCapacity(nextSnapshot)));
                EnsureCapacityFor(capacity);
                _operations.Add(operation.OperationId, record);
                ApplyCapacity(capacity);
                stream.Snapshot = nextSnapshot;
                stream.NextClientSequence = nextClientSequence;
            }
        }

        return new(result);
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
        OutboxLeaseRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = LeasePendingOperationBatch(request, cancellationToken);
        await Task.CompletedTask.ConfigureAwait(false);
        if (batch is not null)
        {
            yield return batch;
        }
    }

    /// <inheritdoc/>
    public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        ArgumentExceptionHelper.ThrowIfNull(result);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var lease = GetActiveLease(leaseId, nowUtc);
            var operations = GetLeaseOperations(lease);
            SyncBatchValidator.Validate(new(leaseId, operations), result);
            var statuses = CreateStatusesFromResult(result, nowUtc);
            ApplyStatusesAndReleaseLease(leaseId, statuses, nowUtc);
        }

        return default;
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
        StreamId streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateInboxLookupInput(streamId, eventIds);
        cancellationToken.ThrowIfCancellationRequested();
        IReadOnlyList<Guid> result;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            List<Guid> unapplied = [];
            for (var index = 0; index < eventIds.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!_inbox.Contains(new(streamId, eventIds[index])))
                {
                    unapplied.Add(eventIds[index]);
                }
            }

            result = new ReadOnlyCollection<Guid>(unapplied);
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateRemoteApplyInput(batch, snapshotMutation);
        cancellationToken.ThrowIfCancellationRequested();
        RemoteApplyResult result;
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var stream = GetStream(batch.StreamId);
            ValidateRemoteVersion(stream, batch, snapshotMutation);
            EnsureRemoteEventsUnapplied(batch);
            var committedAtUtc = nowUtc;
            var nextRevision = checked(snapshotMutation.ExpectedRevision + 1);
            var nextSnapshot = new LocalSnapshot(batch.StreamId, snapshotMutation.FormatVersion, batch.NextCursor, snapshotMutation.State, nextRevision, committedAtUtc);
            var capacity = AddCapacity(
                new(0, checked(StringBytes(batch.NextCursor) - StringBytes(stream.ServerCursor))),
                CapacityDifference(LocalSnapshotCapacity(stream.Snapshot), LocalSnapshotCapacity(nextSnapshot)));
            for (var index = 0; index < batch.Events.Count; index++)
            {
                capacity = AddCapacity(capacity, InboxKeyCapacity(new(batch.StreamId, batch.Events[index].EventId)));
            }

            EnsureCapacityFor(capacity);
            AddInboxEntries(batch);
            ApplyCapacity(capacity);
            stream.Snapshot = nextSnapshot;
            stream.ServerCursor = batch.NextCursor;
            result = new(batch.NextCursor, batch.Events.Count, DuplicateCount: 0, nextRevision);
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        cancellationToken.ThrowIfCancellationRequested();
        SyncOperationStatus? result;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            result = _operations.TryGetValue(operationId, out var record) ? record.Status : null;
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        cancellationToken.ThrowIfCancellationRequested();
        RetryState? result;
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            result = _operations.TryGetValue(operationId, out var record) ? record.RetryState : null;
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateAttemptInput(leaseId, operationId, nextAttempt);
        cancellationToken.ThrowIfCancellationRequested();
        AttemptBarrierResult result;
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var lease = GetActiveLease(leaseId, nowUtc);
            if (!lease.Owns(operationId))
            {
                throw new InvalidOperationException("The lease does not own the operation.");
            }

            var record = _operations[operationId];
            if (record.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce && record.Attempt > 0)
            {
                result = new(operationId, nextAttempt, MaySend: false, AtMostOnceAttemptRecordedReason);
            }
            else if (nextAttempt <= record.Attempt)
            {
                result = new(operationId, nextAttempt, MaySend: false, "OC.AttemptAlreadyRecorded");
            }
            else
            {
                var state = GetAttemptState(record.Operation.Policy);
                var status = CreateStatus(record.Operation, state, nextAttempt, nowUtc, null);
                var terminalAtUtc = record.TerminalAtUtc;
                if (state == SyncOperationState.Ambiguous)
                {
                    terminalAtUtc = nowUtc;
                }

                var capacity = CapacityDifference(
                    OperationRecordCapacity(record),
                    OperationRecordCapacity(record, status, record.RetryState, record.LeaseId, record.LeaseExpiresAtUtc, terminalAtUtc));
                EnsureCapacityFor(capacity);
                record.Attempt = nextAttempt;
                record.Status = status;
                if (state == SyncOperationState.Ambiguous)
                {
                    record.TerminalAtUtc = nowUtc;
                }

                ApplyCapacity(capacity);
                result = new(operationId, nextAttempt, MaySend: true, ReasonCode: null);
            }
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateOperationId(operationId, nameof(operationId));
        InMemoryLocalStoreAdapterValidation.ValidateRetryState(retryState);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var record = GetOperation(operationId);
            if (IsDefinitiveTerminal(record.Status.State) || IsBlockingHead(record.Status.State)
                || (record.Attempt > 0 && record.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce))
            {
                throw new InvalidOperationException("The operation cannot be rescheduled from its current state.");
            }

            var capacity = CapacityDifference(
                OperationRecordCapacity(record),
                OperationRecordCapacity(record, record.Status, retryState, record.LeaseId, record.LeaseExpiresAtUtc, record.TerminalAtUtc));
            EnsureCapacityFor(capacity);
            record.RetryState = retryState;
            ApplyCapacity(capacity);
        }

        return default;
    }

    /// <inheritdoc/>
    public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseRenewalInput(leaseId, extension);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var lease = GetActiveLease(leaseId, nowUtc);
            lease.ExpiresAtUtc = CheckedAdd(lease.ExpiresAtUtc, extension, nameof(extension));
        }

        return default;
    }

    /// <inheritdoc/>
    public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseId(leaseId);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var lease = GetActiveLease(leaseId, nowUtc);
            ReleaseLeaseCore(leaseId, lease);
        }

        return default;
    }

    /// <inheritdoc/>
    public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        if (request.TargetBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.TargetBytes, "Target bytes must not be negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        CompactionResult result;
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            var removable = GetCompactableOperations(request, nowUtc);
            removable.Sort(CompareTerminalTime);
            result = RemoveCompactedOperations(removable, request);
        }

        return new(result);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _disposed = true;
            _streams.Clear();
            _operations.Clear();
            _leases.Clear();
            _inbox.Clear();
            _encodedBytes = 0;
            _recordCount = 0;
            _storeIdentity = null;
        }

        return default;
    }
}
