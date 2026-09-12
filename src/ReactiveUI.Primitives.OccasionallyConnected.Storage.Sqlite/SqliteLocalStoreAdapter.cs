// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists occasionally connected stream state in SQLite on a bounded single-command worker.</summary>
[DebuggerDisplay("Capabilities = {Capabilities}")]
public sealed class SqliteLocalStoreAdapter : ILocalStoreAdapter
{
    /// <summary>The current SQLite local commit backend schema version.</summary>
    private const int CurrentSchemaVersion = SqliteStoreSchema.LocalCommitSchemaVersion;

    /// <summary>The local store capabilities backed by the SQLite implementation.</summary>
    private const LocalStoreCapabilities SupportedCapabilities =
        LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.DurableLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply
        | LocalStoreCapabilities.DurableInbox
        | LocalStoreCapabilities.LeasedOutbox;

    /// <summary>The synchronous SQLite implementation.</summary>
    private readonly SqliteLocalCommitStore _store;

    /// <summary>The database path used for single-writer ownership.</summary>
    private readonly string _databasePath;

    /// <summary>The gate for input snapshots captured before worker admission.</summary>
    private readonly Lock _captureGate = new();

    /// <summary>The bounded single worker used for all SQLite store operations.</summary>
    private readonly SqliteSynchronousCommandWorker _worker;

    /// <summary>The retention policy used for compaction.</summary>
    private readonly RetentionOptions _retention;

    /// <summary>The capacity-bound retained input sizer.</summary>
    private readonly SqliteLocalStoreAdapterSizing _sizing;

    /// <summary>The maximum retained caller input bytes admitted to the SQLite worker when empty.</summary>
    private readonly long _workerCapacityBytes;

    /// <summary>The maximum admitted SQLite commands, including the active command.</summary>
    private readonly int _workerCapacity;

    /// <summary>The retained caller input bytes currently held before or inside the SQLite worker.</summary>
    private long _capturedInputBytes;

    /// <summary>The caller input snapshots currently held before or inside the SQLite worker.</summary>
    private int _capturedInputCount;

    /// <summary>The process-owned single-writer ownership handle.</summary>
    private SqliteSingleWriterOwnership? _ownership;

    /// <summary>A value indicating whether capture-stage admission is closed.</summary>
    private bool _captureAdmissionClosed;

    /// <summary>Completes when capture-stage work admitted before disposal drains.</summary>
    private TaskCompletionSource<bool>? _captureDrained;

    /// <summary>Initializes a new instance of the <see cref="SqliteLocalStoreAdapter"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <exception cref="ArgumentException">The database path is blank or not a real file path.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="databasePath"/> is null.</exception>
    public SqliteLocalStoreAdapter(string databasePath)
        : this(databasePath, new SqliteLocalStoreAdapterOptions())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteLocalStoreAdapter"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="options">The adapter options.</param>
    /// <exception cref="ArgumentException">The database path is blank or not a real file path.</exception>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A worker bound or retention interval is not positive.</exception>
    public SqliteLocalStoreAdapter(string databasePath, SqliteLocalStoreAdapterOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _retention = options.Retention;
        _sizing = new(options.WorkerCapacityBytes);
        _workerCapacityBytes = options.WorkerCapacityBytes;
        _workerCapacity = options.WorkerCapacity;
        SqliteLocalCommitValidation.ThrowIfBlank(databasePath, nameof(databasePath), "The SQLite database path cannot be empty.");
        SqliteLocalCommitValidation.ThrowIfUnsupportedPath(databasePath);
        _databasePath = Path.GetFullPath(databasePath);
        _store = new(_databasePath, options.TimeProvider);
        _worker = new(options.WorkerCapacity, options.WorkerCapacityBytes);
    }

    /// <inheritdoc/>
    public LocalStoreCapabilities Capabilities => SupportedCapabilities;

    /// <inheritdoc/>
    public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken)
    {
        var backendInitialization = CreateBackendInitialization(initialization);
        return new(ExecuteAsync(
            token =>
            {
                var acquiredOwnership = EnsureOwnership();
                try
                {
                    _store.Initialize(backendInitialization, token);
                }
                catch
                {
                    ReleaseOwnershipIfNew(acquiredOwnership);
                    throw;
                }

                return true;
            },
            _sizing.InitializationBytes(initialization),
            cancellationToken));
    }

    /// <inheritdoc/>
    public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
        StreamId streamId,
        SubscriptionId? preferredId,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.GetOrCreateSubscriptionId(streamId, preferredId, token),
            _sizing.SubscriptionLookupBytes(streamId, preferredId),
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<RecoveredStream> RecoverStreamAsync(
        StreamId streamId,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.RecoverStream(streamId, subscriptionId, token),
            _sizing.RecoveryBytes(streamId),
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
        SyncOperation operation,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.CommitLocalOperation(operation, snapshotMutation, token),
            _sizing.CommitBytes(operation, snapshotMutation),
            cancellationToken));

    /// <inheritdoc/>
    public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
        OutboxLeaseRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = await ExecuteAsync(
            token => _store.LeasePendingOperationBatch(request, token),
            _sizing.LeaseRequestBytes(request),
            cancellationToken).ConfigureAwait(false);
        if (batch is not null)
        {
            yield return batch;
        }
    }

    /// <inheritdoc/>
    public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token =>
            {
                _store.ApplySyncResult(leaseId, result, token);
                return true;
            },
            _sizing.SyncResultBytes(result),
            cancellationToken));

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
        StreamId streamId,
        IReadOnlyList<Guid> eventIds,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(eventIds);
        cancellationToken.ThrowIfCancellationRequested();
        var count = eventIds.Count;
        var retainedBytes = _sizing.EventIdLookupBytes(streamId, count);
        ReserveCapture(retainedBytes);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            List<Guid> eventIdList = [with(capacity: count)];
            for (var index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                eventIdList.Add(eventIds[index]);
            }

            var eventIdSnapshot = new ReadOnlyCollection<Guid>(eventIdList);
            return await ExecuteAsync(
                token => _store.GetUnappliedEventIds(streamId, eventIdSnapshot, token),
                retainedBytes,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseCapture(retainedBytes);
        }
    }

    /// <inheritdoc/>
    public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        SnapshotMutation snapshotMutation,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.ApplyRemoteBatch(batch, snapshotMutation, token),
            _sizing.RemoteApplyBytes(batch, snapshotMutation),
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.GetOperationStatus(operationId, token),
            _sizing.OperationIdentifierBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.GetRetryState(operationId, token),
            _sizing.OperationIdentifierBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
        Guid leaseId,
        OperationId operationId,
        int nextAttempt,
        CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.TryBeginRemoteAttempt(leaseId, operationId, nextAttempt, token),
            _sizing.AttemptBarrierBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token =>
            {
                _store.SaveRetryState(operationId, retryState, token);
                return true;
            },
            _sizing.RetryStateBytes(retryState),
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token =>
            {
                _store.RenewLease(leaseId, extension, token);
                return true;
            },
            _sizing.LeaseMutationBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token =>
            {
                _store.ReleaseLease(leaseId, token);
                return true;
            },
            _sizing.LeaseMutationBytes,
            cancellationToken));

    /// <inheritdoc/>
    public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
        new(ExecuteAsync(
            token => _store.Compact(request, _retention, token),
            _sizing.CompactionBytes(request),
            cancellationToken));

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        var captureDrainTask = CloseCaptureAdmission();
        var workerDrain = _worker.DisposeAsync();
        await captureDrainTask.ConfigureAwait(false);
        await workerDrain.ConfigureAwait(false);
        _store.Dispose();
        _ownership?.Dispose();
    }

    /// <summary>Maps public initialization requirements to the current SQLite backend schema.</summary>
    /// <param name="initialization">The public initialization requirements.</param>
    /// <returns>The backend initialization requirements.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="initialization"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The required schema version is not positive.</exception>
    /// <exception cref="NotSupportedException">The caller requires a newer schema than this adapter supports.</exception>
    private static LocalStoreInitialization CreateBackendInitialization(LocalStoreInitialization initialization)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        if (initialization.RequiredSchemaVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialization), initialization.RequiredSchemaVersion, "Required schema version must be positive.");
        }

        if (initialization.RequiredSchemaVersion > CurrentSchemaVersion)
        {
            throw new NotSupportedException("The SQLite local store does not support the required schema version.");
        }

        return initialization with { RequiredSchemaVersion = CurrentSchemaVersion };
    }

    /// <summary>Acquires the single-writer owner handle once for this adapter.</summary>
    /// <returns>The new owner when this call acquired it; otherwise, null.</returns>
    private SqliteSingleWriterOwnership? EnsureOwnership()
    {
        if (_ownership is not null)
        {
            return null;
        }

        var ownership = SqliteSingleWriterOwnership.Acquire(_databasePath);
        _ownership = ownership;
        return ownership;
    }

    /// <summary>Releases ownership acquired by a failed initialization attempt.</summary>
    /// <param name="acquiredOwnership">The owner acquired by the current call, if any.</param>
    private void ReleaseOwnershipIfNew(SqliteSingleWriterOwnership? acquiredOwnership)
    {
        if (acquiredOwnership is null)
        {
            return;
        }

        _ownership = null;
        acquiredOwnership.Dispose();
    }

    /// <summary>Reserves bounded capture-stage ownership before copying caller input.</summary>
    /// <param name="retainedBytes">The retained caller input byte count.</param>
    /// <exception cref="ObjectDisposedException">Capture admission is closed.</exception>
    /// <exception cref="QueueCapacityExceededException">The input cannot be captured within adapter bounds.</exception>
    private void ReserveCapture(long retainedBytes)
    {
        lock (_captureGate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_captureAdmissionClosed, this);
            if (_capturedInputCount >= _workerCapacity || _capturedInputBytes > _workerCapacityBytes - retainedBytes)
            {
                throw new QueueCapacityExceededException("The SQLite command worker has reached its configured capacity.", canFitWhenEmpty: true);
            }

            _capturedInputCount++;
            _capturedInputBytes += retainedBytes;
        }
    }

    /// <summary>Closes capture-stage admission before worker disposal rejects queued commands.</summary>
    /// <returns>The task that completes when admitted capture work drains.</returns>
    private Task CloseCaptureAdmission()
    {
        Task captureDrainTask;
        lock (_captureGate)
        {
            _captureAdmissionClosed = true;
            if (_capturedInputCount == 0)
            {
                captureDrainTask = Task.CompletedTask;
            }
            else
            {
                _captureDrained ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                captureDrainTask = _captureDrained.Task;
            }
        }

        return captureDrainTask;
    }

    /// <summary>Queues a command on the bounded worker.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="command">The synchronous command.</param>
    /// <param name="retainedBytes">The retained caller input byte count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The queued command task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task<T> ExecuteAsync<T>(
        Func<CancellationToken, T> command,
        long retainedBytes,
        CancellationToken cancellationToken) =>
        _worker.ExecuteAsync(command, Math.Max(retainedBytes, SqliteLocalStoreAdapterSizing.MinimumCommandBytes), cancellationToken);

    /// <summary>Releases a capture-stage reservation.</summary>
    /// <param name="retainedBytes">The retained caller input byte count.</param>
    private void ReleaseCapture(long retainedBytes)
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_captureGate)
        {
            _capturedInputCount--;
            _capturedInputBytes -= retainedBytes;
            if (_captureAdmissionClosed && _capturedInputCount == 0)
            {
                drained = _captureDrained;
                _captureDrained = null;
            }
        }

        _ = drained?.TrySetResult(true);
    }
}
