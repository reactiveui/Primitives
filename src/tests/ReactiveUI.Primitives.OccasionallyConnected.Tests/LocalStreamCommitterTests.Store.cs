// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>A scripted fake atomic store.</summary>
    private sealed class ScriptedLocalStore : ILocalStoreAdapter
    {
        /// <summary>The event identifiers recorded in the durable inbox.</summary>
        private readonly HashSet<Guid> _appliedEventIds = [];

        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities { get; init; } =
            LocalStoreCapabilities.AtomicLocalCommit
            | LocalStoreCapabilities.DurableLocalCommit
            | LocalStoreCapabilities.AtomicRemoteApply
            | LocalStoreCapabilities.DurableInbox
            | LocalStoreCapabilities.LeasedOutbox;

        /// <summary>Gets or sets the recovered stream returned by recovery.</summary>
        public RecoveredStream Recovery { get; set; } = CreateRecoveredStream(null, [], 1);

        /// <summary>Gets or sets the exception thrown by local commit.</summary>
        public Exception? CommitException { get; set; }

        /// <summary>Gets or sets asynchronous work to run before commit.</summary>
        public Func<Task>? BeforeCommitAsync { get; set; }

        /// <summary>Gets or sets asynchronous work before the remote transaction.</summary>
        public Func<Task>? BeforeRemoteCommitAsync { get; set; }

        /// <summary>Gets or sets a remote transaction failure before persistence.</summary>
        public Exception? RemoteCommitException { get; set; }

        /// <summary>Gets or sets asynchronous work before the upload result transaction.</summary>
        public Func<Task>? BeforeResultCommitAsync { get; set; }

        /// <summary>Gets or sets an upload result transaction failure before persistence.</summary>
        public Exception? ResultCommitException { get; set; }

        /// <summary>Gets or sets asynchronous work before the dead-letter transaction.</summary>
        public Func<Task>? BeforeDeadLetterCommitAsync { get; set; }

        /// <summary>Gets or sets a dead-letter transaction failure before persistence.</summary>
        public Exception? DeadLetterCommitException { get; set; }

        /// <summary>Gets or sets a transformation simulating a malformed adapter receipt.</summary>
        public Func<RemoteApplyResult, RemoteApplyResult>? TransformRemoteReceipt { get; set; }

        /// <summary>Gets or sets a transformation simulating a malformed result adapter receipt.</summary>
        public Func<IReadOnlyList<LocalSnapshot>, IReadOnlyList<LocalSnapshot>>? TransformResultSnapshots { get; set; }

        /// <summary>Gets or sets a transformation simulating a malformed dead-letter adapter receipt.</summary>
        public Func<LocalSnapshot, LocalSnapshot>? TransformDeadLetterSnapshot { get; set; }

        /// <summary>Gets or sets asynchronous work to run before recovery.</summary>
        public Func<Task>? BeforeRecoveryAsync { get; set; }

        /// <summary>Gets or sets the token source canceled after successful commit.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulCommit { get; set; }

        /// <summary>Gets or sets the token source canceled after successful remote apply.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulRemoteApply { get; set; }

        /// <summary>Gets or sets the token source canceled after successful upload result apply.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulResultApply { get; set; }

        /// <summary>Gets or sets the token source canceled after successful dead-letter apply.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulDeadLetterApply { get; set; }

        /// <summary>Gets or sets the sequence offset applied to the returned receipt.</summary>
        public long ReceiptSequenceOffset { get; set; }

        /// <summary>Gets or sets the revision offset applied to the returned remote receipt.</summary>
        public long RemoteReceiptRevisionOffset { get; set; }

        /// <summary>Gets or sets the revision offset applied to returned result snapshots.</summary>
        public long ResultSnapshotRevisionOffset { get; set; }

        /// <summary>Gets or sets the revision offset applied to returned dead-letter snapshots.</summary>
        public long DeadLetterSnapshotRevisionOffset { get; set; }

        /// <summary>Gets or sets a value indicating whether commit returns a null receipt.</summary>
        public bool ReturnNullCommitResult { get; set; }

        /// <summary>Gets or sets a value indicating whether remote apply returns a null receipt.</summary>
        public bool ReturnNullRemoteApplyResult { get; set; }

        /// <summary>Gets or sets a value indicating whether result apply returns a null snapshot receipt.</summary>
        public bool ReturnNullResultSnapshots { get; set; }

        /// <summary>Gets or sets the event identifiers returned by the inbox lookup.</summary>
        public IReadOnlyList<Guid>? UnappliedEventIdsOverride { get; set; }

        /// <summary>Gets or sets a value indicating whether the inbox lookup returns no result.</summary>
        public bool ReturnNullUnappliedLookupResult { get; set; }

        /// <summary>Gets the last committed operation.</summary>
        public SyncOperation? CommittedOperation { get; private set; }

        /// <summary>Gets the last committed snapshot mutation.</summary>
        public SnapshotMutation? CommittedSnapshot { get; private set; }

        /// <summary>Gets the last applied remote batch.</summary>
        public RemoteEventBatch? AppliedRemoteBatch { get; private set; }

        /// <summary>Gets the last applied remote snapshot mutation.</summary>
        public SnapshotMutation? AppliedRemoteSnapshot { get; private set; }

        /// <summary>Gets the commit call count.</summary>
        public int CommitCallCount { get; private set; }

        /// <summary>Gets the unapplied inbox lookup call count.</summary>
        public int UnappliedLookupCallCount { get; private set; }

        /// <summary>Gets the remote apply call count.</summary>
        public int RemoteApplyCallCount { get; private set; }

        /// <summary>Gets the result apply call count.</summary>
        public int ResultApplyCallCount { get; private set; }

        /// <summary>Gets the dead-letter apply call count.</summary>
        public int DeadLetterApplyCallCount { get; private set; }

        /// <summary>Marks a remote event identifier as already applied.</summary>
        /// <param name="eventId">The remote event identifier.</param>
        /// <returns><see langword="true"/> when the identifier was not already present.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MarkEventApplied(Guid eventId) => _appliedEventIds.Add(eventId);

        /// <inheritdoc/>
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public async ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken)
        {
            if (BeforeRecoveryAsync is not null)
            {
                await BeforeRecoveryAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return Recovery;
        }

        /// <inheritdoc/>
        public async ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            CommitCallCount++;
            if (BeforeCommitAsync is not null)
            {
                await BeforeCommitAsync().ConfigureAwait(false);
            }

            if (CommitException is not null)
            {
                throw CommitException;
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (operation.ClientSequence != Recovery.NextClientSequence
                || snapshotMutation.ExpectedRevision != (Recovery.Snapshot?.Revision ?? 0))
            {
                throw new InvalidOperationException("The stream revision or client sequence is stale.");
            }

            CommittedOperation = operation;
            CommittedSnapshot = snapshotMutation;
            List<SyncOperation> pending = new(Recovery.PendingOperations) { operation };
            var snapshot = new LocalSnapshot(
                operation.StreamId,
                snapshotMutation.FormatVersion,
                Recovery.ServerCursor,
                snapshotMutation.State,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc) { AuthoritativeState = SelectAuthoritativePayload(snapshotMutation) };
            Recovery = new(Subscription, Recovery.ServerCursor, snapshot, pending, Recovery.DeadLetters, operation.ClientSequence + 1);
            _ = CancelAfterSuccessfulCommit?.CancelAsync();
            return ReturnNullCommitResult
                ? await default(ValueTask<LocalCommitResult>)
                : new(
                operation.OperationId,
                operation.ClientSequence + ReceiptSequenceOffset,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc);
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public async ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken)
        {
            ResultApplyCallCount++;
            await RunBeforeResultCommitAsync(cancellationToken).ConfigureAwait(false);
            var rejected = CreateRejectedOperationSet(result);
            var retained = CreateRetainedResultOperations(rejected);
            var snapshots = CreateResultSnapshots(snapshotMutations);
            ApplyResultRecovery(snapshots, retained);
            _ = CancelAfterSuccessfulResultApply?.CancelAsync();
            return await CreateResultSnapshotReceiptAsync(snapshots).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            DeadLetterApplyCallCount++;
            await RunBeforeDeadLetterCommitAsync(cancellationToken).ConfigureAwait(false);
            var snapshot = CreateDeadLetterSnapshot(snapshotMutation);
            ApplyDeadLetterRecovery(operationId, reasonCode, snapshot);
            _ = CancelAfterSuccessfulDeadLetterApply?.CancelAsync();
            return TransformDeadLetterSnapshot is null ? snapshot : TransformDeadLetterSnapshot(snapshot);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken)
        {
            UnappliedLookupCallCount++;
            cancellationToken.ThrowIfCancellationRequested();
            if (ReturnNullUnappliedLookupResult)
            {
                return default;
            }

            if (UnappliedEventIdsOverride is not null)
            {
                return ValueTask.FromResult(UnappliedEventIdsOverride);
            }

            List<Guid> unapplied = [];
            for (var index = 0; index < eventIds.Count; index++)
            {
                var eventId = eventIds[index];
                if (!_appliedEventIds.Contains(eventId))
                {
                    unapplied.Add(eventId);
                }
            }

            return ValueTask.FromResult<IReadOnlyList<Guid>>(new ReadOnlyCollection<Guid>(unapplied));
        }

        /// <inheritdoc/>
        public async ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            RemoteApplyCallCount++;
            if (BeforeRemoteCommitAsync is not null)
            {
                await BeforeRemoteCommitAsync();
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (RemoteCommitException is not null)
            {
                throw RemoteCommitException;
            }

            if (batch.StreamId != Stream
                || !string.Equals(batch.PreviousCursor, Recovery.ServerCursor, StringComparison.Ordinal)
                || snapshotMutation.ExpectedRevision != (Recovery.Snapshot?.Revision ?? 0))
            {
                throw new InvalidOperationException("The stream cursor or snapshot revision is stale.");
            }

            AppliedRemoteBatch = batch;
            AppliedRemoteSnapshot = snapshotMutation;
            var previousEventCount = _appliedEventIds.Count;
            for (var index = 0; index < batch.Events.Count; index++)
            {
                _ = _appliedEventIds.Add(batch.Events[index].EventId);
            }

            var snapshot = new LocalSnapshot(
                batch.StreamId,
                snapshotMutation.FormatVersion,
                batch.NextCursor,
                snapshotMutation.State,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc) { AuthoritativeState = SelectAuthoritativePayload(snapshotMutation) };
            Recovery = new(Subscription, batch.NextCursor, snapshot, Recovery.PendingOperations, Recovery.DeadLetters, Recovery.NextClientSequence);
            _ = CancelAfterSuccessfulRemoteApply?.CancelAsync();
            return await CreateRemoteReceiptAsync(batch, snapshotMutation.ExpectedRevision, _appliedEventIds.Count - previousEventCount);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            ValueTask.FromResult<SyncOperationStatus?>(null);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            ValueTask.FromResult<RetryState?>(null);

        /// <inheritdoc/>
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Creates the operation identifiers rejected by the upload result.</summary>
        /// <param name="result">The upload result.</param>
        /// <returns>The rejected operation identifiers.</returns>
        private static HashSet<OperationId> CreateRejectedOperationSet(RemoteSyncResult result)
        {
            HashSet<OperationId> rejected = [];
            for (var index = 0; index < result.Operations.Count; index++)
            {
                if (result.Operations[index].Kind == OperationResultKind.Rejected)
                {
                    _ = rejected.Add(result.Operations[index].OperationId);
                }
            }

            return rejected;
        }

        /// <summary>Runs configured precommit behavior for upload result tests.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The asynchronous operation.</returns>
        private async ValueTask RunBeforeResultCommitAsync(CancellationToken cancellationToken)
        {
            if (BeforeResultCommitAsync is not null)
            {
                await BeforeResultCommitAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (ResultCommitException is not null)
            {
                throw ResultCommitException;
            }
        }

        /// <summary>Runs configured precommit behavior for dead-letter tests.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The asynchronous operation.</returns>
        private async ValueTask RunBeforeDeadLetterCommitAsync(CancellationToken cancellationToken)
        {
            if (BeforeDeadLetterCommitAsync is not null)
            {
                await BeforeDeadLetterCommitAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (DeadLetterCommitException is not null)
            {
                throw DeadLetterCommitException;
            }
        }

        /// <summary>Creates retained pending and replay operations after rejected entries are removed.</summary>
        /// <param name="rejected">The rejected operation identifiers.</param>
        /// <returns>The retained operations.</returns>
        private (List<SyncOperation> Pending, List<SyncOperation> Replay) CreateRetainedResultOperations(HashSet<OperationId> rejected)
        {
            List<SyncOperation> pending = [];
            List<SyncOperation> replay = [];
            for (var index = 0; index < Recovery.PendingOperations.Count; index++)
            {
                var operation = Recovery.PendingOperations[index];
                if (rejected.Contains(operation.OperationId))
                {
                    continue;
                }

                pending.Add(operation);
                replay.Add(operation);
            }

            return (pending, replay);
        }

        /// <summary>Creates snapshots returned by the upload result test transaction.</summary>
        /// <param name="snapshotMutations">The requested mutations.</param>
        /// <returns>The committed snapshots.</returns>
        private List<LocalSnapshot> CreateResultSnapshots(IReadOnlyList<SnapshotMutation> snapshotMutations)
        {
            List<LocalSnapshot> snapshots = [];
            for (var index = 0; index < snapshotMutations.Count; index++)
            {
                snapshots.Add(CreateResultSnapshot(snapshotMutations[index]));
            }

            return snapshots;
        }

        /// <summary>Creates one snapshot returned by the upload result test transaction.</summary>
        /// <param name="mutation">The requested mutation.</param>
        /// <returns>The committed snapshot.</returns>
        /// <exception cref="InvalidOperationException">The fake store has no current snapshot.</exception>
        private LocalSnapshot CreateResultSnapshot(SnapshotMutation mutation)
        {
            var current = Recovery.Snapshot ?? throw new InvalidOperationException("The stream requires a snapshot.");
            return new(
                mutation.StreamId,
                mutation.FormatVersion,
                Recovery.ServerCursor,
                mutation.State,
                mutation.ExpectedRevision + 1 + ResultSnapshotRevisionOffset,
                CommittedUtc) { AuthoritativeState = current.AuthoritativeState };
        }

        /// <summary>Creates the snapshot returned by the dead-letter test transaction.</summary>
        /// <param name="mutation">The requested mutation.</param>
        /// <returns>The committed snapshot.</returns>
        /// <exception cref="InvalidOperationException">The recovered stream has no snapshot.</exception>
        private LocalSnapshot CreateDeadLetterSnapshot(SnapshotMutation mutation)
        {
            var current = Recovery.Snapshot ?? throw new InvalidOperationException("The stream requires a snapshot.");
            return new(
                mutation.StreamId,
                mutation.FormatVersion,
                Recovery.ServerCursor,
                mutation.State,
                mutation.ExpectedRevision + 1 + DeadLetterSnapshotRevisionOffset,
                CommittedUtc) { AuthoritativeState = current.AuthoritativeState };
        }

        /// <summary>Applies successful fake result recovery state.</summary>
        /// <param name="snapshots">The committed snapshots.</param>
        /// <param name="retained">The retained operations.</param>
        private void ApplyResultRecovery(
            List<LocalSnapshot> snapshots,
            (List<SyncOperation> Pending, List<SyncOperation> Replay) retained)
        {
            if (snapshots.Count == 0)
            {
                return;
            }

            var recovery = new RecoveredStream(Recovery.SubscriptionId, Recovery.ServerCursor, snapshots[0], retained.Pending, Recovery.DeadLetters, Recovery.NextClientSequence);
            Recovery = recovery with { ReplayOperations = retained.Replay };
        }

        /// <summary>Applies successful fake dead-letter recovery state.</summary>
        /// <param name="operationId">The dead-lettered operation identifier.</param>
        /// <param name="reasonCode">The reason code.</param>
        /// <param name="snapshot">The committed snapshot.</param>
        private void ApplyDeadLetterRecovery(OperationId operationId, string reasonCode, LocalSnapshot snapshot)
        {
            List<SyncOperation> pending = [];
            List<SyncOperation> replay = [];
            SyncOperation? deadLetter = null;
            for (var index = 0; index < Recovery.PendingOperations.Count; index++)
            {
                var operation = Recovery.PendingOperations[index];
                if (operation.OperationId == operationId)
                {
                    deadLetter = operation;
                    continue;
                }

                pending.Add(operation);
            }

            for (var index = 0; index < Recovery.ReplayOperations.Count; index++)
            {
                var operation = Recovery.ReplayOperations[index];
                if (operation.OperationId != operationId)
                {
                    replay.Add(operation);
                }
            }

            deadLetter ??= FindReplayOperation(operationId);
            List<DeadLetterRecord> deadLetters = [.. Recovery.DeadLetters, new(deadLetter, reasonCode, Attempts: 0, CommittedUtc)];
            var recovery = new RecoveredStream(Recovery.SubscriptionId, Recovery.ServerCursor, snapshot, pending, deadLetters, Recovery.NextClientSequence);
            Recovery = recovery with { ReplayOperations = replay };
        }

        /// <summary>Finds a replay operation by identifier.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <returns>The operation.</returns>
        /// <exception cref="InvalidOperationException">The operation is missing.</exception>
        private SyncOperation FindReplayOperation(OperationId operationId)
        {
            for (var index = 0; index < Recovery.ReplayOperations.Count; index++)
            {
                var operation = Recovery.ReplayOperations[index];
                if (operation.OperationId == operationId)
                {
                    return operation;
                }
            }

            throw new InvalidOperationException("The fake store does not contain the dead-letter target.");
        }

        /// <summary>Creates the fake store receipt for upload result tests.</summary>
        /// <param name="snapshots">The committed snapshots.</param>
        /// <returns>The snapshot receipt.</returns>
        private async ValueTask<IReadOnlyList<LocalSnapshot>> CreateResultSnapshotReceiptAsync(List<LocalSnapshot> snapshots)
        {
            if (ReturnNullResultSnapshots)
            {
                return await default(ValueTask<IReadOnlyList<LocalSnapshot>>);
            }

            IReadOnlyList<LocalSnapshot> resultSnapshots = new ReadOnlyCollection<LocalSnapshot>(snapshots);
            return TransformResultSnapshots is null ? resultSnapshots : TransformResultSnapshots(resultSnapshots);
        }

        /// <summary>Selects a replacement or preserved authoritative payload.</summary>
        /// <param name="mutation">The snapshot mutation.</param>
        /// <returns>The next authoritative payload.</returns>
        private PayloadEnvelope? SelectAuthoritativePayload(SnapshotMutation mutation) =>
            mutation.AuthoritativeState ?? Recovery.Snapshot?.AuthoritativeState;

        /// <summary>Creates the configurable remote receipt after persistence.</summary>
        /// <param name="batch">The persisted batch.</param>
        /// <param name="expectedRevision">The preceding revision.</param>
        /// <param name="appliedCount">The number of new inbox entries.</param>
        /// <returns>The configured adapter receipt.</returns>
        private async ValueTask<RemoteApplyResult> CreateRemoteReceiptAsync(RemoteEventBatch batch, long expectedRevision, int appliedCount)
        {
            var receipt = ReturnNullRemoteApplyResult
                ? await default(ValueTask<RemoteApplyResult>)
                : new RemoteApplyResult(
                    batch.NextCursor,
                    appliedCount,
                    batch.Events.Count - appliedCount,
                    expectedRevision + 1 + RemoteReceiptRevisionOffset);
            return TransformRemoteReceipt is null ? receipt : TransformRemoteReceipt(receipt);
        }
    }
}
