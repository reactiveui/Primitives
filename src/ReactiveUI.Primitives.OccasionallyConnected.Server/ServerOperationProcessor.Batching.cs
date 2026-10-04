// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Processes authorized client operations through an internal server commit journal.</summary>
/// <content>Prepares bounded operation groups outside native transactions.</content>
internal sealed partial class ServerOperationProcessor
{
    /// <summary>Selects one operation proof while preserving projected state.</summary>
    /// <param name="snapshot">The projected snapshot.</param>
    /// <param name="key">The operation key.</param>
    /// <returns>The operation-specific snapshot.</returns>
    private static ServerCommitSnapshot SelectOperationSnapshot(ServerCommitSnapshot snapshot, ServerOperationKey key)
    {
        var entries = new List<ServerLedgerEntry>();
        foreach (var entry in snapshot.Entries)
        {
            if (entry.OperationKey == key)
            {
                entries.Add(entry);
            }
        }

        return new(CopyProjectionOptions(snapshot) with { Entries = entries });
    }

    /// <summary>Captures the bounded keys for one preparation group.</summary>
    /// <param name="operations">The captured operations.</param>
    /// <param name="client">The trusted client.</param>
    /// <param name="offset">The group offset.</param>
    /// <param name="count">The group count.</param>
    /// <returns>The captured keys.</returns>
    private static ServerOperationKey[] CaptureGroupKeys(SyncOperation[] operations, ClientIdentity client, int offset, int count)
    {
        var keys = new ServerOperationKey[count];
        for (var index = 0; index < count; index++)
        {
            keys[index] = new(client.ClientId, operations[offset + index].OperationId);
        }

        return keys;
    }

    /// <summary>Projects the same state and sequence changes as one successful journal operation.</summary>
    /// <param name="snapshot">The preceding snapshot.</param>
    /// <param name="plan">The independently fenced operation plan.</param>
    /// <returns>The next speculative snapshot.</returns>
    private static ServerCommitSnapshot ProjectSnapshot(ServerCommitSnapshot snapshot, ServerCommitPlan plan)
    {
        var events = plan.Entries[0].Events;
        return new(CopyProjectionOptions(snapshot) with
        {
            Revision = checked(snapshot.Revision + 1),
            State = plan.NewState ?? snapshot.State,
            LastWriteStamp = plan.NewWriteStamp ?? snapshot.LastWriteStamp,
            LastCursor = events.Count == 0 ? snapshot.LastCursor : events[events.Count - 1].ServerCursor,
            LastEventSequence = checked(snapshot.LastEventSequence + events.Count),
            LastGroupSequence = checked(snapshot.LastGroupSequence + 1),
        });
    }

    /// <summary>Copies snapshot fields without adding uncommitted replay proofs.</summary>
    /// <param name="source">The source snapshot.</param>
    /// <returns>The owned projection fields.</returns>
    private static ServerCommitSnapshotOptions CopyProjectionOptions(ServerCommitSnapshot source) =>
        new()
        {
            StreamKey = source.StreamKey,
            Revision = source.Revision,
            State = source.State,
            LastWriteStamp = source.LastWriteStamp,
            LastCursor = source.LastCursor,
            LastEventSequence = source.LastEventSequence,
            LastGroupSequence = source.LastGroupSequence,
            Entries = source.Entries,
        };

    /// <summary>Processes bounded groups with one durable result and fence per operation.</summary>
    /// <param name="journal">The durable journal.</param>
    /// <param name="client">The trusted client.</param>
    /// <param name="operations">The captured bounded batch.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The ordered operation receipts.</returns>
    private async ValueTask<ServerOperationReceipt[]> ProcessSqliteBatchAsync(
        SqliteServerCommitJournal journal,
        ClientIdentity client,
        SyncOperation[] operations,
        CancellationToken cancellationToken)
    {
        var receipts = new ServerOperationReceipt[operations.Length];
        var groupSize = journal.MaximumBatchReadOperations;
        for (var offset = 0; offset < operations.Length; offset += groupSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = Math.Min(groupSize, operations.Length - offset);
            await ProcessPreparedGroupAsync(journal, client, operations, receipts, offset, count, cancellationToken).ConfigureAwait(false);
        }

        return receipts;
    }

    /// <summary>Prepares a bounded group without holding a SQLite transaction across callbacks.</summary>
    /// <param name="journal">The durable journal.</param>
    /// <param name="client">The trusted client.</param>
    /// <param name="operations">The captured operations.</param>
    /// <param name="receipts">The ordered receipts.</param>
    /// <param name="offset">The first group operation.</param>
    /// <param name="count">The group operation count.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The group processing operation.</returns>
    private async ValueTask ProcessPreparedGroupAsync(
        SqliteServerCommitJournal journal,
        ClientIdentity client,
        SyncOperation[] operations,
        ServerOperationReceipt[] receipts,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        var scope = Authorize(client, operations[offset]);
        var streamKey = new ServerStreamKey(scope.TenantId, operations[offset].StreamId);
        var keys = CaptureGroupKeys(operations, client, offset, count);
        var snapshot = await journal.ExecuteAsync(() => journal.Read(streamKey, keys), cancellationToken).ConfigureAwait(false);
        var pending = new List<PreparedBatchOperation>(count);
        try
        {
            for (var index = offset; index < offset + count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var operation = operations[index];
                var operationScope = Authorize(client, operation);
                if (new ServerStreamKey(operationScope.TenantId, operation.StreamId) != streamKey || !CanProjectGroup(snapshot))
                {
                    await FlushPreparedGroupAsync(journal, pending, receipts, client, cancellationToken).ConfigureAwait(false);
                    receipts[index] = await ProcessOperationAsync(client, operation, cancellationToken).ConfigureAwait(false);
                    snapshot = await journal.ExecuteAsync(() => journal.Read(streamKey, keys), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var fingerprint = new ServerCommitFingerprint(CanonicalOperationFingerprint.Compute(
                    operationScope.TenantId,
                    operationScope.ClientId,
                    operation,
                    _options.MaximumCanonicalOperationBytes));
                var current = SelectOperationSnapshot(snapshot, keys[index - offset]);
                var replay = TryReplay(operation.OperationId, current, fingerprint);
                if (replay is not null)
                {
                    receipts[index] = replay;
                    continue;
                }

                var plan = await PrepareBatchOperationAsync(
                    client,
                    operation,
                    operationScope,
                    current,
                    keys[index - offset],
                    fingerprint,
                    cancellationToken).ConfigureAwait(false);
                _ = journal.ValidatePreparation(plan);
                pending.Add(new(index, operation, fingerprint, plan));
                snapshot = ProjectSnapshot(snapshot, plan);
            }
        }
        finally
        {
            // A later preparation failure must not erase the already prepared prefix.
            await FlushPreparedGroupAsync(journal, pending, receipts, client, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Prepares one operation from projected sequential state.</summary>
    /// <param name="client">The trusted client.</param>
    /// <param name="operation">The captured operation.</param>
    /// <param name="scope">The authorized scope.</param>
    /// <param name="snapshot">The projected snapshot.</param>
    /// <param name="key">The operation key.</param>
    /// <param name="fingerprint">The canonical fingerprint.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The independently fenced plan.</returns>
    private async ValueTask<ServerCommitPlan> PrepareBatchOperationAsync(
        ClientIdentity client,
        SyncOperation operation,
        ServerOperationScope scope,
        ServerCommitSnapshot snapshot,
        ServerOperationKey key,
        ServerCommitFingerprint fingerprint,
        CancellationToken cancellationToken)
    {
        var context = new ServerOperationContext(
            client,
            operation,
            scope,
            snapshot.StreamKey,
            key,
            snapshot,
            CreateCandidateWrite(scope, operation, snapshot));
        var preparation = await _handler.PrepareAsync(context, cancellationToken).ConfigureAwait(false);
        ArgumentExceptionHelper.ThrowIfNull(preparation);
        cancellationToken.ThrowIfCancellationRequested();
        ValidatePreparation(context, preparation);
        var entry = new ServerLedgerEntry(key, fingerprint, preparation.Result, preparation.Conflicts, StampEvents(context, preparation.Events));
        var stamp = preparation.NewState is null ? (ServerWriteStamp?)null : context.CandidateWrite;
        return new(snapshot.StreamKey, snapshot.Revision, preparation.NewState, stamp, [entry]);
    }

    /// <summary>Flushes prepared plans once and re-prepares stale plans within the original retry budget.</summary>
    /// <param name="journal">The durable journal.</param>
    /// <param name="pending">The bounded prepared prefix.</param>
    /// <param name="receipts">The ordered receipts.</param>
    /// <param name="client">The trusted client.</param>
    /// <param name="cancellationToken">The token used to stop stale re-preparation.</param>
    /// <returns>The durable flush operation.</returns>
    /// <exception cref="InvalidOperationException">The journal returns an unknown status.</exception>
    private async ValueTask FlushPreparedGroupAsync(
        SqliteServerCommitJournal journal,
        List<PreparedBatchOperation> pending,
        ServerOperationReceipt[] receipts,
        ClientIdentity client,
        CancellationToken cancellationToken)
    {
        if (pending.Count == 0)
        {
            return;
        }

        var captured = pending.ToArray();
        pending.Clear();
        var plans = new ServerCommitPlan[captured.Length];
        for (var index = 0; index < captured.Length; index++)
        {
            plans[index] = captured[index].Plan;
        }

        var results = await journal.ExecuteAsync(() => journal.TryCommitBatch(plans), CancellationToken.None).ConfigureAwait(false);
        for (var index = 0; index < captured.Length; index++)
        {
            var operation = captured[index];
            var result = results[index];
            receipts[operation.Index] = result.Status switch
            {
                ServerCommitStatus.Committed => ReplayCommitted(operation.Operation.OperationId, result.Snapshot, operation.Fingerprint),
                ServerCommitStatus.StaleRevision => await ProcessOperationAsync(
                    client,
                    operation.Operation,
                    cancellationToken,
                    startingAttempt: 1).ConfigureAwait(false),
                ServerCommitStatus.IntentMismatch => Rejected(operation.Operation.OperationId, IntentMismatchReason),
                ServerCommitStatus.CapacityExceeded => Retryable(operation.Operation.OperationId, CapacityExceededReason),
                ServerCommitStatus.RevisionOverflow => Rejected(operation.Operation.OperationId, RevisionOverflowReason),
                ServerCommitStatus.EventSequenceOverflow => Rejected(operation.Operation.OperationId, EventSequenceOverflowReason),
                _ => throw new InvalidOperationException("The server commit journal returned an unknown status."),
            };
        }
    }

    /// <summary>Returns whether bounded speculative sequence arithmetic is safe.</summary>
    /// <param name="snapshot">The current projected snapshot.</param>
    /// <returns>Whether the next operation can be projected safely.</returns>
    private bool CanProjectGroup(ServerCommitSnapshot snapshot) =>
        snapshot.Revision < long.MaxValue
        && snapshot.LastGroupSequence < long.MaxValue
        && snapshot.LastEventSequence <= long.MaxValue - _options.MaximumPreparedEvents;

    /// <summary>Retains one bounded prepared plan and its response location.</summary>
    /// <param name="Index">The operation's batch index.</param>
    /// <param name="Operation">The captured operation.</param>
    /// <param name="Fingerprint">The canonical operation fingerprint.</param>
    /// <param name="Plan">The prepared independently fenced plan.</param>
    private sealed record PreparedBatchOperation(int Index, SyncOperation Operation, ServerCommitFingerprint Fingerprint, ServerCommitPlan Plan);
}
