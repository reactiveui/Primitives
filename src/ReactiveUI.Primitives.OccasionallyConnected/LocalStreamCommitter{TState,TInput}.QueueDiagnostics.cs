// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Queue diagnostic helpers for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>Creates a bounded queue diagnostic snapshot from recovered store state.</summary>
    /// <param name="recovered">The recovered stream state.</param>
    /// <returns>The recovered queue snapshot.</returns>
    private static QueueDiagnosticSnapshot CreateRecoveredQueueSnapshot(RecoveredStream recovered)
    {
        var pendingBytes = 0L;
        for (var index = 0; index < recovered.PendingOperations.Count; index++)
        {
            pendingBytes = checked(pendingBytes + OperationRetentionSizing.GetOperationRetainedBytes(recovered.PendingOperations[index]));
        }

        return new(recovered.PendingOperations.Count, pendingBytes, recovered.Snapshot?.Revision ?? 0);
    }

    /// <summary>Creates bounded upload wake metadata from recovered pending work.</summary>
    /// <param name="recovered">The recovered stream state.</param>
    /// <returns>The recovered upload head metadata, or <see langword="null"/> when no work is pending.</returns>
    private static RecoveredUploadHead? CreateRecoveredUploadHead(RecoveredStream recovered)
    {
        if (recovered.PendingOperations.Count == 0)
        {
            return null;
        }

        var priority = recovered.PendingOperations[0].Policy.Priority;
        for (var index = 1; index < recovered.PendingOperations.Count; index++)
        {
            priority = Math.Max(priority, recovered.PendingOperations[index].Policy.Priority);
        }

        return new(priority, recovered.PendingUploadNotBeforeUtc);
    }

    /// <summary>Creates the bounded queue diagnostic snapshot after a new local commit.</summary>
    /// <param name="current">The current queue aggregate.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="revision">The monotonic diagnostic revision that owns the queue change.</param>
    /// <returns>The updated queue snapshot.</returns>
    private static QueueDiagnosticSnapshot CreateCommittedQueueSnapshot(
        QueueDiagnosticSnapshot current,
        SyncOperation operation,
        long revision) =>
        new(
            checked(current.PendingOperations + 1),
            checked(current.PendingBytes + OperationRetentionSizing.GetOperationRetainedBytes(operation)),
            revision);

    /// <summary>Creates the bounded queue diagnostic snapshot after terminal upload decisions.</summary>
    /// <param name="current">The current queue aggregate.</param>
    /// <param name="batch">The uploaded batch.</param>
    /// <param name="result">The durable upload result.</param>
    /// <param name="revision">The monotonic diagnostic revision that owns the queue change.</param>
    /// <returns>The updated queue snapshot.</returns>
    /// <exception cref="InvalidOperationException">The terminal result would underflow the queue aggregate.</exception>
    private static QueueDiagnosticSnapshot CreateTerminalQueueSnapshot(
        QueueDiagnosticSnapshot current,
        SyncBatch batch,
        RemoteSyncResult result,
        long revision)
    {
        var releasedOperations = 0L;
        var releasedBytes = 0L;
        foreach (var operation in batch.Operations)
        {
            if (!TryFindResult(operation.OperationId, result, out var operationResult) || operationResult is null)
            {
                continue;
            }

            if (!IsTerminalUploadResult(operationResult.Kind))
            {
                continue;
            }

            releasedOperations++;
            releasedBytes = checked(releasedBytes + OperationRetentionSizing.GetOperationRetainedBytes(operation));
        }

        if (releasedOperations == 0)
        {
            return current;
        }

        var pendingOperations = checked(current.PendingOperations - releasedOperations);
        var pendingBytes = checked(current.PendingBytes - releasedBytes);
        if (pendingOperations >= 0 && pendingBytes >= 0)
        {
            return new(pendingOperations, pendingBytes, revision);
        }

        throw new InvalidOperationException("Terminal upload result would underflow the queue diagnostic aggregate.");
    }

    /// <summary>Creates the bounded queue diagnostic snapshot after one dead-letter transition.</summary>
    /// <param name="current">The current queue aggregate.</param>
    /// <param name="operation">The dead-lettered operation.</param>
    /// <param name="revision">The monotonic diagnostic revision that owns the queue change.</param>
    /// <returns>The updated queue snapshot.</returns>
    /// <exception cref="InvalidOperationException">The transition would underflow the queue aggregate.</exception>
    private static QueueDiagnosticSnapshot CreateDeadLetterQueueSnapshot(
        QueueDiagnosticSnapshot current,
        SyncOperation operation,
        long revision)
    {
        var pendingOperations = checked(current.PendingOperations - 1);
        var pendingBytes = checked(current.PendingBytes - OperationRetentionSizing.GetOperationRetainedBytes(operation));
        if (pendingOperations >= 0 && pendingBytes >= 0)
        {
            return new(pendingOperations, pendingBytes, revision);
        }

        throw new InvalidOperationException("Dead-letter transition would underflow the queue diagnostic aggregate.");
    }

    /// <summary>Finds a replay operation by identity.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <returns>The recovered operation.</returns>
    /// <exception cref="InvalidOperationException">The operation is missing from replay state.</exception>
    private static SyncOperation FindReplayOperation(RecoveredStream recovered, OperationId operationId)
    {
        for (var index = 0; index < recovered.ReplayOperations.Count; index++)
        {
            var operation = recovered.ReplayOperations[index];
            if (operation.OperationId == operationId)
            {
                return operation;
            }
        }

        throw new InvalidOperationException("The dead-letter operation was not present in recovered replay state.");
    }

    /// <summary>Finds an operation result by identity without depending on result ordering.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="result">The upload result.</param>
    /// <param name="operationResult">The matching result.</param>
    /// <returns>Whether the result contains the operation.</returns>
    private static bool TryFindResult(
        OperationId operationId,
        RemoteSyncResult result,
        out OperationSyncResult? operationResult)
    {
        foreach (var candidate in result.Operations)
        {
            if (candidate.OperationId != operationId)
            {
                continue;
            }

            operationResult = candidate;
            return true;
        }

        operationResult = null;
        return false;
    }

    /// <summary>Checks whether a remote upload result consumes pending queue work.</summary>
    /// <param name="kind">The result kind.</param>
    /// <returns>Whether the result is terminal for queue accounting.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsTerminalUploadResult(OperationResultKind kind) =>
        kind is OperationResultKind.Accepted or OperationResultKind.Rejected;

    /// <summary>Stores a recovered queue snapshot and seeds the diagnostic revision.</summary>
    /// <param name="snapshot">The recovered snapshot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetRecoveredQueueSnapshot(QueueDiagnosticSnapshot snapshot) => CommitQueueDiagnosticSnapshot(snapshot);

    /// <summary>Calculates the next monotonic queue diagnostic revision without advancing it.</summary>
    /// <returns>The next revision.</returns>
    private long PeekNextQueueDiagnosticRevision() => checked(_queueDiagnosticRevision + 1);

    /// <summary>Stores a queue snapshot after its durable transition succeeds.</summary>
    /// <param name="snapshot">The committed queue snapshot.</param>
    private void CommitQueueDiagnosticSnapshot(QueueDiagnosticSnapshot snapshot)
    {
        RecoveredQueueSnapshot = snapshot;
        _queueDiagnosticRevision = snapshot.Revision;
    }
}
