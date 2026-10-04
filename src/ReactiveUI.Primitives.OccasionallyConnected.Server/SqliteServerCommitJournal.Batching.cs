// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Amortizes durable synchronization across bounded prepared operations.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>The finite prepared-operation transaction bound.</summary>
    internal const int MaximumPreparedBatchOperations = 8;

    /// <summary>The number of successful physical write transactions completed by this instance.</summary>
    private long _committedTransactionCount;

    /// <summary>Gets the physical commit count for transaction-amplification tests.</summary>
    internal long CommittedTransactionCount => Interlocked.Read(ref _committedTransactionCount);

    /// <summary>Gets the maximum read and preparation group supported by the journal's capture bound.</summary>
    internal int MaximumBatchReadOperations => Math.Min(MaximumPreparedBatchOperations, _options.MaximumOperationCaptureCount);

    /// <summary>Validates a prepared operation against this journal's limits before it joins a pending prefix.</summary>
    /// <param name="plan">The prepared operation plan.</param>
    /// <returns>The captured validated plan.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ServerCommitValidationResult ValidatePreparation(ServerCommitPlan plan)
    {
        ThrowIfDisposed();
        return ServerCommitJournalGuard.ValidatePlan(plan, _options);
    }

    /// <summary>Commits bounded prepared plans with independent fences, results and capacity checks in one transaction.</summary>
    /// <param name="plans">The bounded prepared operation plans.</param>
    /// <returns>The ordered results, published only after the transaction is durable.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The group is empty or exceeds the preparation bound.</exception>
    internal ServerCommitResult[] TryCommitBatch(IReadOnlyList<ServerCommitPlan> plans)
    {
        ThrowIfDisposed();
        ArgumentExceptionHelper.ThrowIfNull(plans);
        if (plans.Count is <= 0 or > MaximumPreparedBatchOperations)
        {
            throw new ArgumentOutOfRangeException(nameof(plans));
        }

        var commits = new ServerCommitValidationResult[plans.Count];
        for (var index = 0; index < plans.Count; index++)
        {
            commits[index] = ValidatePreparation(plans[index]);
        }

        using var connectionLease = AcquireConnection();
        var connection = _connection;
        using var transaction = connection.BeginTransaction();
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var results = new ServerCommitResult[commits.Length];
        var changed = false;
        var canApplyNext = true;
        for (var index = 0; index < commits.Length; index++)
        {
            results[index] = canApplyNext
                ? TryCommitInTransaction(connection, transaction, commits[index], _options.TimeProvider.GetUtcNow())
                : new(
                    ServerCommitStatus.StaleRevision,
                    ServerCommitJournalOperations.CreateSnapshot(
                        commits[index].StreamKey,
                        ReadStreamRecord(connection, transaction, commits[index].StreamKey, commits[index].OperationKeys),
                        commits[index].OperationKeys));
            canApplyNext &= results[index].Status == ServerCommitStatus.Committed;
            changed |= results[index].Status == ServerCommitStatus.Committed;
        }

        if (changed)
        {
            _faultPoint.Reached(SqliteServerCommitCheckpoint.TryCommitBeforeCommit);
        }

        transaction.Commit();
        if (changed)
        {
            _ = Interlocked.Increment(ref _committedTransactionCount);
        }

        if (changed)
        {
            _faultPoint.Reached(SqliteServerCommitCheckpoint.TryCommitAfterCommit);
        }

        return results;
    }
}
