// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if NET8_0_OR_GREATER
using System.Security.Cryptography;
#endif
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>Applies a snapshot recovery transaction to the initialized store partition.</summary>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    /// <exception cref="ArgumentException">The recovery mutation is invalid.</exception>
    /// <exception cref="InvalidOperationException">The store has not been initialized or a durable fence rejects recovery.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    internal LocalSnapshotRecoveryResult ApplySnapshotRecovery(
        LocalSnapshotRecoveryMutation mutation,
        CancellationToken cancellationToken)
    {
        SqliteLocalCommitValidation.ValidateSnapshotRecoveryMutationShape(mutation);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            return ApplySnapshotRecoveryLocked(mutation, nowUtc, cancellationToken);
        }
    }

    /// <summary>Compares snapshot recovery cursors using ordinal UTF-16 identity without short-circuiting matching content.</summary>
    /// <param name="left">The left cursor.</param>
    /// <param name="right">The right cursor.</param>
    /// <returns>Whether the cursors have the same ordinal value.</returns>
    private static bool SnapshotRecoveryCursorOrdinalEquals(string? left, string? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        var leftBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(left.AsSpan());
        var rightBytes = System.Runtime.InteropServices.MemoryMarshal.AsBytes(right.AsSpan());
        return SnapshotRecoveryCursorBytesEqual(leftBytes, rightBytes);
    }

    /// <summary>Compares raw cursor bytes using the best fixed-time primitive for the target framework.</summary>
    /// <param name="left">The left cursor bytes.</param>
    /// <param name="right">The right cursor bytes.</param>
    /// <returns>Whether the cursor bytes are equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool SnapshotRecoveryCursorBytesEqual(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
#if NET8_0_OR_GREATER
        return CryptographicOperations.FixedTimeEquals(left, right);
#else
        return SnapshotRecoveryCursorBytesEqualFallback(left, right);
#endif
    }

#if !NET8_0_OR_GREATER
    /// <summary>Compares same-length cursor byte sequences with work that depends on length, not byte values.</summary>
    /// <param name="left">The left cursor bytes.</param>
    /// <param name="right">The right cursor bytes.</param>
    /// <returns>Whether the cursor bytes are equal.</returns>
    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    private static bool SnapshotRecoveryCursorBytesEqualFallback(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var length = left.Length;
        var difference = 0;
        for (var index = 0; index < length; index++)
        {
            difference |= left[index] - right[index];
        }

        return difference == 0;
    }
#endif

    /// <summary>Validates stream-scoped snapshot recovery fences.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <exception cref="InvalidOperationException">A durable fence rejects recovery.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static void PrepareSnapshotRecoveryStream(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryMutation mutation)
    {
        SqliteLocalCommitSql.ThrowIfStreamQuarantined(connection, transaction, storeIdentity, mutation.StreamId);
        var subscriptionId = SqliteLocalCommitSql.SelectSubscriptionId(connection, transaction, storeIdentity, mutation.StreamId);
        ValidateSnapshotRecoverySubscription(subscriptionId, mutation);
        SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, storeIdentity, mutation.StreamId, subscriptionId);
        var stream = SqliteLocalCommitSql.ReadStreamState(connection, transaction, storeIdentity, mutation.StreamId);
        ValidateSnapshotRecoveryFences(connection, transaction, storeIdentity, stream, mutation);
    }

    /// <summary>Writes the recovered snapshot and cursor state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="counts">The operation disposition counts.</param>
    /// <returns>The recovery result.</returns>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static LocalSnapshotRecoveryResult PersistSnapshotRecoverySnapshot(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryMutation mutation,
        DateTimeOffset nowUtc,
        SnapshotRecoveryOperationCounts counts)
    {
        var nextRevision = checked(mutation.ExpectedRevision + 1);
        var snapshot = new LocalSnapshot(
            mutation.StreamId,
            mutation.SnapshotFormatVersion,
            mutation.Checkpoint.FrontierCursor,
            mutation.OptimisticState,
            nextRevision,
            nowUtc) { AuthoritativeState = mutation.Checkpoint.ClientState };
        SqliteLocalCommitSql.UpdateServerCursor(
            connection,
            transaction,
            storeIdentity,
            mutation.StreamId,
            mutation.ExpectedPreviousCursor,
            mutation.Checkpoint.FrontierCursor);
        SqliteLocalCommitSql.UpsertSnapshot(
            connection,
            transaction,
            storeIdentity,
            new(mutation.StreamId, mutation.OptimisticState, mutation.SnapshotFormatVersion, mutation.ExpectedRevision) { AuthoritativeState = mutation.Checkpoint.ClientState },
            nextRevision,
            mutation.Checkpoint.FrontierCursor,
            nowUtc);
        return new()
        {
            Snapshot = snapshot,
            IncludedOperationCount = counts.IncludedOperationCount,
            TerminalOperationCount = counts.TerminalOperationCount,
            PreservedPendingOperationCount = counts.PreservedPendingOperationCount,
        };
    }

    /// <summary>Projects shape-validated recovery dispositions into typed facts for the transaction.</summary>
    /// <param name="dispositions">The source dispositions.</param>
    /// <returns>The projected disposition facts.</returns>
    private static SnapshotRecoveryDisposition[] CaptureSnapshotRecoveryDispositions(IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var captured = new SnapshotRecoveryDisposition[dispositions.Count];
        for (var index = 0; index < dispositions.Count; index++)
        {
            captured[index] = CreateSnapshotRecoveryDisposition(dispositions[index]);
        }

        return captured;
    }

    /// <summary>Projects typed facts for one previously shape-validated disposition.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The typed disposition facts.</returns>
    private static SnapshotRecoveryDisposition CreateSnapshotRecoveryDisposition(SnapshotOperationDisposition disposition) =>
        new(
            disposition.OperationId,
            disposition.Kind,
            disposition.Result?.Kind ?? OperationResultKind.Retryable,
            disposition.Result?.ReasonCode);

    /// <summary>Applies operation dispositions that passed the recovery fences.</summary>
    /// <param name="context">The operation application context.</param>
    /// <param name="pending">The pending operations.</param>
    /// <param name="replayOnly">The replay-only operations.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The operation disposition counts.</returns>
    /// <exception cref="OperationCanceledException">The operation is canceled while applying dispositions.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static SnapshotRecoveryOperationCounts ApplySnapshotRecoveryOperations(
        in SnapshotRecoveryOperationContext context,
        List<SqliteSnapshotRecoveryPendingOperation> pending,
        List<OperationId> replayOnly,
        SnapshotRecoveryDisposition[] dispositions)
    {
        var included = 0;
        var terminal = 0;
        var preserved = 0;
        for (var index = 0; index < dispositions.Length; index++)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            var replayOnlyDisposition = index >= pending.Count;
            var operationId = replayOnlyDisposition ? replayOnly[index - pending.Count] : pending[index].OperationId;
            var disposition = dispositions[index];
            if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted)
            {
                ApplyIncludedSnapshotRecoveryOperation(context, operationId, disposition, replayOnlyDisposition);
                if (replayOnlyDisposition || disposition.ResultKind == OperationResultKind.Accepted)
                {
                    included++;
                }
                else
                {
                    preserved++;
                }

                continue;
            }

            if (disposition.Kind == SnapshotOperationDispositionKind.TerminalRejected)
            {
                ApplyRejectedSnapshotRecoveryOperation(context, operationId, disposition);
                terminal++;
                continue;
            }

            ReleaseExpiredSnapshotRecoveryLease(context, operationId);
            preserved++;
        }

        return new(included, terminal, preserved);
    }

    /// <summary>Applies one included operation disposition.</summary>
    /// <param name="context">The operation application context.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="disposition">The disposition.</param>
    /// <param name="replayOnly">Whether the disposition targets replay-only receive inclusion.</param>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static void ApplyIncludedSnapshotRecoveryOperation(
        in SnapshotRecoveryOperationContext context,
        OperationId operationId,
        in SnapshotRecoveryDisposition disposition,
        bool replayOnly)
    {
        if (!replayOnly)
        {
            SqliteLocalCommitSql.ApplySnapshotRecoveryOperationState(
                context.Connection,
                context.Transaction,
                context.StoreIdentity,
                operationId,
                GetSnapshotRecoveryResultState(disposition.ResultKind),
                context.NowUtc,
                disposition.ReasonCode);
            SqliteLocalCommitSql.ReleaseSnapshotRecoveryLeaseOperation(context.Connection, context.Transaction, context.StoreIdentity, operationId);
        }

        SqliteLocalCommitSql.InsertReceiveInclusion(context.Connection, context.Transaction, context.StoreIdentity, operationId);
    }

    /// <summary>Applies one terminal rejected operation disposition.</summary>
    /// <param name="context">The operation application context.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="disposition">The disposition.</param>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static void ApplyRejectedSnapshotRecoveryOperation(
        in SnapshotRecoveryOperationContext context,
        OperationId operationId,
        in SnapshotRecoveryDisposition disposition)
    {
        SqliteLocalCommitSql.ApplySnapshotRecoveryOperationState(
            context.Connection,
            context.Transaction,
            context.StoreIdentity,
            operationId,
            SyncOperationState.Rejected,
            context.NowUtc,
            disposition.ReasonCode);
        SqliteLocalCommitSql.ReleaseSnapshotRecoveryLeaseOperation(context.Connection, context.Transaction, context.StoreIdentity, operationId);
    }

    /// <summary>Releases an expired lease for an unknown disposition.</summary>
    /// <param name="context">The operation application context.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static void ReleaseExpiredSnapshotRecoveryLease(in SnapshotRecoveryOperationContext context, OperationId operationId)
    {
        if (context.ExpiredLeases.Contains(operationId))
        {
            SqliteLocalCommitSql.ReleaseSnapshotRecoveryLeaseOperation(context.Connection, context.Transaction, context.StoreIdentity, operationId);
        }
    }

    /// <summary>Maps result proof to durable outbox state.</summary>
    /// <param name="kind">The result kind.</param>
    /// <returns>The operation state.</returns>
    private static SyncOperationState GetSnapshotRecoveryResultState(OperationResultKind kind) =>
        kind == OperationResultKind.Accepted ? SyncOperationState.Synchronized : SyncOperationState.Conflict;

    /// <summary>Validates exact pending then replay-only operation membership and order.</summary>
    /// <param name="pending">The pending operations.</param>
    /// <param name="replayOnly">The replay-only operations.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <exception cref="ArgumentException">The dispositions do not exactly match recovery operations.</exception>
    private static void ValidateSnapshotRecoveryDispositions(
        List<SqliteSnapshotRecoveryPendingOperation> pending,
        List<OperationId> replayOnly,
        SnapshotRecoveryDisposition[] dispositions)
    {
        if (checked(pending.Count + replayOnly.Count) != dispositions.Length)
        {
            throw new ArgumentException("Snapshot recovery dispositions must exactly match local recovery operations.", nameof(dispositions));
        }

        HashSet<OperationId> seen = [];
        for (var index = 0; index < dispositions.Length; index++)
        {
            var disposition = dispositions[index];
            if (!seen.Add(disposition.OperationId))
            {
                throw new ArgumentException("Snapshot recovery dispositions must not contain duplicate operations.", nameof(dispositions));
            }

            if (index < pending.Count)
            {
                if (pending[index].OperationId == disposition.OperationId)
                {
                    continue;
                }

                throw new ArgumentException("Snapshot recovery dispositions must preserve pending operation order.", nameof(dispositions));
            }

            if (replayOnly[index - pending.Count] != disposition.OperationId)
            {
                throw new ArgumentException("Snapshot recovery dispositions must preserve replay operation order after pending operations.", nameof(dispositions));
            }

            if (disposition.Kind == SnapshotOperationDispositionKind.IncludedAccepted
                && disposition.ResultKind == OperationResultKind.Accepted)
            {
                continue;
            }

            throw new ArgumentException("Replay-only snapshot recovery dispositions must carry accepted inclusion proof.", nameof(dispositions));
        }
    }

    /// <summary>Validates durable stream fences.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="stream">The stream state.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <exception cref="InvalidOperationException">A durable fence rejects recovery.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static void ValidateSnapshotRecoveryFences(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        SqliteLocalStreamState stream,
        LocalSnapshotRecoveryMutation mutation)
    {
        if (!SnapshotRecoveryCursorOrdinalEquals(stream.ServerCursor, mutation.ExpectedPreviousCursor))
        {
            throw new InvalidOperationException("Snapshot recovery cursor fence does not match the local stream.");
        }

        var currentRevision = SqliteLocalCommitSql.ReadSnapshotRevision(connection, transaction, storeIdentity, mutation.StreamId);
        if (currentRevision == mutation.ExpectedRevision)
        {
            return;
        }

        throw new InvalidOperationException("Snapshot recovery revision fence does not match the local stream.");
    }

    /// <summary>Validates the stream subscription fence.</summary>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <exception cref="InvalidOperationException">The subscription fence rejects recovery.</exception>
    private static void ValidateSnapshotRecoverySubscription(SubscriptionId subscriptionId, LocalSnapshotRecoveryMutation mutation)
    {
        if (subscriptionId == mutation.SubscriptionId)
        {
            return;
        }

        throw new InvalidOperationException("Snapshot recovery subscription does not match the local stream.");
    }

    /// <summary>Applies snapshot recovery changes inside the active SQLite transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovery result.</returns>
    /// <exception cref="ArgumentException">The recovery dispositions do not exactly match pending operations.</exception>
    /// <exception cref="InvalidOperationException">A durable fence rejects recovery.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled while applying operation dispositions.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private static LocalSnapshotRecoveryResult ApplySnapshotRecoveryTransaction(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalSnapshotRecoveryMutation mutation,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        PrepareSnapshotRecoveryStream(connection, transaction, storeIdentity, mutation);
        var dispositions = CaptureSnapshotRecoveryDispositions(mutation.OperationDispositions);
        var pending = SqliteLocalCommitSql.ReadSnapshotRecoveryPendingOperations(
            connection,
            transaction,
            storeIdentity,
            mutation.StreamId,
            checked(dispositions.Length + 1),
            cancellationToken);
        var replayOnlyLimit = pending.Count > dispositions.Length ? 1 : checked(dispositions.Length - pending.Count + 1);
        var replayOnly = SqliteLocalCommitSql.ReadSnapshotRecoveryReplayOnlyOperationIds(
            connection,
            transaction,
            storeIdentity,
            mutation.StreamId,
            replayOnlyLimit,
            cancellationToken);
        ValidateSnapshotRecoveryDispositions(pending, replayOnly, dispositions);
        var expiredLeases = SqliteLocalCommitSql.ReadSnapshotRecoveryExpiredLeaseOperationIds(
            connection,
            transaction,
            storeIdentity,
            pending,
            nowUtc);
        var counts = ApplySnapshotRecoveryOperations(
            new(connection, transaction, storeIdentity, expiredLeases, nowUtc, cancellationToken),
            pending,
            replayOnly,
            dispositions);
        return PersistSnapshotRecoverySnapshot(connection, transaction, storeIdentity, mutation, nowUtc, counts);
    }

    /// <summary>Applies snapshot recovery while holding the store gate.</summary>
    /// <param name="mutation">The recovery mutation.</param>
    /// <param name="nowUtc">The current UTC timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    /// <exception cref="InvalidOperationException">The store has not been initialized or a durable fence rejects recovery.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled before the transaction commits.</exception>
    /// <exception cref="SqliteException">SQLite rejects the operation.</exception>
    private LocalSnapshotRecoveryResult ApplySnapshotRecoveryLocked(
        LocalSnapshotRecoveryMutation mutation,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var storeIdentity = GetInitializedStoreIdentity();
        cancellationToken.ThrowIfCancellationRequested();
        using var connection = SqliteLocalCommitConnection.OpenConnection(_databasePath);
        SqliteLocalCommitConnection.ConfigureLockPolling(connection);
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        using var transaction = SqliteLocalCommitConnection.BeginWriteTransaction(connection, cancellationToken);
        var result = ApplySnapshotRecoveryTransaction(connection, transaction, storeIdentity, mutation, nowUtc, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return result;
    }

    /// <summary>Groups state used while applying snapshot recovery operation dispositions.</summary>
    /// <param name="Connection">The connection.</param>
    /// <param name="Transaction">The transaction.</param>
    /// <param name="StoreIdentity">The store identity.</param>
    /// <param name="ExpiredLeases">The expired leased operation identifiers.</param>
    /// <param name="NowUtc">The current UTC timestamp.</param>
    /// <param name="CancellationToken">The cancellation token.</param>
    private readonly record struct SnapshotRecoveryOperationContext(
        SqliteConnection Connection,
        SqliteTransaction Transaction,
        string StoreIdentity,
        HashSet<OperationId> ExpiredLeases,
        DateTimeOffset NowUtc,
        CancellationToken CancellationToken);

    /// <summary>Describes one validated recovery disposition without nullable proof lookups.</summary>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="Kind">The disposition kind.</param>
    /// <param name="ResultKind">The proven result kind, or a placeholder for unknown dispositions.</param>
    /// <param name="ReasonCode">The proven result reason code.</param>
    private readonly record struct SnapshotRecoveryDisposition(
        OperationId OperationId,
        SnapshotOperationDispositionKind Kind,
        OperationResultKind ResultKind,
        string? ReasonCode);

    /// <summary>Counts applied recovery operation dispositions.</summary>
    /// <param name="IncludedOperationCount">The included operation count.</param>
    /// <param name="TerminalOperationCount">The terminal operation count.</param>
    /// <param name="PreservedPendingOperationCount">The preserved pending operation count.</param>
    private readonly record struct SnapshotRecoveryOperationCounts(
        int IncludedOperationCount,
        int TerminalOperationCount,
        int PreservedPendingOperationCount);
}
