// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Snapshot recovery helpers for the SQLite local store adapter.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Creates a local recovery mutation bound to current SQLite fixtures.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="expectedCursor">The expected cursor.</param>
    /// <param name="dispositions">The exact operation dispositions.</param>
    /// <returns>The local recovery mutation.</returns>
    private static LocalSnapshotRecoveryMutation CreateSnapshotRecoveryMutation(
        SubscriptionId subscriptionId,
        long expectedRevision,
        string? expectedCursor,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            ExpectedRevision = expectedRevision,
            ExpectedPreviousCursor = expectedCursor,
            Checkpoint = new()
            {
                StreamId = Stream,
                SubscriptionId = subscriptionId,
                FrontierCursor = SnapshotRecoveryCursor,
                ServerVersion = ServerVersion,
                SnapshotFormatVersion = 1,
                ClientState = CreatePayload(SnapshotRecoveryAuthoritativeText),
                ObservedAtUtc = DateTimeOffset.UnixEpoch,
            },
            OptimisticState = CreatePayload(SnapshotRecoveryOptimisticText),
            SnapshotFormatVersion = 1,
            OperationDispositions = dispositions,
        };

    /// <summary>Creates an included snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="kind">The server result kind.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition IncludedSnapshotDisposition(OperationId operationId, OperationResultKind kind) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted, Result = new(operationId, kind, null, ServerVersion) };

    /// <summary>Creates a rejected snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition RejectedSnapshotDisposition(OperationId operationId)
    {
        var result = new OperationSyncResult(operationId, OperationResultKind.Rejected, "OC.Rejected", ServerVersion);
        return new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.TerminalRejected, Result = result };
    }

    /// <summary>Creates an unknown snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition UnknownSnapshotDisposition(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown };

    /// <summary>Creates an invalid local recovery mutation for validation coverage.</summary>
    /// <param name="scenario">The invalid scenario.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The invalid mutation.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The scenario is unknown.</exception>
    private static LocalSnapshotRecoveryMutation CreateInvalidSnapshotRecoveryMutation(
        string scenario,
        SubscriptionId subscriptionId,
        OperationId operationId)
    {
        var mutation = CreateSnapshotRecoveryMutation(subscriptionId, expectedRevision: 0, expectedCursor: null, [UnknownSnapshotDisposition(operationId)]);
        return scenario switch
        {
            "checkpoint-stream" => mutation with { Checkpoint = mutation.Checkpoint with { StreamId = new("snapshot-recovery-other-stream") } },
            "negative-revision" => mutation with { ExpectedRevision = -1 },
            "revision-overflow" => mutation with { ExpectedRevision = long.MaxValue },
            "unknown-with-result" => mutation with
            {
                OperationDispositions =
                [
                    new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown, Result = new(operationId, OperationResultKind.Accepted, null, ServerVersion) },
                ],
            },
            "result-operation" => mutation with
            {
                OperationDispositions =
                [
                    new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted, Result = new(OperationId.New(), OperationResultKind.Accepted, null, ServerVersion) },
                ],
            },
            "included-rejected" => mutation with { OperationDispositions = [IncludedSnapshotDisposition(operationId, OperationResultKind.Rejected)] },
            "terminal-accepted" => mutation with
            {
                OperationDispositions =
                [
                    new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.TerminalRejected, Result = new(operationId, OperationResultKind.Accepted, null, ServerVersion) },
                ],
            },
            "snapshot-format" => mutation with { SnapshotFormatVersion = 0 },
            "checkpoint-format" => mutation with { Checkpoint = mutation.Checkpoint with { SnapshotFormatVersion = 0 } },
            _ => throw new ArgumentOutOfRangeException(nameof(scenario), scenario, "Unknown snapshot recovery validation scenario."),
        };
    }

    /// <summary>Compares optional payload envelopes by content.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The expected payload.</param>
    /// <returns>Whether the payloads have matching content.</returns>
    private static bool SamePayload(PayloadEnvelope? left, PayloadEnvelope right) =>
        left is not null
        && string.Equals(left.ContractId, right.ContractId, StringComparison.Ordinal)
        && left.SchemaVersion == right.SchemaVersion
        && string.Equals(left.ContentType, right.ContentType, StringComparison.Ordinal)
        && CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(left.PayloadHash), Encoding.UTF8.GetBytes(right.PayloadHash))
        && left.Payload.ToArray().SequenceEqual(right.Payload.ToArray());

    /// <summary>Requires the SQLite snapshot recovery interface without depending on the adapter declaration.</summary>
    /// <param name="candidate">The candidate store.</param>
    /// <returns>The snapshot recovery store.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not implemented the interface.</exception>
    private static ILocalSnapshotRecoveryStore RequireSnapshotRecoveryStore(object candidate) =>
        candidate as ILocalSnapshotRecoveryStore
        ?? throw new InvalidOperationException("Expected the SQLite local store to implement snapshot recovery.");

    /// <summary>Creates a trigger that aborts recovery snapshot updates.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CreateSnapshotRecoveryRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_snapshot_recovery_abort
            AFTER UPDATE OF revision ON oc_snapshots
            WHEN NEW.server_cursor = 'snapshot-recovery-cursor'
            BEGIN
                SELECT RAISE(ABORT, 'rollback snapshot recovery');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox operation id to oversized corrupt text.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetOutboxOperationIdOversized(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            UPDATE oc_outbox SET operation_id = $operationId;
            PRAGMA foreign_keys = ON;
            """;
        _ = command.Parameters.AddWithValue("$operationId", new string('x', OversizedOperationIdentifierLength));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Restores the outbox operation identifier.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SetOutboxOperationId(string path, OperationId operationId) =>
        SetOutboxOperationIdText(path, operationId.Value.ToString("D"));

    /// <summary>Sets the outbox operation identifier to raw text.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier text.</param>
    private static void SetOutboxOperationIdText(string path, string operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            UPDATE oc_outbox SET operation_id = $operationId;
            PRAGMA foreign_keys = ON;
            """;
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the durable operation state directly.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="operationState">The operation state value.</param>
    private static void SetOutboxOperationState(string path, OperationId operationId, int operationState)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO oc_outbox_operation_states
                (store_identity, operation_id, operation_state, attempt_count, changed_at_utc)
            VALUES ($storeIdentity, $operationId, $operationState, 0, $changedAtUtc)
            ON CONFLICT (store_identity, operation_id)
            DO UPDATE SET
                operation_state = excluded.operation_state,
                changed_at_utc = excluded.changed_at_utc;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.Parameters.AddWithValue("$operationState", operationState);
        _ = command.Parameters.AddWithValue("$changedAtUtc", DateTimeOffset.UnixEpoch.ToString("O"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes the durable operation state directly.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    private static void DeleteOutboxOperationState(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DELETE FROM oc_outbox_operation_states
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads the number of durable lease rows for one operation.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The durable lease row count.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected count shape.</exception>
    private static long ReadLeaseOperationCount(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM oc_outbox_leases
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(OperationIdParameter, operationId.Value.ToString("D"));
        return command.ExecuteScalar() is long count
            ? count
            : throw new InvalidOperationException("SQLite lease row count returned an unexpected value.");
    }

    /// <summary>A manually advanced clock for lease expiry tests.</summary>
    /// <param name="utcNow">The initial UTC timestamp.</param>
    private sealed class MutableTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current timestamp.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Advances the current timestamp.</summary>
        /// <param name="duration">The positive duration to add.</param>
        public void Advance(TimeSpan duration) => _utcNow = _utcNow.Add(duration);
    }
}
