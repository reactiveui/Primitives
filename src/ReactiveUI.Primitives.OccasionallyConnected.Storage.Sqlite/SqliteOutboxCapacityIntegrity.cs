// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Validates derived accounting on cold opens and external changes, not on trusted warm commits.</summary>
internal static class SqliteOutboxCapacityIntegrity
{
    /// <summary>The authentication context for immutable byte charges.</summary>
    private const string ProofColumn = "outbox_capacity_bytes";

    /// <summary>The encoded charge column in a proof rewrite query.</summary>
    private const int EncodedChargeIndex = 2;

    /// <summary>The proof column in an integrity query.</summary>
    private const int ChargeProofIndex = 2;

    /// <summary>The stored proof length column.</summary>
    private const int ChargeProofLengthIndex = 3;

    /// <summary>The largest authenticated envelope needed for one eight-byte immutable charge.</summary>
    private const int MaximumChargeProofBytes = sizeof(long) + SqliteRecordProtection.MaximumEnvelopeOverhead;

    /// <summary>The connection-local trusted counter version.</summary>
    private static readonly ConditionalWeakTable<SqliteDatabase, TrustedVersion> Versions = new();

    /// <summary>Initializes proofs after encryption transition and verifies derived rows.</summary>
    /// <param name="connection">The initialized connection.</param>
    /// <param name="transaction">The initialization transaction.</param>
    /// <param name="migrated">Whether this transaction enabled protection on plaintext rows.</param>
    internal static void Initialize(SqliteDatabase connection, SqliteTransaction transaction, bool migrated)
    {
        if (migrated)
        {
            RefreshProofs(connection, transaction);
        }

        VerifyWhenChanged(connection, transaction);
        if (!migrated && SqliteRecordCipher.For(connection) is not null)
        {
            RefreshProofs(connection, transaction);
        }
    }

    /// <summary>Creates a proof bound to the store and operation id.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="bytes">The immutable plaintext byte charge.</param>
    /// <returns>The protected proof, or null for plaintext stores.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte[]? CreateProof(SqliteDatabase connection, OperationId operationId, long bytes) =>
        SqliteRecordCipher.For(connection)?.ProtectBytes(
            BitConverter.GetBytes(bytes),
            SqliteRecordContext.OutboxMetadata(operationId, ProofColumn),
            ProofColumn);

    /// <summary>Verifies a changed snapshot while the write lock prevents external races.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <exception cref="InvalidOperationException">Derived accounting is missing or inconsistent.</exception>
    internal static void VerifyWhenChanged(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var trusted = Versions.GetValue(connection, static _ => new());
        var version = SqliteLocalCommitConnection.GetDataVersion(connection, transaction);
        var changes = TotalChanges(connection);
        if (trusted.DataVersion == version && trusted.TotalChanges == changes)
        {
            return;
        }

        VerifyRows(connection, transaction);
        VerifyProofs(connection, transaction);
        trusted.DataVersion = version;
        trusted.TotalChanges = changes;
    }

    /// <summary>Records only successfully committed store-owned changes as trusted.</summary>
    /// <param name="connection">The connection.</param>
    internal static void RecordTrustedCommit(SqliteDatabase connection)
    {
        var trusted = Versions.GetValue(connection, static _ => new());
        trusted.TotalChanges = TotalChanges(connection);
    }

    /// <summary>Reprotects immutable charges during encryption transition or key rotation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    internal static void RefreshProofs(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT store_identity, operation_id, encoded_bytes FROM oc_outbox_capacity_charges;");
        using var rows = command.Query();
        while (rows.Read())
        {
            using var update = connection.CreateStatement();
            update.UseTransaction(transaction);
            update.SetSql("""
                UPDATE oc_outbox_capacity_charges SET proof = $proof
                WHERE store_identity = $identity AND operation_id = $operation;
                """);
            _ = update.Bind("$identity", rows.GetString(0));
            _ = update.Bind("$operation", rows.GetString(1));
            _ = update.Bind("$proof", CreateProof(connection, new(Guid.Parse(rows.GetString(1))), rows.GetInt64(EncodedChargeIndex)));
            _ = update.Execute();
        }
    }

    /// <summary>Checks row coverage, terminal classification, and usage against immutable charges.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <exception cref="InvalidOperationException">The derived accounting is inconsistent.</exception>
    private static void VerifyRows(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT EXISTS (
                SELECT 1 FROM oc_outbox AS operation
                LEFT JOIN oc_outbox_capacity_charges AS charge
                    ON charge.store_identity = operation.store_identity AND charge.operation_id = operation.operation_id
                LEFT JOIN oc_outbox_operation_states AS state
                    ON state.store_identity = operation.store_identity AND state.operation_id = operation.operation_id
                WHERE charge.operation_id IS NULL OR charge.unresolved != COALESCE(state.operation_state NOT IN (4, 5, 6), 1)
            ) OR EXISTS (
                SELECT 1 FROM oc_outbox_capacity_charges AS charge
                LEFT JOIN oc_outbox AS operation
                    ON charge.store_identity = operation.store_identity AND charge.operation_id = operation.operation_id
                WHERE operation.operation_id IS NULL
            ) OR EXISTS (
                SELECT 1 FROM (
                    SELECT store_identity, SUM(unresolved) AS count, SUM(encoded_bytes * unresolved) AS bytes
                    FROM oc_outbox_capacity_charges GROUP BY store_identity
                ) AS expected
                LEFT JOIN oc_outbox_capacity_usage AS usage ON usage.store_identity = expected.store_identity
                WHERE usage.store_identity IS NULL OR usage.operation_count != expected.count OR usage.encoded_bytes != expected.bytes
            ) OR EXISTS (
                SELECT 1 FROM oc_outbox_capacity_usage AS usage
                WHERE NOT EXISTS (SELECT 1 FROM oc_outbox_capacity_charges AS charge WHERE charge.store_identity = usage.store_identity)
                    AND (usage.operation_count != 0 OR usage.encoded_bytes != 0)
            );
            """);
        if ((long)command.Scalar()! != 0)
        {
            throw new InvalidOperationException("The SQLite outbox capacity accounting is inconsistent.");
        }
    }

    /// <summary>Authenticates small immutable charge proofs without materializing unrelated payloads.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <exception cref="LocalStoreRecordAuthenticationException">An immutable charge proof is invalid.</exception>
    private static void VerifyProofs(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var cipher = SqliteRecordCipher.For(connection);
        if (cipher is null)
        {
            return;
        }

        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT operation_id, encoded_bytes, IFNULL(substr(CAST(proof AS BLOB), 1, $maximumProofBytes), x''),
                COALESCE(length(CAST(proof AS BLOB)), 0)
            FROM oc_outbox_capacity_charges;
            """);
        _ = command.Bind("$maximumProofBytes", MaximumChargeProofBytes);
        using var rows = command.Query();
        while (rows.Read())
        {
            var operationId = new OperationId(Guid.Parse(rows.GetString(0)));
            if (rows.GetInt64(ChargeProofLengthIndex) > MaximumChargeProofBytes)
            {
                throw new LocalStoreRecordAuthenticationException("The SQLite outbox capacity proof exceeds its bounded envelope.");
            }

            var proof = rows.GetFieldValue<byte[]>(ChargeProofIndex);
            var decoded = cipher.UnprotectBytes(proof, SqliteRecordContext.OutboxMetadata(operationId, ProofColumn), ProofColumn);
            if (decoded.Length != sizeof(long) || BitConverter.ToInt64(decoded, 0) != rows.GetInt64(1))
            {
                throw new LocalStoreRecordAuthenticationException("The SQLite outbox capacity charge failed authentication.");
            }
        }
    }

    /// <summary>Reads the connection-local write count.</summary>
    /// <param name="connection">The connection.</param>
    /// <returns>The write count including rolled-back changes.</returns>
    private static long TotalChanges(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        command.SetSql("SELECT total_changes();");
        return (long)command.Scalar()!;
    }

    /// <summary>Tracks the last trusted connection snapshot.</summary>
    private sealed class TrustedVersion
    {
        /// <summary>Gets or sets the external database version.</summary>
        internal long DataVersion { get; set; } = -1;

        /// <summary>Gets or sets the trusted local write count.</summary>
        internal long TotalChanges { get; set; } = -1;
    }
}
