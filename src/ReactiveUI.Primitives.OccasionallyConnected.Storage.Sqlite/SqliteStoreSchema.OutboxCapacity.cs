// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Defines and validates the SQLite local commit schema.</summary>
/// <content>Defines transactional outbox capacity accounting.</content>
internal static partial class SqliteStoreSchema
{
    /// <summary>The immutable plaintext charge and current unresolved classification of each operation.</summary>
    private const string CapacityChargesSql = """
        CREATE TABLE oc_outbox_capacity_charges (
            store_identity TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            encoded_bytes INTEGER NOT NULL CHECK (typeof(encoded_bytes) = 'integer' AND encoded_bytes >= 0),
            proof BLOB NULL,
            unresolved INTEGER NOT NULL CHECK (unresolved IN (0, 1)),
            PRIMARY KEY (store_identity, operation_id),
            FOREIGN KEY (store_identity, operation_id) REFERENCES oc_outbox (store_identity, operation_id)
                ON UPDATE CASCADE ON DELETE CASCADE);
        """;

    /// <summary>The maintained global usage for a store partition.</summary>
    private const string CapacityUsageSql = """
        CREATE TABLE oc_outbox_capacity_usage (
            store_identity TEXT NOT NULL PRIMARY KEY,
            operation_count INTEGER NOT NULL CHECK (typeof(operation_count) = 'integer' AND operation_count >= 0),
            encoded_bytes INTEGER NOT NULL CHECK (typeof(encoded_bytes) = 'integer' AND encoded_bytes >= 0));
        """;

    /// <summary>Checks whether the optional version-one capacity extension is present.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>Whether the charge table exists.</returns>
    internal static bool HasOutboxCapacitySchema(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'oc_outbox_capacity_charges';");
        return (long)command.Scalar()! != 0;
    }

    /// <summary>Creates counters and their transactional maintenance triggers.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void CreateOutboxCapacitySchema(SqliteDatabase connection, SqliteTransaction transaction)
    {
        ExecuteCapacitySql(connection, transaction, CapacityChargesSql);
        ExecuteCapacitySql(connection, transaction, CapacityUsageSql);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_charge_insert AFTER INSERT ON oc_outbox_capacity_charges BEGIN
                INSERT INTO oc_outbox_capacity_usage (store_identity, operation_count, encoded_bytes)
                VALUES (NEW.store_identity, NEW.unresolved, NEW.encoded_bytes * NEW.unresolved)
                ON CONFLICT (store_identity) DO UPDATE SET
                    operation_count = operation_count + NEW.unresolved,
                    encoded_bytes = encoded_bytes + NEW.encoded_bytes * NEW.unresolved;
            END;
            """);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_charge_update AFTER UPDATE OF unresolved ON oc_outbox_capacity_charges BEGIN
                UPDATE oc_outbox_capacity_usage SET
                    operation_count = operation_count + NEW.unresolved - OLD.unresolved,
                    encoded_bytes = encoded_bytes + NEW.encoded_bytes * (NEW.unresolved - OLD.unresolved)
                WHERE store_identity = NEW.store_identity;
            END;
            """);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_charge_delete AFTER DELETE ON oc_outbox_capacity_charges BEGIN
                UPDATE oc_outbox_capacity_usage SET
                    operation_count = operation_count - OLD.unresolved,
                    encoded_bytes = encoded_bytes - OLD.encoded_bytes * OLD.unresolved
                WHERE store_identity = OLD.store_identity;
            END;
            """);
        CreateOutboxCapacityStateTriggers(connection, transaction);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_outbox_delete BEFORE DELETE ON oc_outbox BEGIN
                DELETE FROM oc_outbox_capacity_charges
                WHERE store_identity = OLD.store_identity AND operation_id = OLD.operation_id;
            END;
            """);
    }

    /// <summary>Creates state transition triggers including terminal release and retry resurrection.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void CreateOutboxCapacityStateTriggers(SqliteDatabase connection, SqliteTransaction transaction)
    {
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_state_insert AFTER INSERT ON oc_outbox_operation_states BEGIN
                UPDATE oc_outbox_capacity_charges SET unresolved = NEW.operation_state NOT IN (4, 5, 6)
                WHERE store_identity = NEW.store_identity AND operation_id = NEW.operation_id;
            END;
            """);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_state_update AFTER UPDATE OF operation_state ON oc_outbox_operation_states BEGIN
                UPDATE oc_outbox_capacity_charges SET unresolved = NEW.operation_state NOT IN (4, 5, 6)
                WHERE store_identity = NEW.store_identity AND operation_id = NEW.operation_id;
            END;
            """);
        ExecuteCapacitySql(connection, transaction, """
            CREATE TRIGGER oc_capacity_state_delete AFTER DELETE ON oc_outbox_operation_states BEGIN
                UPDATE oc_outbox_capacity_charges SET unresolved = 1
                WHERE store_identity = OLD.store_identity AND operation_id = OLD.operation_id;
            END;
            """);
    }

    /// <summary>Gets table names for exact schema validation without upgrading the public schema version.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The optional extension names.</returns>
    private static string[] GetOutboxCapacityTableNames(SqliteDatabase connection, SqliteTransaction transaction) =>
        HasOutboxCapacitySchema(connection, transaction) ? ["oc_outbox_capacity_charges", "oc_outbox_capacity_usage"] : [];

    /// <summary>Validates the extension definitions when present. The checksum also covers its triggers.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void ValidateOutboxCapacitySchema(SqliteDatabase connection, SqliteTransaction transaction)
    {
        if (!HasOutboxCapacitySchema(connection, transaction))
        {
            return;
        }

        ValidateTableDefinition(connection, transaction, "oc_outbox_capacity_charges", CapacityChargesSql);
        ValidateTableDefinition(connection, transaction, "oc_outbox_capacity_usage", CapacityUsageSql);
    }

    /// <summary>Executes one schema statement inside the initialization transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="sql">The statement text.</param>
    private static void ExecuteCapacitySql(SqliteDatabase connection, SqliteTransaction transaction, string sql)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(sql);
        _ = command.Execute();
    }
}
