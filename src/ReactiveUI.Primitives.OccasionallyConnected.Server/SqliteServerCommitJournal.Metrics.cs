// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Maintains transactional retained counters without scanning the store on each request.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>The retained stream counter name.</summary>
    private const string StreamMetric = "streams";

    /// <summary>The retained ledger counter name.</summary>
    private const string LedgerMetric = "ledger";

    /// <summary>The retained event counter name.</summary>
    private const string EventMetric = "events";

    /// <summary>The retained subscription counter name.</summary>
    private const string SubscriptionMetric = "subscriptions";

    /// <summary>The retained offer counter name.</summary>
    private const string OfferMetric = "offers";

    /// <summary>The retained byte column name.</summary>
    private const string LogicalBytesColumnName = "logical_bytes";

    /// <summary>The retained byte validity expression.</summary>
    private const string InvalidBytesExpression = "typeof(logical_bytes) != 'integer' OR logical_bytes < 0";

    /// <summary>The insert trigger operation.</summary>
    private const string InsertOperation = "INSERT";

    /// <summary>The delete trigger operation.</summary>
    private const string DeleteOperation = "DELETE";

    /// <summary>Creates indexes for operation children, receive pages and expiry reclamation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The startup transaction.</param>
    private static void InitializeReadIndexes(SqliteDatabase connection, SqliteTransaction transaction)
    {
        string[] indexes =
        [
            "events_operation ON oc_server_journal_events (tenant_id, stream_id, client_id, operation_id, event_index)",
            "ledger_expiry ON oc_server_journal_ledger (expires_at_utc)",
            "ledger_group ON oc_server_journal_ledger (tenant_id, stream_id, group_sequence)",
            "ledger_timestamp ON oc_server_journal_ledger (tenant_id, stream_id, committed_at_utc, group_sequence)",
            "subscription_expiry ON oc_server_journal_subscriptions (last_touched_utc)",
            "offer_expiry ON oc_server_journal_subscription_offers (offered_at_utc)",
            "offer_group ON oc_server_journal_subscription_offers (subscription_id, group_sequence)",
        ];
        foreach (var index in indexes)
        {
            using var command = connection.CreateStatement();
            command.UseTransaction(transaction);
            command.SetSql($"CREATE INDEX IF NOT EXISTS oc_server_{index};");
            _ = command.Execute();
        }
    }

    /// <summary>Seeds counters once for existing journals and installs transactional delta triggers.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The startup transaction.</param>
    private static void InitializeMetrics(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var exists = connection.CreateStatement();
        exists.UseTransaction(transaction);
        exists.SetSql("SELECT COUNT(*) FROM oc_server_journal_metadata WHERE key = 'metric_streams';");
        if (ReadCount(exists.Scalar(), InvalidLogicalBytesMessage) == 0)
        {
            var metrics = ScanMetrics(connection, transaction);
            InsertMetric(connection, transaction, StreamMetric, metrics.StreamCount);
            InsertMetric(connection, transaction, LedgerMetric, metrics.LedgerEntryCount);
            InsertMetric(connection, transaction, EventMetric, metrics.EventCount);
            InsertMetric(connection, transaction, SubscriptionMetric, metrics.SubscriptionCount);
            InsertMetric(connection, transaction, OfferMetric, metrics.SubscriptionOfferCount);
            InsertMetric(connection, transaction, "bytes", metrics.LogicalBytes);
            InsertMetric(connection, transaction, "invalid", 0);
        }

        CreateMetricTriggers(
            connection,
            transaction,
            StreamMetric,
            StreamsTableName,
            "24 + length(CAST(tenant_id AS BLOB)) + length(CAST(stream_id AS BLOB)) + state_bytes + last_cursor_bytes",
            """
            typeof(state_bytes) != 'integer' OR state_bytes < 0 OR typeof(last_cursor_bytes) != 'integer' OR last_cursor_bytes < 0
            OR typeof(tenant_id) != 'text' OR trim(tenant_id) = ''
            OR typeof(stream_id) != 'text' OR trim(stream_id) = '' OR instr(stream_id, '..') > 0
            OR instr(stream_id, '//') > 0 OR substr(stream_id, 1, 1) = '/' OR substr(stream_id, -1) = '/'
            OR length(CAST(stream_id AS BLOB)) > 256
            """,
            ["tenant_id", "stream_id", "state_bytes", "last_cursor_bytes"]);
        CreateMetricTriggers(
            connection,
            transaction,
            LedgerMetric,
            LedgerTableName,
            LogicalBytesColumnName,
            InvalidBytesExpression + " " + """
                OR typeof(client_id) != 'text' OR trim(client_id) = ''
                OR typeof(operation_id) != 'text' OR length(operation_id) != 36
                OR operation_id = '00000000-0000-0000-0000-000000000000'
                """,
            [LogicalBytesColumnName, "client_id", "operation_id"]);
        CreateMetricTriggers(connection, transaction, EventMetric, EventsTableName, "0", "0", []);
        CreateMetricTriggers(connection, transaction, SubscriptionMetric, SubscriptionsTableName, LogicalBytesColumnName, InvalidBytesExpression, [LogicalBytesColumnName]);
        CreateMetricTriggers(connection, transaction, OfferMetric, SubscriptionOffersTableName, LogicalBytesColumnName, InvalidBytesExpression, [LogicalBytesColumnName]);
    }

    /// <summary>Stores one initial metric value.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="name">The metric name.</param>
    /// <param name="value">The initial value.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static void InsertMetric(SqliteDatabase connection, SqliteTransaction transaction, string name, long value) =>
        InsertMetadata(connection, transaction, $"metric_{name}", value.ToString(CultureInfo.InvariantCulture));

    /// <summary>Creates insert, delete and update delta triggers for one retained table.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="name">The counter name.</param>
    /// <param name="table">The table name.</param>
    /// <param name="bytes">The row byte expression.</param>
    /// <param name="invalid">The invalid byte expression.</param>
    /// <param name="columns">The expression's column names.</param>
    private static void CreateMetricTriggers(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string name,
        string table,
        string bytes,
        string invalid,
        string[] columns)
    {
        var newBytes = QualifyMetricExpression(bytes, columns, "NEW");
        var oldBytes = QualifyMetricExpression(bytes, columns, "OLD");
        var newInvalid = $"CASE WHEN {QualifyMetricExpression(invalid, columns, "NEW")} THEN 1 ELSE 0 END";
        var oldInvalid = $"CASE WHEN {QualifyMetricExpression(invalid, columns, "OLD")} THEN 1 ELSE 0 END";
        string[] operations = [InsertOperation, DeleteOperation, "UPDATE"];
        foreach (var operation in operations)
        {
            var countDelta = GetMetricDelta(operation, "1", "1");
            var byteDelta = GetMetricDelta(operation, newBytes, oldBytes);
            var invalidDelta = GetMetricDelta(operation, newInvalid, oldInvalid);
            using var command = connection.CreateStatement();
            command.UseTransaction(transaction);
            command.SetSql($"""
                CREATE TRIGGER IF NOT EXISTS oc_server_metric_{name}_{operation} AFTER {operation} ON {table}
                BEGIN
                    UPDATE oc_server_journal_metadata SET value = CAST(value AS INTEGER) + ({countDelta}) WHERE key = 'metric_{name}';
                    UPDATE oc_server_journal_metadata SET value = CAST(value AS INTEGER) + ({byteDelta}) WHERE key = 'metric_bytes';
                    UPDATE oc_server_journal_metadata SET value = CAST(value AS INTEGER) + ({invalidDelta}) WHERE key = 'metric_invalid';
                END;
                """);
            _ = command.Execute();
        }
    }

    /// <summary>Creates a transactional metric delta for one trigger operation.</summary>
    /// <param name="operation">The insert, delete or update operation.</param>
    /// <param name="newValue">The new row expression.</param>
    /// <param name="oldValue">The old row expression.</param>
    /// <returns>The SQL delta expression.</returns>
    private static string GetMetricDelta(string operation, string newValue, string oldValue) =>
        operation switch
        {
            InsertOperation => newValue,
            DeleteOperation => $"-({oldValue})",
            _ => $"({newValue}) - ({oldValue})",
        };

    /// <summary>Qualifies trusted column names in a metric expression.</summary>
    /// <param name="expression">The trusted SQL expression.</param>
    /// <param name="columns">The expression columns.</param>
    /// <param name="prefix">The trigger row prefix.</param>
    /// <returns>The qualified expression.</returns>
    private static string QualifyMetricExpression(string expression, string[] columns, string prefix)
    {
        foreach (var column in columns)
        {
            expression = expression.Replace(column, $"{prefix}.{column}");
        }

        return expression;
    }

    /// <summary>Reads retained metrics using primary-key lookups.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The retained metrics.</returns>
    private static RetainedMetrics ReadMetrics(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var metrics = new RetainedMetrics
        {
            StreamCount = checked((int)ReadMetric(connection, transaction, StreamMetric)),
            LedgerEntryCount = checked((int)ReadMetric(connection, transaction, LedgerMetric)),
            EventCount = checked((int)ReadMetric(connection, transaction, EventMetric)),
            SubscriptionCount = checked((int)ReadMetric(connection, transaction, SubscriptionMetric)),
            SubscriptionOfferCount = checked((int)ReadMetric(connection, transaction, OfferMetric)),
            LogicalBytes = ReadMetric(connection, transaction, "bytes"),
        };
        ThrowIfFalse(ReadMetric(connection, transaction, "invalid") == 0, InvalidLogicalBytesMessage);
        return metrics;
    }

    /// <summary>Reads one validated nonnegative metric.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="name">The metric name.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">The stored metric is invalid.</exception>
    private static long ReadMetric(SqliteDatabase connection, SqliteTransaction transaction, string name) =>
        long.TryParse(SelectMetadata(connection, transaction, $"metric_{name}"), NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= 0
            ? value
            : throw new InvalidOperationException(InvalidLogicalBytesMessage);
}
