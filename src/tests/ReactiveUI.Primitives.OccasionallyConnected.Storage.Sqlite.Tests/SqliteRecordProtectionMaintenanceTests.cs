// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteRecordProtectionMaintenance"/>.</summary>
public sealed class SqliteRecordProtectionMaintenanceTests
{
    /// <summary>Verifies an unknown protection marker prevents opening the database with a key.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnsupportedProtectionFormatFailsBeforeRecordAccess()
    {
        await using var connection = await CreateMetadataConnectionAsync();
        await InsertMetadataAsync(connection, SqliteRecordProtectionMaintenance.ProtectionMetadataKey, "future-format");
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();

        await Assert.That(() => SqliteRecordProtectionMaintenance.EnsureProtectionState(
                connection,
                transaction,
                CreateProtection(),
                CancellationToken.None))
            .ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies key rotation refuses a database without a protection marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task KeyRotationRequiresProtectedDatabase()
    {
        await using var connection = await CreateMetadataConnectionAsync();
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();

        await Assert.That(() => SqliteRecordProtectionMaintenance.RotateKeys(
                connection,
                transaction,
                CreateProtection(),
                CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a protected database without its key check cannot be reopened.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task MissingKeyCheckFailsClosed()
    {
        await using var connection = await CreateMetadataConnectionAsync();
        await InsertMetadataAsync(
            connection,
            SqliteRecordProtectionMaintenance.ProtectionMetadataKey,
            SqliteRecordProtectionMaintenance.ProtectionFormat);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();

        await Assert.That(() => SqliteRecordProtectionMaintenance.EnsureProtectionState(
                connection,
                transaction,
                CreateProtection(),
                CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a valid envelope containing the wrong key check text fails closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task IncorrectKeyCheckPlaintextFailsClosed()
    {
        await using var connection = await CreateMetadataConnectionAsync();
        var cipher = new SqliteRecordCipher(CreateProtection(), string.Empty);
        var check = cipher.ProtectText("wrong check value", SqliteRecordContext.KeyCheck(), "value");
        await InsertMetadataAsync(
            connection,
            SqliteRecordProtectionMaintenance.ProtectionMetadataKey,
            SqliteRecordProtectionMaintenance.ProtectionFormat);
        await InsertMetadataAsync(connection, SqliteRecordProtectionMaintenance.KeyCheckMetadataKey, check);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync();

        await Assert.That(() => SqliteRecordProtectionMaintenance.EnsureProtectionState(
                connection,
                transaction,
                CreateProtection(),
                CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates an in-memory metadata table without protection rows.</summary>
    /// <returns>The open connection.</returns>
    private static async Task<SqliteConnection> CreateMetadataConnectionAsync()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = ":memory:" }.ToString();
        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE oc_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);";
        _ = await command.ExecuteNonQueryAsync();
        return connection;
    }

    /// <summary>Inserts one metadata row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    /// <returns>A task that represents the asynchronous insert.</returns>
    private static async Task InsertMetadataAsync(SqliteConnection connection, string key, string value)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ($key, $value);";
        _ = command.Parameters.AddWithValue("$key", key);
        _ = command.Parameters.AddWithValue("$value", value);
        _ = await command.ExecuteNonQueryAsync();
    }

    /// <summary>Creates protection with a fixed test key.</summary>
    /// <returns>The record protection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SqliteRecordProtection CreateProtection() =>
        SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(new LocalStoreKey("maintenance", new byte[32])));
}
