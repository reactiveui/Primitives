// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
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
        using var connection = await CreateMetadataConnectionAsync();
        await InsertMetadataAsync(connection, SqliteRecordProtectionMaintenance.ProtectionMetadataKey, "future-format");
        using var transaction = connection.BeginTransaction();

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
        using var connection = await CreateMetadataConnectionAsync();
        using var transaction = connection.BeginTransaction();

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
        using var connection = await CreateMetadataConnectionAsync();
        await InsertMetadataAsync(
            connection,
            SqliteRecordProtectionMaintenance.ProtectionMetadataKey,
            SqliteRecordProtectionMaintenance.ProtectionFormat);
        using var transaction = connection.BeginTransaction();

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
        using var connection = await CreateMetadataConnectionAsync();
        var cipher = new SqliteRecordCipher(CreateProtection(), string.Empty);
        var check = cipher.ProtectText("wrong check value", SqliteRecordContext.KeyCheck(), "value");
        await InsertMetadataAsync(
            connection,
            SqliteRecordProtectionMaintenance.ProtectionMetadataKey,
            SqliteRecordProtectionMaintenance.ProtectionFormat);
        await InsertMetadataAsync(connection, SqliteRecordProtectionMaintenance.KeyCheckMetadataKey, check);
        using var transaction = connection.BeginTransaction();

        await Assert.That(() => SqliteRecordProtectionMaintenance.EnsureProtectionState(
                connection,
                transaction,
                CreateProtection(),
                CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates an in-memory metadata table without protection rows.</summary>
    /// <returns>The open connection.</returns>
    private static async Task<SqliteDatabase> CreateMetadataConnectionAsync()
    {
        var connection = new SqliteDatabase(":memory:");

        using var command = connection.CreateStatement();
        command.SetSql("CREATE TABLE oc_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);");
        _ = command.Execute();
        return connection;
    }

    /// <summary>Inserts one metadata row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    /// <returns>A task that represents the asynchronous insert.</returns>
    private static async Task InsertMetadataAsync(SqliteDatabase connection, string key, string value)
    {
        using var command = connection.CreateStatement();
        command.SetSql("INSERT INTO oc_metadata (key, value) VALUES ($key, $value);");
        _ = command.Bind("$key", key);
        _ = command.Bind("$value", value);
        _ = command.Execute();
    }

    /// <summary>Creates protection with a fixed test key.</summary>
    /// <returns>The record protection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SqliteRecordProtection CreateProtection() =>
        SqliteRecordProtection.Create(new StaticLocalStoreKeyProvider(new LocalStoreKey("maintenance", new byte[32])));
}
