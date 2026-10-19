// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests platform requirements for native SQLite persistence.</summary>
public sealed class SqlitePlatformTests
{
    /// <summary>The number of settings restoration attempts.</summary>
    private const int RestorationAttempts = 2;

    /// <summary>Verifies both SQLite flags are enabled and restored when changed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FullFileSyncEnablesAndRestoresBothConnectionFlags()
    {
        using var connection = new SqliteDatabase(":memory:");
        for (var attempt = 0; attempt < RestorationAttempts; attempt++)
        {
            connection.Execute("PRAGMA fullfsync = OFF; PRAGMA checkpoint_fullfsync = OFF;");
            SqlitePlatform.ConfigureFullFileSync(connection);
            using var command = connection.CreateStatement();
            command.SetSql("PRAGMA fullfsync;");
            await Assert.That(command.Scalar()).IsEqualTo(1L);
            command.SetSql("PRAGMA checkpoint_fullfsync;");
            await Assert.That(command.Scalar()).IsEqualTo(1L);
        }
    }

    /// <summary>Verifies required synchronization fails closed for unsupported or invalid responses.</summary>
    /// <param name="value">The returned flag value.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(null)]
    [Arguments(0L)]
    [Arguments("1")]
    public async Task FullFileSyncRejectsUnexpectedFlagValues(object? value)
    {
        Action verify = () => SqlitePlatform.VerifyFullFileSync(value);
        await Assert.That(verify).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies browser rejection occurs at the platform capability gate.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DedicatedWorkersRejectUnsupportedPlatform()
    {
        Action create = static () => SqlitePlatform.RequireDedicatedWorkers(false);
        await Assert.That(create).ThrowsExactly<PlatformNotSupportedException>();
        SqlitePlatform.RequireDedicatedWorkers(true);
    }

    /// <summary>Verifies operational settings select full synchronization for the current platform.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OperationalConnectionUsesCurrentPlatformSynchronizationPolicy()
    {
        using var connection = new SqliteDatabase(":memory:");
        connection.Execute("PRAGMA fullfsync = OFF; PRAGMA checkpoint_fullfsync = OFF;");
        SqliteConnectionSettings.ConfigureOperationalConnection(connection);
        var expected = SqlitePlatform.RequiresFullFileSync ? 1L : 0L;
        using var command = connection.CreateStatement();
        command.SetSql("PRAGMA fullfsync;");
        await Assert.That(command.Scalar()).IsEqualTo(expected);
        command.SetSql("PRAGMA checkpoint_fullfsync;");
        await Assert.That(command.Scalar()).IsEqualTo(expected);
        await Assert.That(SqlitePlatform.SupportsDedicatedWorkers).IsEqualTo(!OperatingSystem.IsBrowser());
    }
}
