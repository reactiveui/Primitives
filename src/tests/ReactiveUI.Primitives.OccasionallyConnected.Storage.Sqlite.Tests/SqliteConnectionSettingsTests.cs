// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteConnectionSettings"/>.</summary>
public sealed class SqliteConnectionSettingsTests
{
    /// <summary>Verifies durability configuration fails closed when WAL cannot be enabled.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenWalCannotBeEnabled_ThenDurabilityConfigurationFailsClosed()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = ":memory:", Mode = SqliteOpenMode.Memory, Pooling = false }.ToString();
        await using var connection = new SqliteConnection(connectionString);
        connection.Open();

        Action action = () => SqliteConnectionSettings.ConfigureDurability(connection);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies WAL verification fails closed when SQLite returns an unexpected value.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenWalJournalModeValueIsUnexpected_ThenVerificationFailsClosed()
    {
        Action wrongString = static () => SqliteConnectionSettings.VerifyWalJournalMode("delete");
        Action wrongType = static () => SqliteConnectionSettings.VerifyWalJournalMode(1L);

        await Assert.That(wrongString).ThrowsExactly<InvalidOperationException>();
        await Assert.That(wrongType).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies FULL synchronous verification fails closed when SQLite returns an unexpected value.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenFullSynchronousValueIsUnexpected_ThenVerificationFailsClosed()
    {
        Action wrongNumber = static () => SqliteConnectionSettings.VerifyFullSynchronous(1L);
        Action wrongType = static () => SqliteConnectionSettings.VerifyFullSynchronous("2");

        await Assert.That(wrongNumber).ThrowsExactly<InvalidOperationException>();
        await Assert.That(wrongType).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies foreign-key verification fails closed when SQLite returns an unexpected value.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenForeignKeyValueIsUnexpected_ThenVerificationFailsClosed()
    {
        Action wrongNumber = static () => SqliteConnectionSettings.VerifyForeignKeys(0L);
        Action wrongType = static () => SqliteConnectionSettings.VerifyForeignKeys("1");

        await Assert.That(wrongNumber).ThrowsExactly<InvalidOperationException>();
        await Assert.That(wrongType).ThrowsExactly<InvalidOperationException>();
    }
}
