// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteIdentityStoreData"/>.</summary>
public sealed class SqliteIdentityStoreDataTests
{
    /// <summary>Verifies SQLite scalar conversion helpers fail closed for malformed provider values.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenScalarValuesAreMalformed_ThenReadersFailClosed()
    {
        Action version = static () => _ = SqliteIdentityStoreData.ReadUserVersion("1");
        Action hasTables = static () => _ = SqliteIdentityStoreData.ReadHasUserTables("1");
        Action metadata = static () => _ = SqliteIdentityStoreData.ReadMetadataValue(1L);
        Action subscriptionType = static () => _ = SqliteIdentityStoreData.ReadSubscriptionId(1L);
        Action subscriptionText = static () => _ = SqliteIdentityStoreData.ReadSubscriptionId("not-a-guid");
        Action subscriptionEmpty = static () => _ = SqliteIdentityStoreData.ReadSubscriptionId(Guid.Empty.ToString("D"));

        await Assert.That(version).ThrowsExactly<InvalidOperationException>();
        await Assert.That(hasTables).ThrowsExactly<InvalidOperationException>();
        await Assert.That(metadata).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscriptionType).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscriptionText).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscriptionEmpty).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies bare file paths map to the current directory when deriving a creatable directory.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabasePathHasNoDirectory_ThenCurrentDirectoryIsUsedForCreation() =>
        await Assert.That(SqliteIdentityStoreData.GetDirectoryForCreate("identity.db")).IsEqualTo(".");
}
