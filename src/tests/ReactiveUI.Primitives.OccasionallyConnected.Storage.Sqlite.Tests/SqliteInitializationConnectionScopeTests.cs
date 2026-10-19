// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests native ownership transfer after initialization.</summary>
public sealed class SqliteInitializationConnectionScopeTests
{
    /// <summary>Checks a failed initialization releases its connection while adoption leaves it open.</summary>
    /// <param name="retain">Whether initialization transfers the connection to the store.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InitializationScopeTransfersOnlyAdoptedConnections(bool retain)
    {
        using var database = new SqliteDatabase(":memory:");
        using var cancellation = new CancellationTokenSource();
        database.SetCancellation(cancellation.Token);
        var scope = new SqliteInitializationConnectionScope(database);
        if (retain)
        {
            _ = scope.Retain();
        }

        scope.Dispose();
        await cancellation.CancelAsync();
        await Assert.That(database.IsDisposed).IsEqualTo(!retain);
        if (!retain)
        {
            return;
        }

        using var command = database.CreateStatement();
        command.SetSql("SELECT 1;");
        await Assert.That(command.Scalar()).IsEqualTo(1L);
    }
}
