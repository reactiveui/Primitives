// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Local application timestamp tests.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Verifies delayed remote events start inbox retention at local application time.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOldRemoteEventArrives_ThenInboxRecordsLocalApplicationTime()
    {
        using var database = TempDatabase.Create();
        var clock = new ApplicationTimeProvider();
        using var store = new SqliteLocalCommitStore(database.Path, clock);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);

        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT committed_at_utc FROM oc_inbox WHERE event_id = $eventId;";
        _ = command.Parameters.AddWithValue("$eventId", remoteEvent.EventId.ToString("D"));
        var timestamp = await command.ExecuteScalarAsync() as string;
        await Assert.That(timestamp).IsEqualTo(clock.GetUtcNow().ToString("O", CultureInfo.InvariantCulture));
        await Assert.That(timestamp).IsNotEqualTo(remoteEvent.CommittedAtUtc.ToString("O", CultureInfo.InvariantCulture));
    }

    /// <summary>Supplies a local application time after the remote event was produced.</summary>
    private sealed class ApplicationTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => new(2026, 9, 12, 0, 0, 0, TimeSpan.Zero);
    }
}
