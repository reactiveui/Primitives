// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <content>Tests the documented freshness boundary of authenticated database backups.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies ciphertext authentication alone cannot prove the presence of a removed inbox record.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncryptedInboxDeletionRequiresAnExternalCompletenessProof()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        Guid eventId;
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("SELECT event_id FROM oc_inbox LIMIT 1;");
            eventId = Guid.Parse((string)command.Scalar()!);
            command.SetSql("DELETE FROM oc_inbox WHERE event_id = $eventId;");
            _ = command.Bind("$eventId", eventId.ToString("D"));
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var unapplied = await reopened.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None);

        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(eventId);
    }

    /// <summary>Verifies record authentication does not turn a valid old backup into an external freshness proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncryptedBackupFreshnessRequiresAnExternalCheckpoint()
    {
        using var database = TempDatabase.Create();
        var backupPath = $"{database.Path}.verified-backup";
        try
        {
            await using (var initial = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
            {
                await initial.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
                _ = await initial.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
                _ = await initial.CommitLocalOperationAsync(
                    CreateEncryptedOperation(FirstClientSequence, EncryptedPayloadSentinel),
                    CreateSnapshotMutation(expectedRevision: 0),
                    CancellationToken.None);
            }

            File.Copy(database.Path, backupPath);
            await using (var current = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
            {
                await current.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
                _ = await current.CommitLocalOperationAsync(
                    CreateEncryptedOperation(SecondClientSequence, EncryptedSecondPayloadText),
                    CreateSnapshotMutation(expectedRevision: 1),
                    CancellationToken.None);
            }

            File.Copy(backupPath, database.Path, overwrite: true);
            await using var restored = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
            await restored.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            var subscription = await restored.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var recovered = await restored.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

            await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
            await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        }
        finally
        {
            File.Delete(backupPath);
        }
    }
}
