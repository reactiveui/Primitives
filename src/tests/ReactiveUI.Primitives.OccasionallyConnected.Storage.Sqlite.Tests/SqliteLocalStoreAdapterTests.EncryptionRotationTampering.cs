// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Key rotation with corrupted protected records.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies key rotation keeps a tampered old-key value for later authentication failure.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOldKeySnapshotIsTampered_ThenRotationDoesNotLaunderItsCiphertext()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        FlipSnapshotPayloadByte(database.Path);
        var corrupted = ReadSnapshotPayloadEnvelope(database.Path);
        var oldKey = CreateTestKey(FirstKeyId, FirstKeyFill);
        var newKey = CreateTestKey(SecondKeyId, SecondKeyFill);
        await using var adapter = CreateEncryptedAdapter(
            database.Path,
            new StaticLocalStoreKeyProvider(newKey, [oldKey]));
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);

        var rewritten = await adapter.RotateEncryptionKeyAsync(CancellationToken.None);
        var afterRotation = ReadSnapshotPayloadEnvelope(database.Path);

        await Assert.That(rewritten).IsGreaterThan(0);
        await Assert.That(afterRotation.AsSpan().SequenceEqual(corrupted)).IsTrue();
        Func<Task> recover = () => adapter.RecoverStreamAsync(Stream, seed.SubscriptionId, CancellationToken.None).AsTask();
        await Assert.That(recover)
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        var quarantine = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(quarantine?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
    }

    /// <summary>Reads the protected snapshot payload exactly as it is stored on disk.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The stored envelope.</returns>
    private static byte[] ReadSnapshotPayloadEnvelope(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateStatement();
        command.SetSql("SELECT payload FROM oc_snapshots WHERE stream_id = 'sensor/temperature';");
        return (byte[])command.Scalar()!;
    }
}
