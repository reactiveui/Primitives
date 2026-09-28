// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Encryption at rest, key rotation, and plaintext transition tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies new writes use the current key, old keys still decrypt, and rotation re-encrypts every record.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenKeyIsRotated_ThenRecordsMoveToTheCurrentKeyAndOldKeyCanBeRetired()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        var first = CreateTestKey(FirstKeyId, FirstKeyFill);
        var second = CreateTestKey(SecondKeyId, SecondKeyFill);
        await using (var adapter = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(second, [first])))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            await AssertEncryptedSeedAsync(adapter, seed);
            await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(FirstKeyId);
            var rewritten = await adapter.RotateEncryptionKeyAsync(CancellationToken.None);
            var repeated = await adapter.RotateEncryptionKeyAsync(CancellationToken.None);
            await Assert.That(rewritten).IsGreaterThan(0);
            await Assert.That(repeated).IsEqualTo(0);
        }

        await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(SecondKeyId);
        await Assert.That(ReadStreamCursorKeyId(database.Path)).IsEqualTo(SecondKeyId);
        await using var retired = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(second));
        await retired.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(retired, seed);
        await Assert.That(FindPlaintextSentinels(database.Path)).IsEmpty();
    }

    /// <summary>Verifies an existing plaintext database is encrypted in place when a key provider is configured.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPlaintextStoreIsOpenedWithKeyProvider_ThenTransitionEncryptsEveryRecord()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedPlaintextDatabaseAsync(database.Path);
        await Assert.That(FindPlaintextSentinels(database.Path)).IsNotEmpty();

        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            await AssertEncryptedSeedAsync(adapter, seed);
        }

        await Assert.That(FindPlaintextSentinels(database.Path)).IsEmpty();
        await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(FirstKeyId);
        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(reopened, seed);
    }

    /// <summary>Verifies a crash before encryption commits leaves the plaintext database intact for a later retry.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptionTransitionCrashesBeforeCommit_ThenNextOpenRestartsIt()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedPlaintextDatabaseAsync(database.Path);
        await using (var crashing = CreateEncryptedAdapter(
            database.Path,
            CreateFirstKeyProvider(),
            new ThrowingCommitFaultPoint(SqliteCommitCheckpoint.EncryptionTransitionBeforeCommit)))
        {
            Func<Task> action = () => crashing.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None).AsTask();
            await Assert.That(action).ThrowsExactly<IOException>();
        }

        await Assert.That(ReadProtectionMarkerCount(database.Path)).IsEqualTo(0L);
        await using (var plaintext = CreateAdapter(database.Path))
        {
            await plaintext.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);
            await AssertEncryptedSeedAsync(plaintext, seed);
        }

        await using var restarted = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await restarted.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(restarted, seed);
        await Assert.That(ReadProtectionMarkerCount(database.Path)).IsEqualTo(1L);
    }

    /// <summary>Verifies a crash right after encryption commits leaves a fully encrypted database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptionTransitionCrashesAfterCommit_ThenDatabaseIsFullyEncrypted()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedPlaintextDatabaseAsync(database.Path);
        await using (var crashing = CreateEncryptedAdapter(
            database.Path,
            CreateFirstKeyProvider(),
            new ThrowingCommitFaultPoint(SqliteCommitCheckpoint.EncryptionTransitionAfterCommit)))
        {
            Func<Task> action = () => crashing.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None).AsTask();
            await Assert.That(action).ThrowsExactly<IOException>();
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(reopened, seed);
        await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(FirstKeyId);
    }

    /// <summary>Verifies a crash before the rotation commits keeps every record readable under the old key.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRotationCrashesBeforeCommit_ThenRecordsStayUnderTheOldKey()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        var provider = new StaticLocalStoreKeyProvider(CreateTestKey(SecondKeyId, SecondKeyFill), [CreateTestKey(FirstKeyId, FirstKeyFill)]);
        await using (var crashing = CreateEncryptedAdapter(
            database.Path,
            provider,
            new ThrowingCommitFaultPoint(SqliteCommitCheckpoint.KeyRotationBeforeCommit)))
        {
            await crashing.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            Func<Task> action = () => crashing.RotateEncryptionKeyAsync(CancellationToken.None).AsTask();
            await Assert.That(action).ThrowsExactly<IOException>();
        }

        await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(FirstKeyId);
        await using var reopened = CreateEncryptedAdapter(database.Path, provider);
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(reopened, seed);
    }

    /// <summary>Creates a plaintext database that holds every protected record type.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The seeded stream.</returns>
    private static async Task<EncryptedSeed> SeedPlaintextDatabaseAsync(string path)
    {
        await using var adapter = CreateAdapter(path);
        await adapter.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);
        return await SeedEncryptedStreamAsync(adapter);
    }
}
