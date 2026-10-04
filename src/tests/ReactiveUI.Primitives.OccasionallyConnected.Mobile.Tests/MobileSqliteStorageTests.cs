// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Tests real SQLite composition through secure storage and app-data abstractions.</summary>
public sealed partial class MobileSqliteStorageTests
{
    /// <summary>The core store identity.</summary>
    private const string StoreName = "mobile";

    /// <summary>The SQLite file name.</summary>
    private const string FileName = "client.db";

    /// <summary>The secure storage entry name.</summary>
    private const string EntryName = "device";

    /// <summary>Checks encrypted SQLite initialization, rotation, durable identity and reopen.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task EncryptedSqliteUsesStableSecureIdentityAndRetainedKeys()
    {
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        ClientIdentity identity;
        string originalKey;
        await using (var mobile = await CreateAsync(secureStorage, fileSystem))
        {
            identity = mobile.Identity;
            originalKey = mobile.Keys.GetCurrentKey().KeyId;
            await mobile.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = identity.ClientId }, CancellationToken.None);
            await Assert.That(mobile.DatabasePath).IsEqualTo(Path.Combine(fileSystem.AppDataDirectory, FileName));
            await Assert.That((mobile.Store.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) != 0).IsTrue();
            _ = await mobile.Keys.RotateAsync(CancellationToken.None);
            _ = await mobile.Store.RotateEncryptionKeyAsync(CancellationToken.None);
        }

        await using var reopened = await CreateAsync(secureStorage, fileSystem);
        await reopened.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = reopened.Identity.ClientId }, CancellationToken.None);
        await Assert.That(reopened.Identity).IsEqualTo(identity);
        await Assert.That(reopened.Keys.GetKey(originalKey)).IsNotNull();
        await Assert.That(reopened.Keys.GetCurrentKey().KeyId).IsNotEqualTo(originalKey);
    }

    /// <summary>Checks a database cannot silently acquire new keys when secure storage disappears.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ExistingDatabaseWithoutSecureStateFailsClosed()
    {
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        await using (var mobile = await CreateAsync(secureStorage, fileSystem))
        {
            await mobile.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = mobile.Identity.ClientId }, CancellationToken.None);
        }

        secureStorage.RemoveAll();
        await Assert.That((Func<Task>)(() => CreateAsync(secureStorage, fileSystem))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(secureStorage.Writes).IsEqualTo(1);
    }

    /// <summary>Checks file names cannot escape the platform app-data directory.</summary>
    /// <param name="name">The invalid file name.</param>
    /// <returns>The test completion.</returns>
    [Test]
    [Arguments("../outside.db")]
    [Arguments("..\\outside.db")]
    [Arguments("C:\\outside.db")]
    [Arguments("folder/client.db")]
    [Arguments("..")]
    [Arguments(".. ")]
    [Arguments("client.db.")]
    [Arguments("")]
    public async Task InvalidFileNameFailsBeforeSecureProvisioning(string name)
    {
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        Func<Task> create = () => MobileSqliteStorage.CreateAsync(
            secureStorage,
            fileSystem,
            name,
            EntryName,
            new(),
            CancellationToken.None).AsTask();
        await Assert.That(create).Throws<ArgumentException>();
        await Assert.That(secureStorage.Writes).IsEqualTo(0);
        await Assert.That(Directory.Exists(fileSystem.AppDataDirectory)).IsFalse();
    }

    /// <summary>Checks external key providers cannot override the secure composition.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task CallerCannotReplaceTheSecureKeyProvider()
    {
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        var key = await MobileSecureState.OpenAsync(secureStorage, "unrelated", true, CancellationToken.None);
        var options = new SqliteLocalStoreAdapterOptions { KeyProvider = key };
        Func<Task> create = () => MobileSqliteStorage.CreateAsync(
            secureStorage,
            fileSystem,
            FileName,
            EntryName,
            options,
            CancellationToken.None).AsTask();
        await Assert.That(create).ThrowsExactly<ArgumentException>();
        await Assert.That(secureStorage.Writes).IsEqualTo(1);
    }

    /// <summary>Checks a pre-cancelled factory cannot create files or provision state.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task CancelledFactoryDoesNotCreateStateOrDirectory()
    {
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        Func<Task> create = () => MobileSqliteStorage.CreateAsync(
            secureStorage,
            fileSystem,
            FileName,
            EntryName,
            new(),
            cancelled.Token).AsTask();
        await Assert.That(create).Throws<OperationCanceledException>();
        await Assert.That(secureStorage.Writes).IsEqualTo(0);
        await Assert.That(Directory.Exists(fileSystem.AppDataDirectory)).IsFalse();
    }

    /// <summary>Checks the platform must report a real absolute app-data directory before secure provisioning.</summary>
    /// <param name="directory">The invalid platform path.</param>
    /// <returns>The test completion.</returns>
    [Test]
    [Arguments("")]
    [Arguments("relative-mobile-data")]
    public async Task InvalidPlatformAppDataFailsBeforeProvisioning(string directory)
    {
        using var fileSystem = new MobileTestFileSystem { AppDataDirectory = directory };
        var storage = new MobileTestSecureStorage();
        Func<Task> create = async () => { _ = await CreateAsync(storage, fileSystem); };
        await Assert.That(create).ThrowsExactly<ArgumentException>();
        await Assert.That(storage.Writes).IsEqualTo(0);
    }

    /// <summary>Checks a durable payload survives secure key rotation and a real SQLite reopen.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task DurableOutboxSurvivesSecureRotationAndDatabaseReopen()
    {
        const string PayloadText = "mobile-private-outbox-sentinel";
        var stream = new StreamId("mobile-stream");
        using var fileSystem = new MobileTestFileSystem();
        var secureStorage = new MobileTestSecureStorage();
        var payload = CreatePayload(PayloadText);
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = stream,
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = payload,
        };
        SubscriptionId subscription;
        await using (var mobile = await CreateAsync(secureStorage, fileSystem))
        {
            await mobile.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = mobile.Identity.ClientId }, CancellationToken.None);
            subscription = await mobile.Store.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            _ = await mobile.Store.CommitLocalOperationAsync(operation, new(stream, payload, 1), CancellationToken.None);
            _ = await mobile.Keys.RotateAsync(CancellationToken.None);
            _ = await mobile.Store.RotateEncryptionKeyAsync(CancellationToken.None);
        }

        string databasePath;
        await using (var reopened = await CreateAsync(secureStorage, fileSystem))
        {
            await reopened.Store.InitializeAsync(new(StoreName, 1, true) { ClientId = reopened.Identity.ClientId }, CancellationToken.None);
            var recovered = await reopened.Store.RecoverStreamAsync(stream, subscription, CancellationToken.None);
            await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
            await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(Encoding.UTF8.GetString(recovered.PendingOperations[0].Payload.Payload.Span)).IsEqualTo(PayloadText);
            databasePath = reopened.DatabasePath;
        }

        var databaseBytes = await File.ReadAllBytesAsync(databasePath);
        await Assert.That(Encoding.UTF8.GetString(databaseBytes).Contains(PayloadText, StringComparison.Ordinal))
            .IsFalse();
    }

    /// <summary>Creates an actual hashed payload envelope.</summary>
    /// <param name="value">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        return new("mobile-text", 1, "text/plain", bytes, Convert.ToHexString(SHA256.HashData(bytes)));
    }

    /// <summary>Creates the mobile SQLite bundle using only test platform abstractions.</summary>
    /// <param name="storage">The secure storage abstraction.</param>
    /// <param name="fileSystem">The app-data abstraction.</param>
    /// <returns>The composed SQLite bundle.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<MobileSqliteStorage> CreateAsync(MobileTestSecureStorage storage, MobileTestFileSystem fileSystem) =>
        MobileSqliteStorage.CreateAsync(storage, fileSystem, FileName, EntryName, new(), CancellationToken.None).AsTask();
}
