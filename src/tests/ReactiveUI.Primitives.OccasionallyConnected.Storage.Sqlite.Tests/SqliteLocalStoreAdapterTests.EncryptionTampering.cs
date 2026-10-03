// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Encryption at rest tamper, swap, wrong key and missing key tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies a tampered snapshot ciphertext quarantines the stream and fails recovery closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotCiphertextIsTampered_ThenRecoveryQuarantinesTheStream()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        FlipSnapshotPayloadByte(database.Path);

        await AssertRecoveryQuarantinedAsync(database.Path, CreateFirstKeyProvider(), seed.SubscriptionId);
    }

    /// <summary>Verifies an outbox payload copied onto another operation row fails authentication.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOutboxCiphertextIsSwappedBetweenRows_ThenRecoveryQuarantinesTheStream()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        SwapOutboxPayloadRows(database.Path);

        await AssertRecoveryQuarantinedAsync(database.Path, CreateFirstKeyProvider(), seed.SubscriptionId);
    }

    /// <summary>Verifies a changed plaintext column that the associated data binds fails authentication.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenBoundPlaintextColumnIsChanged_ThenRecoveryQuarantinesTheStream()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        ChangePendingOutboxContentType(database.Path);

        await AssertRecoveryQuarantinedAsync(database.Path, CreateFirstKeyProvider(), seed.SubscriptionId);
    }

    /// <summary>Verifies a snapshot cursor moved onto the stream row fails authentication.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCursorCiphertextMovesToAnotherColumn_ThenRecoveryQuarantinesTheStream()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        MoveSnapshotCursorToStream(database.Path);

        await AssertRecoveryQuarantinedAsync(database.Path, CreateFirstKeyProvider(), seed.SubscriptionId);
    }

    /// <summary>Verifies a tampered leased operation is quarantined and never handed out for upload.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLeasedOperationCiphertextIsTampered_ThenLeaseQuarantinesInsteadOfUploading()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        FlipPendingOutboxPayloadByte(database.Path);
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);

        Func<Task> lease = async () => _ = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(lease).ThrowsExactly<LocalStoreRecordAuthenticationException>();
        var quarantine = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(quarantine?.ReasonCode).IsEqualTo(RecordAuthenticationFailedReasonCode);
        await Assert.That(quarantine?.OperationId).IsEqualTo(seed.Second.OperationId);
    }

    /// <summary>Verifies a different key under the same key identifier cannot open the store.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenWrongKeyUsesTheSameKeyId_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using var adapter = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(CreateTestKey(FirstKeyId, WrongKeyFill)));

        Func<Task> action = () => adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies records protected by a key the provider no longer holds are quarantined.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecordKeyIdIsMissing_ThenRecoveryQuarantinesTheStream()
    {
        using var database = TempDatabase.Create();
        var seed = await SeedEncryptedDatabaseAsync(database.Path);
        var first = CreateTestKey(FirstKeyId, FirstKeyFill);
        var second = CreateTestKey(SecondKeyId, SecondKeyFill);
        await using (var rotated = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(second, [first])))
        {
            await rotated.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        }

        await AssertRecoveryQuarantinedAsync(database.Path, new StaticLocalStoreKeyProvider(second), seed.SubscriptionId);
    }

    /// <summary>Creates an encrypted database that holds every protected record type.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The seeded stream.</returns>
    private static async Task<EncryptedSeed> SeedEncryptedDatabaseAsync(string path)
    {
        await using var adapter = CreateEncryptedAdapter(path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        return await SeedEncryptedStreamAsync(adapter);
    }

    /// <summary>Asserts recovery fails closed with a security exception and leaves a durable quarantine marker.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="keyProvider">The key provider.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task AssertRecoveryQuarantinedAsync(string path, ILocalStoreKeyProvider keyProvider, SubscriptionId subscriptionId)
    {
        await using var adapter = CreateEncryptedAdapter(path, keyProvider);
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);

        Func<Task> recover = () => adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<LocalStoreRecordAuthenticationException>();
        var quarantine = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(quarantine?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(quarantine?.ReasonCode).IsEqualTo(RecordAuthenticationFailedReasonCode);
        var guarded = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(guarded.Quarantine).IsNotNull();
        await Assert.That(guarded.Snapshot).IsNull();
        await Assert.That(guarded.PendingOperations.Count).IsEqualTo(0);
        Func<Task> lease = async () => _ = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease).Throws<InvalidOperationException>();
    }
}
