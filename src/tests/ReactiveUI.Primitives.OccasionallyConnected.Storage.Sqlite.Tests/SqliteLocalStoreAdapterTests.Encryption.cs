// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Encryption at rest capability, round trip, and on-disk confidentiality tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies a key provider makes the adapter advertise and satisfy authenticated encryption at rest.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenKeyProviderIsConfigured_ThenAdapterAdvertisesAndSatisfiesEncryptionAtRest()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());

        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);

        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.DurableLocalCommit) != 0).IsTrue();
    }

    /// <summary>Verifies every protected record type round-trips through a reopened encrypted store.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsEncrypted_ThenEveryProtectedRecordRoundTrips()
    {
        using var database = TempDatabase.Create();
        EncryptedSeed seed;
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            seed = await SeedEncryptedStreamAsync(adapter);
            await AssertEncryptedSeedAsync(adapter, seed);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        await AssertEncryptedSeedAsync(reopened, seed);
        var lease = await ReadSingleLeaseAsync(reopened, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(seed.Second.OperationId);
        await Assert.That(lease.Operations[0].BaseVersion).IsEqualTo(EncryptedBaseVersionSentinel);
        await Assert.That(ReadPayloadText(lease.Operations[0].Payload)).IsEqualTo(EncryptedSecondPayloadText);
    }

    /// <summary>Verifies the database and write-ahead log never hold a plaintext sentinel.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsEncrypted_ThenDatabaseFilesHoldNoPlaintextSentinel()
    {
        using var database = TempDatabase.Create();
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            _ = await SeedEncryptedStreamAsync(adapter);
            await Assert.That(FindPlaintextSentinels(database.Path)).IsEmpty();
        }

        await Assert.That(FindPlaintextSentinels(database.Path)).IsEmpty();
    }

    /// <summary>Verifies the same records are stored in plaintext without a key provider, proving the sentinel check can fail.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsPlaintext_ThenSentinelCheckFindsPlaintext()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);

        _ = await SeedEncryptedStreamAsync(adapter);

        await Assert.That(FindPlaintextSentinels(database.Path)).IsNotEmpty();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) != 0).IsFalse();
    }

    /// <summary>Verifies an encrypted database opened without a key provider fails closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptedStoreIsOpenedWithoutKeyProvider_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            _ = await SeedEncryptedStreamAsync(adapter);
        }

        await using var plaintext = CreateAdapter(database.Path);
        Func<Task> action = () => plaintext.InitializeAsync(CreatePlainInitialization(), CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a missing protection marker cannot turn encrypted V1 rows into plaintext.</summary>
    /// <param name="withKey">Whether the reopening adapter has the original key.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task WhenProtectedVersionOneMarkerIsDeleted_ThenReopenFailsWithoutRewrite(bool withKey)
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        var originalKeyId = ReadPendingPayloadKeyId(database.Path);
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("DELETE FROM oc_metadata WHERE key = 'rxui.localstore.record_protection';");
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        var metadataBefore = ReadProtectedMetadata(database.Path);
        await Assert.That(metadataBefore.Contains("rxui.localstore.record_protection_check", StringComparison.Ordinal)).IsTrue();
        await Assert.That(metadataBefore.Contains("rxui.localstore.operation_state_manifest", StringComparison.Ordinal)).IsTrue();

        await using var reopened = withKey
            ? CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider())
            : CreateAdapter(database.Path);
        Func<Task> initialize = () => reopened.InitializeAsync(
            withKey ? CreateEncryptedInitialization() : CreatePlainInitialization(),
            CancellationToken.None).AsTask();
        await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadProtectionMarkerCount(database.Path)).IsEqualTo(0L);
        await Assert.That(ReadProtectedMetadata(database.Path)).IsEqualTo(metadataBefore);
        await Assert.That(ReadPendingPayloadKeyId(database.Path)).IsEqualTo(originalKeyId);
    }

    /// <summary>Verifies rotation needs a protected store.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsPlaintext_ThenRotateEncryptionKeyIsRejected()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);

        Func<Task> action = () => adapter.RotateEncryptionKeyAsync(CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Reads protected metadata in key order for the no-rewrite assertion.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The metadata rows.</returns>
    private static string ReadProtectedMetadata(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateStatement();
        command.SetSql("""
            SELECT group_concat(key || '=' || quote(value), ';')
            FROM (SELECT key, value FROM oc_metadata WHERE key LIKE 'rxui.localstore.%' ORDER BY key);
            """);
        return command.Scalar() as string ?? string.Empty;
    }
}
