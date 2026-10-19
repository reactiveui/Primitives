// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Encrypted payload hash validation during recovery.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies canonical payload hashes survive encrypted persistence and recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptedPayloadUsesCanonicalHash_ThenRecoveryValidatesAndReturnsIt()
    {
        using var database = TempDatabase.Create();
        var payload = CreateCanonicalEncryptedPayload("canonical encrypted payload");
        SubscriptionId subscriptionId;
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var operation = CreateEncryptedOperation(FirstClientSequence, "canonical encrypted payload") with { Payload = payload };
            _ = await adapter.CommitLocalOperationAsync(operation, new(Stream, payload, 1, 0), CancellationToken.None);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovered.Snapshot?.State.PayloadHash).IsEqualTo(payload.PayloadHash);
        await Assert.That(recovered.PendingOperations[0].Payload.PayloadHash).IsEqualTo(payload.PayloadHash);
        await Assert.That(recovered.Quarantine).IsNull();
    }

    /// <summary>Verifies an authenticated but malformed snapshot hash quarantines the stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenEncryptedSnapshotHashHasMalformedCanonicalValue_ThenRecoveryQuarantinesTheStream() =>
        AssertProtectedSnapshotHashIsRejectedAsync("sha256-invalid");

    /// <summary>Verifies an authenticated but mismatched snapshot hash quarantines the stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenEncryptedSnapshotHashDoesNotMatchPayload_ThenRecoveryQuarantinesTheStream() =>
        AssertProtectedSnapshotHashIsRejectedAsync(CanonicalEncryptedPayloadHash("different payload"u8.ToArray()));

    /// <summary>Verifies a refused protected cursor update rolls back the remote apply.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqliteIgnoresEncryptedCursorUpdate_ThenRemoteApplyRollsBack()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateEncryptedOperation(FirstClientSequence, "operation"),
            new(Stream, CreatePayload("initial"), 1, 0),
            CancellationToken.None);
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("""
                CREATE TRIGGER ignore_encrypted_cursor_update
                BEFORE UPDATE OF server_cursor ON oc_streams
                BEGIN
                    SELECT RAISE(IGNORE);
                END;
                """);
            _ = command.Execute();
        }

        var remote = CreateRemoteEvent("next-cursor");
        Func<Task> apply = () => adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, "next-cursor", [remote]),
            new(Stream, CreatePayload("remote snapshot"), 1, 1),
            CancellationToken.None).AsTask();

        await Assert.That(apply).ThrowsExactly<InvalidOperationException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(Stream, [remote.EventId], CancellationToken.None);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Recovers a stream after changing only its authenticated stored snapshot hash.</summary>
    /// <param name="replacementHash">The decrypted replacement hash.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task AssertProtectedSnapshotHashIsRejectedAsync(string replacementHash)
    {
        using var database = TempDatabase.Create();
        var payload = CreateCanonicalEncryptedPayload("original payload");
        SubscriptionId subscriptionId;
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(
                CreateEncryptedOperation(FirstClientSequence, "operation"),
                new(Stream, payload, 1, 0),
                CancellationToken.None);
        }

        var context = SqliteRecordContext.Snapshot(Stream, 1, 1)
            .WithPayloadMetadata(payload.ContractId, payload.SchemaVersion, payload.ContentType);
        var cipher = new SqliteRecordCipher(SqliteRecordProtection.Create(CreateFirstKeyProvider()), StoreIdentity);
        var protectedHash = cipher.ProtectText(replacementHash, context, SqliteRecordContext.PayloadHashColumn);
        using (var connection = OpenRawConnection(database.Path))
        using (var command = connection.CreateStatement())
        {
            command.SetSql("UPDATE oc_snapshots SET payload_hash = $hash WHERE stream_id = $streamId;");
            _ = command.Bind("$hash", protectedHash);
            _ = command.Bind("$streamId", Stream.Value);
            await Assert.That(command.Execute()).IsEqualTo(1);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var quarantine = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(quarantine?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(quarantine?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
    }

    /// <summary>Creates a payload with a canonical SHA-256 hash.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreateCanonicalEncryptedPayload(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return new("reading", 1, "application/json", bytes, CanonicalEncryptedPayloadHash(bytes));
    }

    /// <summary>Hashes payload bytes in the canonical format.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <returns>The canonical hash.</returns>
    private static string CanonicalEncryptedPayloadHash(byte[] bytes) =>
        $"sha256-{Convert.ToBase64String(SHA256.HashData(bytes))}";
}
