// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Shared helpers for encryption at rest tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The payload sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedPayloadSentinel = "SENTINEL-PAYLOAD-7f3a91";

    /// <summary>The snapshot sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedSnapshotSentinel = "SENTINEL-SNAPSHOT-7f3a91";

    /// <summary>The authoritative snapshot sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedAuthoritativeSentinel = "SENTINEL-AUTHORITATIVE-7f3a91";

    /// <summary>The cursor sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedCursorSentinel = "SENTINEL-CURSOR-7f3a91";

    /// <summary>The base version sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedBaseVersionSentinel = "SENTINEL-BASE-7f3a91";

    /// <summary>The metadata value sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedMetadataSentinel = "SENTINEL-METADATA-7f3a91";

    /// <summary>The dead-letter reason sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedReasonSentinel = "SENTINEL-REASON-7f3a91";

    /// <summary>The quarantine evidence sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedQuarantineSentinel = "SENTINEL-QUARANTINE-7f3a91";

    /// <summary>The quarantine cursor sentinel that must never appear on disk in plaintext.</summary>
    private const string EncryptedQuarantineCursorSentinel = "SENTINEL-QCURSOR-7f3a91";

    /// <summary>The second operation payload text.</summary>
    private const string EncryptedSecondPayloadText = "second-operation";

    /// <summary>The quarantine reason code for records that fail authentication.</summary>
    private const string RecordAuthenticationFailedReasonCode = "sqlite-record-authentication-failed";

    /// <summary>The first key identifier.</summary>
    private const string FirstKeyId = "key-1";

    /// <summary>The second key identifier.</summary>
    private const string SecondKeyId = "key-2";

    /// <summary>The fill byte of the first key.</summary>
    private const byte FirstKeyFill = 0x11;

    /// <summary>The fill byte of the second key.</summary>
    private const byte SecondKeyFill = 0x22;

    /// <summary>The fill byte of a wrong key that reuses the first key identifier.</summary>
    private const byte WrongKeyFill = 0x33;

    /// <summary>The key length used by tests.</summary>
    private const int TestKeyBytes = 32;

    /// <summary>The snapshot revision after the seeded stream finishes.</summary>
    private const long EncryptedSeededRevision = 4;

    /// <summary>The revision after the seeded remote apply.</summary>
    private const long EncryptedRemoteAppliedRevision = 2;

    /// <summary>The revision after the second seeded commit.</summary>
    private const long EncryptedSecondCommitRevision = 3;

    /// <summary>The stream used for quarantine evidence.</summary>
    private static readonly StreamId EncryptedQuarantineStream = new("sensor/quarantined");

    /// <summary>The observed timestamp used by encryption tests.</summary>
    private static readonly DateTimeOffset EncryptedObservedAtUtc = new(2026, 3, 4, 5, 6, 7, TimeSpan.Zero);

    /// <summary>Gets every plaintext sentinel written by the seeded stream.</summary>
    private static string[] EncryptedSentinels =>
    [
        EncryptedPayloadSentinel,
        EncryptedSnapshotSentinel,
        EncryptedAuthoritativeSentinel,
        EncryptedCursorSentinel,
        EncryptedBaseVersionSentinel,
        EncryptedMetadataSentinel,
        EncryptedReasonSentinel,
        EncryptedQuarantineSentinel,
        EncryptedQuarantineCursorSentinel,
    ];

    /// <summary>Creates a deterministic test key.</summary>
    /// <param name="keyId">The key identifier.</param>
    /// <param name="fill">The byte used for every key byte.</param>
    /// <returns>The key.</returns>
    private static LocalStoreKey CreateTestKey(string keyId, byte fill)
    {
        var material = new byte[TestKeyBytes];
        Array.Fill(material, fill);
        return new(keyId, material);
    }

    /// <summary>Creates a provider with only the first key.</summary>
    /// <returns>The key provider.</returns>
    private static StaticLocalStoreKeyProvider CreateFirstKeyProvider() => new(CreateTestKey(FirstKeyId, FirstKeyFill));

    /// <summary>Creates an adapter that encrypts records at rest.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="keyProvider">The key provider.</param>
    /// <returns>The adapter.</returns>
    private static SqliteLocalStoreAdapter CreateEncryptedAdapter(string path, ILocalStoreKeyProvider keyProvider) =>
        new(path, new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = NormalWorkerBytes, KeyProvider = keyProvider });

    /// <summary>Creates an adapter that encrypts records at rest and reports write checkpoints.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="keyProvider">The key provider.</param>
    /// <param name="faultPoint">The checkpoint observer.</param>
    /// <returns>The adapter.</returns>
    private static SqliteLocalStoreAdapter CreateEncryptedAdapter(string path, ILocalStoreKeyProvider keyProvider, ISqliteCommitFaultPoint faultPoint) =>
        new(path, new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = NormalWorkerBytes, KeyProvider = keyProvider }, faultPoint);

    /// <summary>Creates the initialization that requires encryption at rest.</summary>
    /// <returns>The initialization.</returns>
    private static LocalStoreInitialization CreateEncryptedInitialization() => new(StoreIdentity, SchemaVersion, true);

    /// <summary>Creates the initialization that does not require encryption at rest.</summary>
    /// <returns>The initialization.</returns>
    private static LocalStoreInitialization CreatePlainInitialization() => new(StoreIdentity, SchemaVersion, false);

    /// <summary>Creates an operation whose protected fields hold sentinels.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateEncryptedOperation(long clientSequence, string payloadText) => CreateOperation(clientSequence) with
    {
        BaseVersion = EncryptedBaseVersionSentinel,
        Payload = CreatePayload(payloadText),
        Metadata = new Dictionary<string, string> { [MetadataOriginKey] = EncryptedMetadataSentinel },
    };

    /// <summary>Writes every protected record type through the adapter.</summary>
    /// <param name="adapter">The initialized adapter.</param>
    /// <returns>The seeded stream.</returns>
    private static async Task<EncryptedSeed> SeedEncryptedStreamAsync(SqliteLocalStoreAdapter adapter)
    {
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = CreateEncryptedOperation(FirstClientSequence, EncryptedPayloadSentinel);
        var initial = new SnapshotMutation(Stream, CreatePayload(EncryptedSnapshotSentinel), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreatePayload(EncryptedAuthoritativeSentinel),
        };
        _ = await adapter.CommitLocalOperationAsync(first, initial, CancellationToken.None);
        var remote = new RemoteEvent(Guid.NewGuid(), Stream, EncryptedCursorSentinel, DateTimeOffset.UnixEpoch, null, CreatePayload("remote"), new Dictionary<string, string>());
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, EncryptedCursorSentinel, [remote]),
            new(Stream, CreatePayload(EncryptedSnapshotSentinel), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);
        var second = CreateEncryptedOperation(SecondClientSequence, EncryptedSecondPayloadText);
        _ = await adapter.CommitLocalOperationAsync(
            second,
            new(Stream, CreatePayload(EncryptedSnapshotSentinel), FormatVersion: 1, EncryptedRemoteAppliedRevision),
            CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        _ = await adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            first.OperationId,
            EncryptedReasonSentinel,
            new(Stream, CreatePayload(EncryptedSnapshotSentinel), FormatVersion: 1, EncryptedSecondCommitRevision),
            CancellationToken.None);
        await SeedEncryptedQuarantineAsync(adapter);
        return new(subscriptionId, first, second);
    }

    /// <summary>Writes a quarantine marker whose evidence holds sentinels.</summary>
    /// <param name="adapter">The initialized adapter.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task SeedEncryptedQuarantineAsync(SqliteLocalStoreAdapter adapter)
    {
        _ = await adapter.GetOrCreateSubscriptionIdAsync(EncryptedQuarantineStream, null, CancellationToken.None);
        _ = await adapter.QuarantinePayloadAsync(
            new()
            {
                StreamId = EncryptedQuarantineStream,
                EventId = Guid.NewGuid(),
                Source = LocalPayloadQuarantineSource.RemoteEvent,
                Reason = LocalPayloadQuarantineReason.SchemaRejected,
                ReasonCode = "schema-rejected",
                Envelope = CreatePayload(EncryptedQuarantineSentinel),
                Cursor = EncryptedQuarantineCursorSentinel,
                ObservedAtUtc = EncryptedObservedAtUtc,
            },
            CancellationToken.None);
    }

    /// <summary>Asserts every protected record of the seeded stream reads back unchanged.</summary>
    /// <param name="adapter">The initialized adapter.</param>
    /// <param name="seed">The seeded stream.</param>
    /// <returns>The asynchronous task.</returns>
    private static async Task AssertEncryptedSeedAsync(SqliteLocalStoreAdapter adapter, EncryptedSeed seed)
    {
        var recovered = await adapter.RecoverStreamAsync(Stream, seed.SubscriptionId, CancellationToken.None);
        await Assert.That(recovered.Quarantine).IsNull();
        await Assert.That(recovered.ServerCursor).IsEqualTo(EncryptedCursorSentinel);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(EncryptedSeededRevision);
        await Assert.That(recovered.Snapshot?.ServerCursor).IsEqualTo(EncryptedCursorSentinel);
        await Assert.That(ReadPayloadText(recovered.Snapshot?.State)).IsEqualTo(EncryptedSnapshotSentinel);
        await Assert.That(ReadPayloadText(recovered.Snapshot?.AuthoritativeState)).IsEqualTo(EncryptedAuthoritativeSentinel);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        var pending = recovered.PendingOperations[0];
        await Assert.That(pending.OperationId).IsEqualTo(seed.Second.OperationId);
        await Assert.That(pending.BaseVersion).IsEqualTo(EncryptedBaseVersionSentinel);
        await Assert.That(pending.Metadata[MetadataOriginKey]).IsEqualTo(EncryptedMetadataSentinel);
        await Assert.That(ReadPayloadText(pending.Payload)).IsEqualTo(EncryptedSecondPayloadText);
        await Assert.That(pending.Payload.PayloadHash).IsEqualTo(seed.Second.Payload.PayloadHash);
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(1);
        await Assert.That(recovered.DeadLetters[0].ReasonCode).IsEqualTo(EncryptedReasonSentinel);
        await Assert.That(ReadPayloadText(recovered.DeadLetters[0].Operation.Payload)).IsEqualTo(EncryptedPayloadSentinel);
        var status = await adapter.GetOperationStatusAsync(seed.First.OperationId, CancellationToken.None);
        await Assert.That(status?.ReasonCode).IsEqualTo(EncryptedReasonSentinel);
        var unapplied = await adapter.GetUnappliedEventIdsAsync(Stream, [Guid.NewGuid()], CancellationToken.None);
        await Assert.That(unapplied.Count).IsEqualTo(1);
        var quarantine = await adapter.GetPayloadQuarantineAsync(EncryptedQuarantineStream, CancellationToken.None);
        await Assert.That(quarantine?.Cursor).IsEqualTo(EncryptedQuarantineCursorSentinel);
        await Assert.That(quarantine?.ReasonCode).IsEqualTo("schema-rejected");
        await Assert.That(Encoding.UTF8.GetString(quarantine!.Evidence.PayloadPrefix.Span)).IsEqualTo(EncryptedQuarantineSentinel);
    }

    /// <summary>Reads a payload as UTF-8 text.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The text, or null.</returns>
    private static string? ReadPayloadText(PayloadEnvelope? payload) =>
        payload is null ? null : Encoding.UTF8.GetString(payload.Payload.Span);

    /// <summary>Reads the database and its write-ahead log as one byte array.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The raw bytes of every database file.</returns>
    private static byte[] ReadRawDatabaseFiles(string path)
    {
        using var buffer = new MemoryStream();
        foreach (var file in new[] { path, $"{path}-wal", $"{path}-journal" })
        {
            if (!File.Exists(file))
            {
                continue;
            }

            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            stream.CopyTo(buffer);
        }

        return buffer.ToArray();
    }

    /// <summary>Returns the sentinels found in the raw database files.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The sentinels that appear in plaintext.</returns>
    private static List<string> FindPlaintextSentinels(string path)
    {
        var raw = ReadRawDatabaseFiles(path);
        List<string> found = [];
        foreach (var sentinel in EncryptedSentinels)
        {
            if (raw.AsSpan().IndexOf(Encoding.UTF8.GetBytes(sentinel)) >= 0)
            {
                found.Add(sentinel);
            }
        }

        return found;
    }

    /// <summary>Copies the first outbox payload and hash onto the second outbox row.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SwapOutboxPayloadRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload = (SELECT payload FROM oc_outbox WHERE client_sequence = 1),
                payload_hash = (SELECT payload_hash FROM oc_outbox WHERE client_sequence = 1)
            WHERE client_sequence = 2;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Changes the plaintext content type of the pending outbox row.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void ChangePendingOutboxContentType(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET payload_content_type = 'text/plain' WHERE client_sequence = 2;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Copies the snapshot cursor ciphertext onto the stream row.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void MoveSnapshotCursorToStream(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_streams
            SET server_cursor = (SELECT server_cursor FROM oc_snapshots WHERE stream_id = 'sensor/temperature')
            WHERE stream_id = 'sensor/temperature';
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Flips the last byte of the snapshot payload ciphertext.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void FlipSnapshotPayloadByte(string path)
    {
        using var connection = OpenRawConnection(path);
        byte[] value;
        using (var read = connection.CreateCommand())
        {
            read.CommandText = "SELECT payload FROM oc_snapshots WHERE stream_id = 'sensor/temperature';";
            value = FlipLastByte(read.ExecuteScalar());
        }

        using var write = connection.CreateCommand();
        write.CommandText = "UPDATE oc_snapshots SET payload = $value WHERE stream_id = 'sensor/temperature';";
        _ = write.Parameters.AddWithValue("$value", value);
        _ = write.ExecuteNonQuery();
    }

    /// <summary>Flips the last byte of the pending outbox payload ciphertext.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void FlipPendingOutboxPayloadByte(string path)
    {
        using var connection = OpenRawConnection(path);
        byte[] value;
        using (var read = connection.CreateCommand())
        {
            read.CommandText = "SELECT payload FROM oc_outbox WHERE client_sequence = 2;";
            value = FlipLastByte(read.ExecuteScalar());
        }

        using var write = connection.CreateCommand();
        write.CommandText = "UPDATE oc_outbox SET payload = $value WHERE client_sequence = 2;";
        _ = write.Parameters.AddWithValue("$value", value);
        _ = write.ExecuteNonQuery();
    }

    /// <summary>Returns a copy of a BLOB value with its last byte flipped.</summary>
    /// <param name="value">The stored BLOB.</param>
    /// <returns>The tampered bytes.</returns>
    private static byte[] FlipLastByte(object? value)
    {
        var bytes = (byte[])value!;
        bytes[^1] ^= 0x01;
        return bytes;
    }

    /// <summary>Reads the key identifier of the pending outbox payload envelope.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The key identifier.</returns>
    private static string ReadPendingPayloadKeyId(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT payload FROM oc_outbox WHERE client_sequence = 2;";
        return ReadEnvelopeKeyId(command.ExecuteScalar());
    }

    /// <summary>Reads the key identifier of the stream cursor envelope.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The key identifier.</returns>
    private static string ReadStreamCursorKeyId(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT server_cursor FROM oc_streams WHERE stream_id = 'sensor/temperature';";
        return ReadEnvelopeKeyId(command.ExecuteScalar());
    }

    /// <summary>Counts the record protection markers.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The marker count.</returns>
    private static object? ReadProtectionMarkerCount(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_metadata WHERE key = 'rxui.localstore.record_protection';";
        return command.ExecuteScalar();
    }

    /// <summary>Reads the key identifier from a stored envelope.</summary>
    /// <param name="value">The stored BLOB or Base64 text.</param>
    /// <returns>The key identifier.</returns>
    private static string ReadEnvelopeKeyId(object? value)
    {
        const int KeyIdOffset = 2;
        var envelope = value is string text ? Convert.FromBase64String(text) : (byte[])value!;
        return Encoding.ASCII.GetString(envelope, KeyIdOffset, envelope[1]);
    }

    /// <summary>Throws at one checkpoint to simulate a crash.</summary>
    /// <param name="target">The checkpoint that throws.</param>
    private sealed class ThrowingCommitFaultPoint(SqliteCommitCheckpoint target) : ISqliteCommitFaultPoint
    {
        /// <inheritdoc/>
        public void Reached(SqliteCommitCheckpoint checkpoint)
        {
            if (checkpoint == target)
            {
                throw new IOException($"Simulated crash at {checkpoint}.");
            }
        }
    }

    /// <summary>Holds the records written by <see cref="SeedEncryptedStreamAsync"/>.</summary>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    /// <param name="First">The dead-lettered operation.</param>
    /// <param name="Second">The pending operation.</param>
    private sealed record EncryptedSeed(SubscriptionId SubscriptionId, SyncOperation First, SyncOperation Second);
}
