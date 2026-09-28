// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Protected snapshot recovery capture and cursor validation tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The snapshot payload used while setting protected stream cursors.</summary>
    private const string ProtectedCaptureSnapshotPayload = "protected-snapshot";

    /// <summary>A cursor length that exceeds the protected SQL projection bound.</summary>
    private const int OversizedProtectedCaptureCursorLength = 128;

    /// <summary>Verifies a plaintext cursor exceeding the request limit fails before capture materializes it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesOversizedPlaintextCursor_ThenCursorLimitFails()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        const string cursor = "too-long";
        SetStreamServerCursorText(database.Path, cursor);
        var request = CreateCursorLimitedSnapshotRecoveryCaptureRequest(subscriptionId, maximumCursorBytes: 1);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => RequireSnapshotRecoveryCaptureStore(adapter)
            .CaptureSnapshotRecoveryAsync(request, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(capture);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
        await Assert.That(exception?.Observed).IsEqualTo(System.Text.Encoding.UTF8.GetByteCount(cursor));
    }

    /// <summary>Verifies a malformed cursor storage class fails before a snapshot recovery capture is returned.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesBlobCursor_ThenInvalidCursorIsRejected()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(CreatePlainInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "UPDATE oc_streams SET server_cursor = zeroblob(1);";
            _ = await command.ExecuteNonQueryAsync(CancellationToken.None);
        }

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => RequireSnapshotRecoveryCaptureStore(adapter)
            .CaptureSnapshotRecoveryAsync(CreateSnapshotRecoveryCaptureRequest(subscriptionId, 1), CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
    }

    /// <summary>Verifies protected cursor and pending operation metadata survive bounded capture.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenProtectedSnapshotRecoveryCaptureHasPendingOperation_ThenCursorAndMetadataRoundTrip()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateEncryptedOperation(FirstClientSequence, "protected-capture");
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        const string cursor = "ok";
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, cursor, [CreateRemoteEvent(cursor)]),
            new(Stream, CreatePayload(ProtectedCaptureSnapshotPayload), FormatVersion: 1, ExpectedRevision: 1),
            CancellationToken.None);

        var capture = await RequireSnapshotRecoveryCaptureStore(adapter).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, 1),
            CancellationToken.None);

        await Assert.That(capture.ServerCursor).IsEqualTo(cursor);
        await Assert.That(capture.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(capture.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(capture.PendingOperations[0].Metadata[MetadataOriginKey]).IsEqualTo(EncryptedMetadataSentinel);
    }

    /// <summary>Verifies protected cursor length is enforced after authentication and decryption.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenProtectedSnapshotRecoveryCaptureCursorExceedsPlaintextLimit_ThenCaptureRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        const string cursor = "ok";
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, cursor, [CreateRemoteEvent(cursor)]),
            new(Stream, CreatePayload(ProtectedCaptureSnapshotPayload), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        var request = CreateCursorLimitedSnapshotRecoveryCaptureRequest(subscriptionId, maximumCursorBytes: 1);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => RequireSnapshotRecoveryCaptureStore(adapter)
            .CaptureSnapshotRecoveryAsync(request, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(capture);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
    }

    /// <summary>Verifies a protected cursor larger than the bounded SQL projection fails before decryption.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenProtectedSnapshotRecoveryCaptureCursorExceedsStoredLimit_ThenCaptureRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var cursor = new string('x', OversizedProtectedCaptureCursorLength);
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, cursor, [CreateRemoteEvent(cursor)]),
            new(Stream, CreatePayload(ProtectedCaptureSnapshotPayload), FormatVersion: 1, ExpectedRevision: 0),
            CancellationToken.None);
        var request = CreateCursorLimitedSnapshotRecoveryCaptureRequest(subscriptionId, maximumCursorBytes: 1);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => RequireSnapshotRecoveryCaptureStore(adapter)
            .CaptureSnapshotRecoveryAsync(request, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(capture);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
        await Assert.That(exception?.Observed ?? 0).IsGreaterThan(1);
    }

    /// <summary>Creates a request that bounds cursor bytes while admitting ordinary snapshot content.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="maximumCursorBytes">The maximum cursor byte count.</param>
    /// <returns>The bounded request.</returns>
    private static LocalSnapshotRecoveryCaptureRequest CreateCursorLimitedSnapshotRecoveryCaptureRequest(
        SubscriptionId subscriptionId,
        int maximumCursorBytes) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            Limits = new() { MaximumPendingOperations = 1, MaximumCursorUtf8Bytes = maximumCursorBytes, MaximumLogicalBytes = SnapshotRecoveryCaptureLargeLogicalBytes },
        };
}
