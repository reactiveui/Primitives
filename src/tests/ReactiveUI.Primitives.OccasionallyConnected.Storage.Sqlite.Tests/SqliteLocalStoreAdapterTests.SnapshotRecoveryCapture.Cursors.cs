// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Snapshot recovery capture cursor and identity tests.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>A server cursor used to prove non-null cursor capture.</summary>
    private const string SnapshotRecoveryCaptureServerCursor = "capture-server-cursor";

    /// <summary>A length-matched subscription identifier that cannot parse as a Guid.</summary>
    private const string InvalidLengthMatchedSnapshotRecoverySubscriptionText = "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz";

    /// <summary>A canonical empty subscription identifier text.</summary>
    private static readonly string EmptySnapshotRecoveryCaptureSubscriptionText = Guid.Empty.ToString("D");

    /// <summary>Verifies matching non-null stream and snapshot cursors are preserved in a bounded capture.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesMatchingServerCursor_ThenCapturePreservesCursorAndSnapshot()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetStreamServerCursorText(database.Path, SnapshotRecoveryCaptureServerCursor);
        SetSnapshotServerCursor(database.Path, SnapshotRecoveryCaptureServerCursor);

        var capture = await RequireSnapshotRecoveryCaptureStore(adapter).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);

        await Assert.That(capture.ServerCursor).IsEqualTo(SnapshotRecoveryCaptureServerCursor);
        await Assert.That(capture.Snapshot?.ServerCursor).IsEqualTo(SnapshotRecoveryCaptureServerCursor);
        await Assert.That(capture.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(capture.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies empty top-level subscription identity text reaches Guid validation and fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesEmptySubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetSubscriptionIdentityText(database.Path, EmptySnapshotRecoveryCaptureSubscriptionText);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadSubscriptionIdentityText(database.Path)).IsEqualTo(EmptySnapshotRecoveryCaptureSubscriptionText);
    }

    /// <summary>Verifies length-matched invalid top-level subscription identity text reaches Guid validation and fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesLengthMatchedInvalidSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetSubscriptionIdentityText(database.Path, InvalidLengthMatchedSnapshotRecoverySubscriptionText);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadSubscriptionIdentityText(database.Path)).IsEqualTo(InvalidLengthMatchedSnapshotRecoverySubscriptionText);
    }

    /// <summary>Verifies empty stream subscription identity text reaches projected Guid validation and fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesEmptyStreamSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetStreamSubscriptionIdText(database.Path, EmptySnapshotRecoveryCaptureSubscriptionText);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadStreamSubscriptionIdentityText(database.Path)).IsEqualTo(EmptySnapshotRecoveryCaptureSubscriptionText);
    }

    /// <summary>Verifies length-matched invalid stream subscription identity text reaches projected Guid validation and fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesLengthMatchedInvalidStreamSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetStreamSubscriptionIdText(database.Path, InvalidLengthMatchedSnapshotRecoverySubscriptionText);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadStreamSubscriptionIdentityText(database.Path)).IsEqualTo(InvalidLengthMatchedSnapshotRecoverySubscriptionText);
    }

    /// <summary>Sets the stream cursor to text storage.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="cursor">The server cursor.</param>
    private static void SetStreamServerCursorText(string path, string cursor)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_streams SET server_cursor = $serverCursor;";
        _ = command.Parameters.AddWithValue("$serverCursor", cursor);
        _ = command.ExecuteNonQuery();
    }
}
