// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Bounded snapshot recovery capture tests for the SQLite local store adapter.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The logical byte width counted for Int32 and enum values.</summary>
    private const long SnapshotCaptureInt32Bytes = 4;

    /// <summary>The logical byte width counted for Int64 values.</summary>
    private const long SnapshotCaptureInt64Bytes = 8;

    /// <summary>The logical byte width counted for Guid values.</summary>
    private const long SnapshotCaptureGuidBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset values.</summary>
    private const long SnapshotCaptureDateTimeOffsetBytes = 16;

    /// <summary>The large logical byte limit used to inspect bounded capture contents.</summary>
    private const long SnapshotRecoveryCaptureLargeLogicalBytes = 1024L * 1024L;

    /// <summary>The independent golden logical byte total for the one-operation SQLite byte-boundary fixture.</summary>
    private const long SingleOperationCaptureGoldenLogicalBytes = 300;

    /// <summary>The independent golden logical byte total after adding the second SQLite operation row.</summary>
    private const long TwoOperationCaptureGoldenLogicalBytes = 455;

    /// <summary>The payload limit that admits the snapshot row and rejects the corrupted operation row.</summary>
    private const int SnapshotPayloadBoundaryBytes = 8;

    /// <summary>The raw outbox evidence count column index.</summary>
    private const int OutboxEvidenceCountIndex = 0;

    /// <summary>The raw outbox evidence operation identifier column index.</summary>
    private const int OutboxEvidenceOperationIdIndex = 1;

    /// <summary>The raw outbox evidence operation type storage column index.</summary>
    private const int OutboxEvidenceOperationTypeIndex = 2;

    /// <summary>The snapshot cursor evidence operation count column index.</summary>
    private const int SnapshotCursorEvidenceOperationCountIndex = 2;

    /// <summary>The blob length used for scalar storage corruption.</summary>
    private const int CorruptScalarBlobLength = 4;

    /// <summary>The operation identifier text that exceeds the canonical Guid text length.</summary>
    private const string OverlongOperationIdText = "operation-id-with-more-than-thirty-six-characters";

    /// <summary>Verifies bounded SQLite capture reads real pending/replay identities while preserving startup recovery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureRuns_ThenPendingReplayIdentitiesAndStartupRecoveryArePreserved()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var committed = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);

        var capture = await RequireSnapshotRecoveryCaptureStore(adapter).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(capture.StreamId).IsEqualTo(Stream);
        await Assert.That(capture.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(capture.Snapshot?.Revision ?? 0).IsEqualTo(FirstClientSequence);
        await Assert.That(capture.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(capture.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(capture.PendingOperations[0].OperationId).IsEqualTo(committed.OperationId);
        await Assert.That(capture.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(capture.ReplayOperations[0].OperationId).IsEqualTo(committed.OperationId);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(committed.OperationId);
    }

    /// <summary>Verifies SQLite capture returns an empty view when only the subscription identity remains.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureHasNoStreamRow_ThenEmptyCaptureIsReturned()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        DeleteStreamRows(database.Path);

        var capture = await RequireSnapshotRecoveryCaptureStore(adapter).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);

        await Assert.That(capture.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(capture.Snapshot).IsNull();
        await Assert.That(capture.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(capture.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(capture.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies SQLite capture includes authoritative snapshot state in its owned result.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureHasAuthoritativeState_ThenCaptureOwnsAuthoritativePayload()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(0) with { AuthoritativeState = CreatePayload("capture-authoritative") },
            CancellationToken.None);

        var capture = await RequireSnapshotRecoveryCaptureStore(adapter).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);

        await Assert.That(capture.Snapshot?.AuthoritativeState).IsNotNull();
        await Assert.That(System.Text.Encoding.UTF8.GetString(capture.Snapshot!.AuthoritativeState!.Payload.Span)).IsEqualTo("capture-authoritative");
        await Assert.That(GetCaptureLogicalBytes(capture)).IsGreaterThan(SingleOperationCaptureGoldenLogicalBytes);
    }

    /// <summary>Verifies SQLite capture rejects a mismatched request subscription without mutating durable state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSubscriptionDiffers_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(SubscriptionId.New(), maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies SQLite capture rejects quarantined streams before materializing payload rows.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesQuarantine_ThenPreflightRejectsBeforeDecode()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNotNull();
    }

    /// <summary>Verifies SQLite pending count limits accept the exact bound and reject overflow without mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureExceedsPendingCount_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var first = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var second = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(FirstClientSequence), CancellationToken.None);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        var exact = await captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: TwoPendingOperations),
            CancellationToken.None);
        Func<Task<LocalSnapshotRecoveryCapture>> overflow = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(overflow);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exact.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
        await Assert.That(exception?.Observed).IsEqualTo(TwoPendingOperations);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies replay-only operations are included in the bounded capture count.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureHasReplayOnlyOverflow_ThenSqliteCountsReplayUnionBeforeMaterializing()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(FirstClientSequence), CancellationToken.None);
        SetOutboxOperationsReplayOnly(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> overflow = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(overflow);

        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
        await Assert.That(exception?.Observed).IsEqualTo(TwoPendingOperations);
    }

    /// <summary>Verifies SQLite logical byte limits accept the exact bound and reject one byte below without mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureExceedsLogicalBytes_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);
        var measured = await captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);
        var exactLogicalBytes = GetCaptureLogicalBytes(measured);

        await Assert.That(exactLogicalBytes).IsEqualTo(SingleOperationCaptureGoldenLogicalBytes);
        var exact = await captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1, maximumLogicalBytes: exactLogicalBytes),
            CancellationToken.None);
        Func<Task<LocalSnapshotRecoveryCapture>> below = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1, maximumLogicalBytes: exactLogicalBytes - 1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(below);
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exact.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        await Assert.That(exception?.Maximum).IsEqualTo(exactLogicalBytes - 1);
        await Assert.That(exception?.Observed).IsEqualTo(exactLogicalBytes);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies aggregate logical byte capacity fails before a later schema-zero corrupt payload is decoded.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureLogicalBytesOverflowBeforeLaterSchemaZeroPayload_ThenCapacityFailsBeforeDecode()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(FirstClientSequence), CancellationToken.None);
        SetOutboxPayloadSchemaZeroAndBytesInvalidForSequence(database.Path, SecondClientSequence);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> overflow = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(
                subscriptionId,
                maximumPendingOperations: TwoPendingOperations,
                maximumLogicalBytes: SingleOperationCaptureGoldenLogicalBytes),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(overflow);

        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        await Assert.That(exception?.Maximum).IsEqualTo(SingleOperationCaptureGoldenLogicalBytes);
        await Assert.That(exception?.Observed).IsEqualTo(TwoOperationCaptureGoldenLogicalBytes);
    }

    /// <summary>Verifies payload rows without a durable stream row are rejected before capture materializes a result.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureFindsPayloadRowsWithoutStreamState_ThenCaptureRejectsDurableState()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        DeleteStreamRows(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadOutboxOperationEvidence(database.Path).Count).IsEqualTo(1);
    }

    /// <summary>Verifies snapshot cursor mismatch is rejected after bounded preflight and before result mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureFindsSnapshotCursorMismatch_ThenCaptureRejectsDurableState()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetSnapshotServerCursor(database.Path, "mismatched-cursor");
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        var evidence = ReadSnapshotCursorEvidence(database.Path);
        await Assert.That(evidence.SnapshotCursor).IsEqualTo("mismatched-cursor");
        await Assert.That(evidence.StreamCursor).IsNull();
        await Assert.That(evidence.OperationCount).IsEqualTo(1);
    }

    /// <summary>Verifies malformed subscription identity projections fail before raw identity values are materialized.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesMalformedSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetSubscriptionIdentityText(database.Path, "not-a-guid");
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadSubscriptionIdentityText(database.Path)).IsEqualTo("not-a-guid");
    }

    /// <summary>Verifies stream subscription mismatches are rejected from bounded projected identity evidence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesStreamSubscriptionMismatch_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetStreamSubscriptionId(database.Path, SubscriptionId.New());
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadSubscriptionIdentityText(database.Path)).IsEqualTo(subscriptionId.Value.ToString("D"));
    }

    /// <summary>Verifies missing subscription identities are rejected before stream or payload materialization.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesMissingSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        DeleteSubscriptionIdentityRows(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadSubscriptionIdentityCount(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies non-text stream cursor storage is rejected from bounded cursor evidence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesBlobStreamCursor_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetStreamServerCursorBlob(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadStreamServerCursorStorage(database.Path)).IsEqualTo("blob");
    }

    /// <summary>Verifies malformed stream subscription projections fail before raw stream values are materialized.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesMalformedStreamSubscriptionIdentity_ThenPreflightRejects()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        SetStreamSubscriptionIdText(database.Path, "short");
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadStreamSubscriptionIdentityText(database.Path)).IsEqualTo("short");
    }

    /// <summary>Verifies corrupted SQLite scalar storage is rejected by bounded preflight before operation decoding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesCorruptOperationScalar_ThenPreflightRejectsWithoutStateChange()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetOutboxOperationTypeInvalid(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture?>> capture = async () => await captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None)
            .AsTask()
            .ConfigureAwait(false);

        await Assert.That(capture).ThrowsExactly<InvalidOperationException>();
        var evidence = ReadOutboxOperationEvidence(database.Path);
        await Assert.That(evidence.Count).IsEqualTo(1);
        await Assert.That(evidence.OperationId).IsEqualTo(operation.OperationId.Value.ToString("D"));
        await Assert.That(evidence.OperationTypeStorage).IsEqualTo("text");
    }

    /// <summary>Verifies oversized operation identifier text is rejected before operation decoding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesOverlongOperationId_ThenPreflightRejectsWithoutStateChange()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetOutboxOperationIdText(database.Path, OverlongOperationIdText);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadOutboxOperationIdLength(database.Path)).IsEqualTo(OverlongOperationIdText.Length);
    }

    /// <summary>Verifies non-text base version storage is rejected before operation decoding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesBlobOperationBaseVersion_ThenPreflightRejectsWithoutStateChange()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetOutboxBaseVersionBlob(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        await Assert.That(ReadOutboxBaseVersionStorage(database.Path)).IsEqualTo("blob");
    }

    /// <summary>Verifies oversized corrupt SQLite payload rows fail capacity before payload decoding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureSeesOversizedInvalidPayload_ThenCapacityFailsBeforeDecode()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        SetOutboxPayloadOversizedAndInvalid(database.Path);
        var captureStore = RequireSnapshotRecoveryCaptureStore(adapter);

        var request = new LocalSnapshotRecoveryCaptureRequest { StreamId = Stream, SubscriptionId = subscriptionId, Limits = CreateTinyPayloadSnapshotRecoveryLimits() };
        Func<Task<LocalSnapshotRecoveryCapture?>> capture = async () => await captureStore.CaptureSnapshotRecoveryAsync(request, CancellationToken.None)
            .AsTask()
            .ConfigureAwait(false);

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(capture);

        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumPayloadBytes));
        await Assert.That(exception?.Maximum).IsEqualTo(SnapshotPayloadBoundaryBytes);
        await Assert.That(exception?.Observed).IsEqualTo(OversizedPayloadLength);
    }

    /// <summary>Verifies SQLite capture observes explicit cancellation without mutating durable state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSnapshotRecoveryCaptureIsCancelled_ThenSqliteStateIsUnchanged()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task<LocalSnapshotRecoveryCapture?>> capture = async () => await RequireSnapshotRecoveryCaptureStore(adapter)
            .CaptureSnapshotRecoveryAsync(
                CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
                cancellation.Token)
            .AsTask()
            .ConfigureAwait(false);

        await Assert.That(capture).ThrowsExactly<OperationCanceledException>();
        var recovered = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Creates a capture request for the shared stream.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="maximumPendingOperations">The pending operation limit.</param>
    /// <returns>The capture request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static LocalSnapshotRecoveryCaptureRequest CreateSnapshotRecoveryCaptureRequest(
        SubscriptionId subscriptionId,
        int maximumPendingOperations) =>
        CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations, SnapshotRecoveryCaptureLargeLogicalBytes);

    /// <summary>Creates a capture request for the shared stream.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="maximumPendingOperations">The pending operation limit.</param>
    /// <param name="maximumLogicalBytes">The logical byte limit.</param>
    /// <returns>The capture request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static LocalSnapshotRecoveryCaptureRequest CreateSnapshotRecoveryCaptureRequest(
        SubscriptionId subscriptionId,
        int maximumPendingOperations,
        long maximumLogicalBytes) =>
        new() { StreamId = Stream, SubscriptionId = subscriptionId, Limits = new() { MaximumPendingOperations = maximumPendingOperations, MaximumLogicalBytes = maximumLogicalBytes } };

    /// <summary>Creates tiny payload limits for pre-decode capacity tests.</summary>
    /// <returns>The snapshot recovery limits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SnapshotRecoveryLimits CreateTinyPayloadSnapshotRecoveryLimits() =>
        new() { MaximumPendingOperations = 1, MaximumPayloadBytes = SnapshotPayloadBoundaryBytes, MaximumLogicalBytes = SnapshotRecoveryCaptureLargeLogicalBytes };

    /// <summary>Requires the bounded SQLite snapshot recovery capture interface.</summary>
    /// <param name="candidate">The candidate store.</param>
    /// <returns>The snapshot recovery capture store.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not implemented the interface.</exception>
    private static ILocalSnapshotRecoveryCaptureStore RequireSnapshotRecoveryCaptureStore(object candidate) =>
        candidate as ILocalSnapshotRecoveryCaptureStore
        ?? throw new InvalidOperationException("Expected the SQLite local store to implement bounded snapshot recovery capture.");

    /// <summary>Corrupts the outbox payload with an oversized value and invalid schema metadata.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetOutboxPayloadOversizedAndInvalid(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload = zeroblob($payloadLength),
                payload_schema_version = 0;
            """;
        _ = command.Parameters.AddWithValue("$payloadLength", OversizedPayloadLength);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads raw outbox evidence without decoding the intentionally corrupted operation scalar.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The operation count, identifier, and operation type storage class.</returns>
    /// <exception cref="InvalidOperationException">The expected outbox evidence row is missing.</exception>
    private static (long Count, string OperationId, string OperationTypeStorage) ReadOutboxOperationEvidence(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*), MIN(operation_id), MIN(typeof(operation_type)) FROM oc_outbox;";
        using var reader = command.ExecuteReader();
        return reader.Read() && !reader.IsDBNull(OutboxEvidenceOperationIdIndex) && !reader.IsDBNull(OutboxEvidenceOperationTypeIndex)
            ? (
                reader.GetInt64(OutboxEvidenceCountIndex),
                reader.GetString(OutboxEvidenceOperationIdIndex),
                reader.GetString(OutboxEvidenceOperationTypeIndex))
            : throw new InvalidOperationException("Expected one outbox operation row.");
    }

    /// <summary>Marks all outbox operations replay-only by simulating accepted local completion without receive inclusion.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetOutboxOperationsReplayOnly(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = 4;
            DELETE FROM oc_outbox_receive_inclusions;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts one payload's schema and bytes while preserving its scalar lengths for preflight accounting.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="clientSequence">The client sequence to corrupt.</param>
    private static void SetOutboxPayloadSchemaZeroAndBytesInvalidForSequence(string path, long clientSequence)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET payload = zeroblob(length(CAST(payload AS BLOB))),
                payload_schema_version = 0
            WHERE client_sequence = $clientSequence;
            """;
        _ = command.Parameters.AddWithValue("$clientSequence", clientSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the operation type storage class without changing payload storage.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetOutboxOperationTypeInvalid(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_outbox
            SET operation_type = 'not-an-integer';
        """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts one operation base version with non-text storage.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetOutboxBaseVersionBlob(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET base_version = zeroblob($blobLength);";
        _ = command.Parameters.AddWithValue("$blobLength", CorruptScalarBlobLength);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes durable stream rows while preserving subscription and payload rows for corruption tests.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void DeleteStreamRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            DELETE FROM oc_streams;
            PRAGMA foreign_keys = ON;
        """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes subscription identity rows while preserving stream rows for corruption tests.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void DeleteSubscriptionIdentityRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            DELETE FROM oc_subscription_identities;
            PRAGMA foreign_keys = ON;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the durable snapshot cursor directly.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="cursor">The corrupt cursor.</param>
    private static void SetSnapshotServerCursor(string path, string cursor)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_snapshots SET server_cursor = $cursor;";
        _ = command.Parameters.AddWithValue("$cursor", cursor);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stream cursor to non-text storage.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void SetStreamServerCursorBlob(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_streams SET server_cursor = zeroblob($blobLength);";
        _ = command.Parameters.AddWithValue("$blobLength", CorruptScalarBlobLength);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the subscription identity row to raw text.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="subscriptionId">The raw subscription identifier text.</param>
    private static void SetSubscriptionIdentityText(string path, string subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            UPDATE oc_subscription_identities SET subscription_id = $subscriptionId;
            PRAGMA foreign_keys = ON;
            """;
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stream subscription identity to a different valid identifier.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    private static void SetStreamSubscriptionId(string path, SubscriptionId subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_streams SET subscription_id = $subscriptionId;";
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stream subscription identity to raw text.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="subscriptionId">The raw subscription identifier text.</param>
    private static void SetStreamSubscriptionIdText(string path, string subscriptionId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_streams SET subscription_id = $rawSubscriptionId;";
        _ = command.Parameters.AddWithValue("$rawSubscriptionId", subscriptionId);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads raw snapshot cursor evidence without using recovery decoders.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The snapshot cursor, stream cursor, and operation count.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static (string? SnapshotCursor, string? StreamCursor, long OperationCount) ReadSnapshotCursorEvidence(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT snapshot.server_cursor, stream.server_cursor, COUNT(outbox.operation_id)
            FROM oc_snapshots AS snapshot
            LEFT JOIN oc_streams AS stream
                ON stream.store_identity = snapshot.store_identity AND stream.stream_id = snapshot.stream_id
            LEFT JOIN oc_outbox AS outbox
                ON outbox.store_identity = snapshot.store_identity AND outbox.stream_id = snapshot.stream_id
            GROUP BY snapshot.server_cursor, stream.server_cursor;
            """;
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException("Expected one snapshot cursor evidence row.");
        }

        var snapshotCursor = reader.IsDBNull(0) ? null : reader.GetString(0);
        var streamCursor = reader.IsDBNull(1) ? null : reader.GetString(1);
        return (snapshotCursor, streamCursor, reader.GetInt64(SnapshotCursorEvidenceOperationCountIndex));
    }

    /// <summary>Reads the raw subscription identity text.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The subscription identity text.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static string ReadSubscriptionIdentityText(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT subscription_id FROM oc_subscription_identities LIMIT 1;";
        return command.ExecuteScalar() is string value
            ? value
            : throw new InvalidOperationException("Expected one subscription identity row.");
    }

    /// <summary>Reads the subscription identity row count.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The subscription identity row count.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static long ReadSubscriptionIdentityCount(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM oc_subscription_identities;";
        return command.ExecuteScalar() is long value
            ? value
            : throw new InvalidOperationException("Expected a subscription identity row count.");
    }

    /// <summary>Reads the stream cursor storage class.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The stream cursor storage class.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static string ReadStreamServerCursorStorage(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT typeof(server_cursor) FROM oc_streams LIMIT 1;";
        return command.ExecuteScalar() is string value
            ? value
            : throw new InvalidOperationException("Expected one stream cursor storage row.");
    }

    /// <summary>Reads the raw stream subscription identity text.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The stream subscription identity text.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static string ReadStreamSubscriptionIdentityText(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT subscription_id FROM oc_streams LIMIT 1;";
        return command.ExecuteScalar() is string value
            ? value
            : throw new InvalidOperationException("Expected one stream subscription identity row.");
    }

    /// <summary>Reads the raw operation identifier byte length.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The operation identifier byte length.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static long ReadOutboxOperationIdLength(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT length(CAST(operation_id AS BLOB)) FROM oc_outbox LIMIT 1;";
        return command.ExecuteScalar() is long value
            ? value
            : throw new InvalidOperationException("Expected one operation id length row.");
    }

    /// <summary>Reads the operation base version storage class.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The operation base version storage class.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected value.</exception>
    private static string ReadOutboxBaseVersionStorage(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT typeof(base_version) FROM oc_outbox LIMIT 1;";
        return command.ExecuteScalar() is string value
            ? value
            : throw new InvalidOperationException("Expected one operation base version storage row.");
    }

    /// <summary>Gets the expected logical byte count for one capture.</summary>
    /// <param name="capture">The capture.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetCaptureLogicalBytes(LocalSnapshotRecoveryCapture capture)
    {
        var bytes = GetRequiredUtf8Bytes(capture.StreamId.Value)
            + SnapshotCaptureGuidBytes
            + GetOptionalUtf8Bytes(capture.ServerCursor)
            + SnapshotCaptureInt64Bytes;
        bytes += capture.Snapshot is null ? 0 : GetSnapshotLogicalBytes(capture.Snapshot);
        bytes += SnapshotCaptureInt32Bytes + SnapshotCaptureInt32Bytes;

        HashSet<OperationId> counted = [];
        foreach (var operation in capture.PendingOperations)
        {
            bytes += counted.Add(operation.OperationId) ? GetOperationLogicalBytes(operation) : 0;
        }

        foreach (var operation in capture.ReplayOperations)
        {
            bytes += counted.Add(operation.OperationId) ? GetOperationLogicalBytes(operation) : 0;
        }

        return bytes;
    }

    /// <summary>Gets the expected logical byte count for one local snapshot.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetSnapshotLogicalBytes(LocalSnapshot snapshot) =>
        GetRequiredUtf8Bytes(snapshot.StreamId.Value)
        + SnapshotCaptureInt32Bytes
        + GetOptionalUtf8Bytes(snapshot.ServerCursor)
        + GetPayloadLogicalBytes(snapshot.State)
        + SnapshotCaptureInt64Bytes
        + SnapshotCaptureDateTimeOffsetBytes
        + (snapshot.AuthoritativeState is null ? 0 : GetPayloadLogicalBytes(snapshot.AuthoritativeState));

    /// <summary>Gets the expected logical byte count for one operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetOperationLogicalBytes(SyncOperation operation) =>
        SnapshotCaptureGuidBytes
        + GetRequiredUtf8Bytes(operation.StreamId.Value)
        + SnapshotCaptureInt64Bytes
        + SnapshotCaptureDateTimeOffsetBytes
        + SnapshotCaptureInt32Bytes
        + GetPayloadLogicalBytes(operation.Payload)
        + GetOptionalUtf8Bytes(operation.BaseVersion)
        + GetMetadataLogicalBytes(operation.Metadata)
        + SnapshotCaptureInt32Bytes
        + SnapshotCaptureInt32Bytes
        + SnapshotCaptureInt32Bytes
        + SnapshotCaptureInt32Bytes;

    /// <summary>Gets the expected logical byte count for one payload.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetPayloadLogicalBytes(PayloadEnvelope payload) =>
        SnapshotCaptureInt32Bytes
        + SnapshotCaptureInt64Bytes
        + GetRequiredUtf8Bytes(payload.ContractId)
        + GetRequiredUtf8Bytes(payload.ContentType)
        + GetRequiredUtf8Bytes(payload.PayloadHash)
        + payload.PayloadLength;

    /// <summary>Gets the expected logical byte count for metadata.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetMetadataLogicalBytes(IReadOnlyDictionary<string, string> metadata)
    {
        var bytes = SnapshotCaptureInt32Bytes;
        foreach (var pair in metadata)
        {
            bytes += GetRequiredUtf8Bytes(pair.Key);
            bytes += GetRequiredUtf8Bytes(pair.Value);
        }

        return bytes;
    }

    /// <summary>Gets the UTF-8 byte count for an optional string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The byte count.</returns>
    private static int GetOptionalUtf8Bytes(string? value) =>
        value is null ? 0 : GetRequiredUtf8Bytes(value);

    /// <summary>Gets the UTF-8 byte count for a required string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRequiredUtf8Bytes(string value) =>
        System.Text.Encoding.UTF8.GetByteCount(value);
}
