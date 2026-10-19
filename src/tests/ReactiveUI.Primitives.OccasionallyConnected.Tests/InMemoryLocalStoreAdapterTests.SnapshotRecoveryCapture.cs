// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Bounded snapshot recovery capture tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
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

    /// <summary>The independent golden logical byte total for the one-operation byte-boundary fixture.</summary>
    private const long SingleOperationCaptureGoldenLogicalBytes = 351;

    /// <summary>Verifies bounded capture reads a real pending/replay view without replacing startup recovery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncCapturesPendingReplayIdentitiesAndPreservesRecoverStreamAsync()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var committed = await CommitOperationAsync(store, Stream, FirstClientSequence, "capture-pending");

        var capture = await RequireSnapshotRecoveryCaptureStore(store).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

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

    /// <summary>Verifies capture rejects mismatched subscription fences without changing recovery state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncRejectsMismatchedSubscriptionWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "mismatch-pending");
        var captureStore = RequireSnapshotRecoveryCaptureStore(store);

        Func<Task<LocalSnapshotRecoveryCapture>> capture = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(SubscriptionId.New(), maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(capture);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies capture skips other streams and included synchronized operations while keeping authoritative state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncSkipsNonVisibleOperationsAndCapturesAuthoritativeSnapshot()
    {
        await using var store = await CreateInitializedBoundStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "included");
        _ = await CommitOperationAsync(store, OtherStream, FirstClientSequence, "other-stream");
        var remoteEvent = CreateOriginEvent(RemoteCursor, operation.OperationId, ClientId);
        var batch = CreateRemoteBatch(null, RemoteCursor, [remoteEvent]) with
        {
            CompletedOperations = [new(new(ClientId, operation.OperationId), [remoteEvent.EventId])],
        };
        _ = await store.ApplyRemoteBatchAsync(
            batch,
            CreateSnapshotMutation(expectedRevision: 1) with { AuthoritativeState = CreatePayload(AuthoritativePayloadText) },
            CancellationToken.None);
        await SetServerResultAsync(store, operation, OperationResultKind.Accepted);

        var capture = await RequireSnapshotRecoveryCaptureStore(store).CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None);

        await Assert.That(capture.ServerCursor).IsEqualTo(RemoteCursor);
        await Assert.That(capture.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(capture.ReplayOperations.Count).IsEqualTo(0);
        await Assert.That(capture.Snapshot?.AuthoritativeState).IsNotNull();
        await Assert.That(System.Text.Encoding.UTF8.GetString(capture.Snapshot!.AuthoritativeState!.Payload.Span)).IsEqualTo(AuthoritativePayloadText);
    }

    /// <summary>Verifies pending count limits accept the exact bound and reject one above it without mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncEnforcesPendingCountBeforeMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var first = await CommitOperationAsync(store, Stream, FirstClientSequence, "count-first");
        var second = await CommitOperationAsync(store, Stream, SecondClientSequence, "count-second");
        var captureStore = RequireSnapshotRecoveryCaptureStore(store);

        var exact = await captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: ExpectedPendingOperationCount),
            CancellationToken.None);
        Func<Task<LocalSnapshotRecoveryCapture>> overflow = () => captureStore.CaptureSnapshotRecoveryAsync(
            CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<SnapshotRecoveryCapacityExceededException>(overflow);
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exact.PendingOperations.Count).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumPendingOperations));
        await Assert.That(exception?.Maximum).IsEqualTo(1);
        await Assert.That(exception?.Observed).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedPendingOperationCount);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Verifies logical byte limits accept the exact bound and reject one byte below without mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncEnforcesLogicalBytesBeforeMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "byte-boundary");
        var captureStore = RequireSnapshotRecoveryCaptureStore(store);
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
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exact.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(exception?.LimitName).IsEqualTo(nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        await Assert.That(exception?.Maximum).IsEqualTo(exactLogicalBytes - 1);
        await Assert.That(exception?.Observed).IsEqualTo(exactLogicalBytes);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies cancellation is explicit and leaves recovered durable state unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureSnapshotRecoveryAsyncObservesCancellationWithoutMutation()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, "cancel-pending");
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task<LocalSnapshotRecoveryCapture?>> capture = async () => await RequireSnapshotRecoveryCaptureStore(store)
            .CaptureSnapshotRecoveryAsync(
                CreateSnapshotRecoveryCaptureRequest(subscriptionId, maximumPendingOperations: 1),
                cancellation.Token)
            .AsTask()
            .ConfigureAwait(false);

        await Assert.That(capture).ThrowsExactly<OperationCanceledException>();
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
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
    private static LocalSnapshotRecoveryCaptureRequest CreateSnapshotRecoveryCaptureRequest(
        SubscriptionId subscriptionId,
        int maximumPendingOperations,
        long maximumLogicalBytes) =>
        new() { StreamId = Stream, SubscriptionId = subscriptionId, Limits = new() { MaximumPendingOperations = maximumPendingOperations, MaximumLogicalBytes = maximumLogicalBytes } };

    /// <summary>Requires the bounded snapshot recovery capture interface.</summary>
    /// <param name="candidate">The candidate store.</param>
    /// <returns>The snapshot recovery capture store.</returns>
    /// <exception cref="InvalidOperationException">The adapter has not implemented the interface.</exception>
    private static ILocalSnapshotRecoveryCaptureStore RequireSnapshotRecoveryCaptureStore(object candidate) =>
        candidate as ILocalSnapshotRecoveryCaptureStore
        ?? throw new InvalidOperationException("Expected the in-memory local store to implement bounded snapshot recovery capture.");

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
