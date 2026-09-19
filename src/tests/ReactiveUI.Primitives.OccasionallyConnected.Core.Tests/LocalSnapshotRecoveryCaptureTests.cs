// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalSnapshotRecoveryCapture"/>.</summary>
public sealed class LocalSnapshotRecoveryCaptureTests
{
    /// <summary>The mutation offset used by ownership checks.</summary>
    private const int MutationOffset = 1;

    /// <summary>The next client sequence used by capture tests.</summary>
    private const long NextClientSequence = 2;

    /// <summary>The stream used by capture tests.</summary>
    private static readonly StreamId Stream = new("capture-stream");

    /// <summary>The subscription used by capture tests.</summary>
    private static readonly SubscriptionId Subscription = SubscriptionId.New();

    /// <summary>Verifies capture records own snapshot payload bytes and operation collections.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureOwnsSnapshotPayloadBytesAndOperationCollections()
    {
        var operation = CreateOperation();
        var snapshot = CreateSnapshot();
        List<SyncOperation> pending = [operation];
        List<SyncOperation> replay = [operation];
        var capture = new LocalSnapshotRecoveryCapture
        {
            StreamId = Stream,
            SubscriptionId = Subscription,
            ServerCursor = null,
            Snapshot = snapshot,
            NextClientSequence = NextClientSequence,
            PendingOperations = pending,
            ReplayOperations = replay,
        };

        pending.Add(CreateOperation());
        replay.Clear();
        var snapshotPayloadExposesArray = MemoryMarshal.TryGetArray(capture.Snapshot!.State.Payload, out var segment);
        var operationPayloadExposesArray = MemoryMarshal.TryGetArray(capture.PendingOperations[0].Payload.Payload, out var operationSegment);

        await Assert.That(snapshotPayloadExposesArray).IsTrue();
        await Assert.That(operationPayloadExposesArray).IsTrue();
        segment.Array![segment.Offset + MutationOffset] = (byte)'z';
        operationSegment.Array![operationSegment.Offset + MutationOffset] = (byte)'z';

        await Assert.That(capture.StreamId).IsEqualTo(Stream);
        await Assert.That(capture.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(capture.NextClientSequence).IsEqualTo(NextClientSequence);
        await Assert.That(capture.Snapshot).IsNotSameReferenceAs(snapshot);
        await Assert.That(capture.Snapshot.State).IsNotSameReferenceAs(snapshot.State);
        await Assert.That(capture.Snapshot.Revision).IsEqualTo(1);
        await Assert.That(Encoding.UTF8.GetString(capture.Snapshot.State.Payload.Span)).IsEqualTo("state");
        await Assert.That(capture.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(capture.PendingOperations[0]).IsNotSameReferenceAs(operation);
        await Assert.That(capture.PendingOperations[0].Payload).IsNotSameReferenceAs(operation.Payload);
        await Assert.That(Encoding.UTF8.GetString(capture.PendingOperations[0].Payload.Payload.Span)).IsEqualTo("operation");
        await Assert.That(capture.ReplayOperations).Count().IsEqualTo(1);
        await Assert.That(capture.ReplayOperations[0]).IsNotSameReferenceAs(operation);
        await Assert.That(capture.ReplayOperations[0].Payload).IsNotSameReferenceAs(operation.Payload);
        await Assert.That(capture.ReplayOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(((ICollection<SyncOperation>)capture.PendingOperations).IsReadOnly).IsTrue();
        await Assert.That(((ICollection<SyncOperation>)capture.ReplayOperations).IsReadOnly).IsTrue();
    }

    /// <summary>Verifies missing snapshots remain absent and authoritative payload bytes are owned.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CaptureAcceptsMissingSnapshotAndOwnsAuthoritativePayloadBytes()
    {
        var authoritative = CreatePayload("authoritative");
        var snapshot = CreateSnapshot(authoritative);
        var withSnapshot = new LocalSnapshotRecoveryCapture { StreamId = Stream, SubscriptionId = Subscription, ServerCursor = null, Snapshot = snapshot, NextClientSequence = NextClientSequence };
        var withoutSnapshot = new LocalSnapshotRecoveryCapture { StreamId = Stream, SubscriptionId = Subscription, ServerCursor = null, Snapshot = null, NextClientSequence = NextClientSequence };

        var authoritativePayloadExposesArray = MemoryMarshal.TryGetArray(authoritative.Payload, out var segment);

        await Assert.That(authoritativePayloadExposesArray).IsTrue();
        segment.Array![segment.Offset + MutationOffset] = (byte)'z';
        await Assert.That(withoutSnapshot.Snapshot).IsNull();
        await Assert.That(withSnapshot.Snapshot).IsNotSameReferenceAs(snapshot);
        await Assert.That(withSnapshot.Snapshot!.AuthoritativeState).IsNotSameReferenceAs(authoritative);
        await Assert.That(Encoding.UTF8.GetString(withSnapshot.Snapshot.AuthoritativeState!.Payload.Span)).IsEqualTo("authoritative");
    }

    /// <summary>Creates a representative local snapshot.</summary>
    /// <returns>The local snapshot.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static LocalSnapshot CreateSnapshot() =>
        CreateSnapshot(null);

    /// <summary>Creates a representative local snapshot with optional authoritative state.</summary>
    /// <param name="authoritativeState">The authoritative state payload, if one exists.</param>
    /// <returns>The local snapshot.</returns>
    private static LocalSnapshot CreateSnapshot(PayloadEnvelope? authoritativeState) =>
        new(Stream, 1, null, CreatePayload("state"), Revision: 1, DateTimeOffset.UnixEpoch) { AuthoritativeState = authoritativeState };

    /// <summary>Creates a representative sync operation.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = CreatePayload("operation"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Creates a payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", 1, "application/json", Encoding.UTF8.GetBytes(text), $"hash-{text}");
}
