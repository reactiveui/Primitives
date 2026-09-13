// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Computes deterministic logical retained input sizes admitted to the SQLite local store adapter worker.</summary>
internal sealed class SqliteLocalStoreAdapterSizing
{
    /// <summary>Gets the minimum worker input size reported for a command.</summary>
    internal const long MinimumCommandBytes = 1;

    /// <summary>The fixed retained size used for a GUID.</summary>
    private const long GuidBytes = 16;

    /// <summary>The fixed retained size used for a long value.</summary>
    private const long LongBytes = 8;

    /// <summary>The fixed retained size used for an int value.</summary>
    private const long IntBytes = 4;

    /// <summary>The fixed retained size used for a date-time value.</summary>
    private const long DateTimeOffsetBytes = 16;

    /// <summary>The fixed retained size used for a time-span value.</summary>
    private const long TimeSpanBytes = 8;

    /// <summary>The fixed retained size used for a nullable value marker.</summary>
    private const long NullableMarkerBytes = 1;

    /// <summary>The fixed retained size used for one collection or object shell.</summary>
    private const long ObjectHeaderBytes = 32;

    /// <summary>The maximum logical input size accepted by the worker when empty.</summary>
    private readonly long _capacityBytes;

    /// <summary>Initializes a new instance of the <see cref="SqliteLocalStoreAdapterSizing"/> class.</summary>
    /// <param name="capacityBytes">The maximum logical input size accepted by the worker when empty.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacityBytes"/> is not positive.</exception>
    internal SqliteLocalStoreAdapterSizing(long capacityBytes)
    {
        if (capacityBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "Worker capacity bytes must be positive.");
        }

        _capacityBytes = capacityBytes;
    }

    /// <summary>Gets the retained bytes for an operation identifier.</summary>
    internal long OperationIdentifierBytes => Add(0, GuidBytes);

    /// <summary>Gets the retained bytes for an attempt barrier command.</summary>
    internal long AttemptBarrierBytes => Add(GuidBytes, GuidBytes + IntBytes);

    /// <summary>Gets the retained bytes for a lease mutation command.</summary>
    internal long LeaseMutationBytes => Add(GuidBytes, DateTimeOffsetBytes);

    /// <summary>Computes retained input bytes for initialization.</summary>
    /// <param name="initialization">The initialization input.</param>
    /// <returns>The retained bytes.</returns>
    internal long InitializationBytes(LocalStoreInitialization initialization)
    {
        ArgumentExceptionHelper.ThrowIfNull(initialization);
        var bytes = Add(ObjectHeaderBytes, Add(StringBytes(initialization.StoreIdentity), IntBytes + NullableMarkerBytes));
        return Add(bytes, StringBytes(initialization.ClientId));
    }

    /// <summary>Computes retained input bytes for subscription lookup.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long SubscriptionLookupBytes(StreamId streamId, SubscriptionId? preferredId) =>
        Add(StreamIdBytes(streamId), preferredId.HasValue ? GuidBytes : NullableMarkerBytes);

    /// <summary>Computes retained input bytes for stream recovery.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long RecoveryBytes(StreamId streamId) => Add(StreamIdBytes(streamId), GuidBytes);

    /// <summary>Computes retained input bytes for local commit.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The retained bytes.</returns>
    internal long CommitBytes(SyncOperation operation, SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        return Add(SyncOperationBytes(operation), SnapshotMutationBytes(snapshotMutation));
    }

    /// <summary>Computes retained input bytes for lease acquisition.</summary>
    /// <param name="request">The lease request.</param>
    /// <returns>The retained bytes.</returns>
    internal long LeaseRequestBytes(OutboxLeaseRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        var streamBytes = request.StreamId.HasValue ? StreamIdBytes(request.StreamId.Value) : NullableMarkerBytes;
        return Add(ObjectHeaderBytes, Add(streamBytes, IntBytes + LongBytes + DateTimeOffsetBytes));
    }

    /// <summary>Computes retained input bytes for sync result application.</summary>
    /// <param name="result">The result.</param>
    /// <returns>The retained bytes.</returns>
    internal long SyncResultBytes(RemoteSyncResult result)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        var bytes = Add(GuidBytes, GuidBytes);
        bytes = Add(bytes, StringBytes(result.ServerCursor));
        bytes = Add(bytes, result.RetryAfter.HasValue ? DateTimeOffsetBytes : NullableMarkerBytes);
        return Add(bytes, CollectionBytes(result.Operations, OperationSyncResultBytes));
    }

    /// <summary>Computes retained input bytes for one dead-letter reconciliation.</summary>
    /// <param name="reasonCode">The reason code.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The retained bytes.</returns>
    internal long DeadLetterBytes(string reasonCode, SnapshotMutation snapshotMutation)
    {
        var bytes = Add(GuidBytes, GuidBytes);
        bytes = Add(bytes, StringBytes(reasonCode));
        return Add(bytes, SnapshotMutationBytes(snapshotMutation));
    }

    /// <summary>Adds retained input bytes for an owned snapshot mutation collection header.</summary>
    /// <param name="bytes">The current byte count.</param>
    /// <param name="snapshotMutationCount">The validated snapshot mutation count.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long AddSnapshotMutationCollectionBytes(long bytes, int snapshotMutationCount)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(snapshotMutationCount);
        return Add(bytes, ObjectHeaderBytes + IntBytes);
    }

    /// <summary>Adds retained input bytes for one owned snapshot mutation.</summary>
    /// <param name="bytes">The current byte count.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long AddSnapshotMutationBytes(long bytes, SnapshotMutation snapshotMutation) => Add(bytes, SnapshotMutationBytes(snapshotMutation));

    /// <summary>Computes retained input bytes for event identifier lookup.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventIdCount">The copied event identifier count.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long EventIdLookupBytes(StreamId streamId, int eventIdCount)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(eventIdCount);
        return Add(StreamIdBytes(streamId), Add(ObjectHeaderBytes, GuidBytes * (long)eventIdCount));
    }

    /// <summary>Computes retained input bytes for remote apply.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The retained bytes.</returns>
    internal long RemoteApplyBytes(RemoteEventBatch batch, SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        var bytes = Add(GuidBytes, StreamIdBytes(batch.StreamId));
        bytes = Add(bytes, StringBytes(batch.PreviousCursor));
        bytes = Add(bytes, StringBytes(batch.NextCursor));
        bytes = Add(bytes, CollectionBytes(batch.Events, RemoteEventBytes));
        bytes = Add(bytes, CollectionBytes(batch.CompletedOperations, RemoteOperationCompletionBytes));
        return Add(bytes, SnapshotMutationBytes(snapshotMutation));
    }

    /// <summary>Computes retained input bytes for retry state persistence.</summary>
    /// <param name="retryState">The retry state.</param>
    /// <returns>The retained bytes.</returns>
    internal long RetryStateBytes(RetryState retryState)
    {
        ArgumentExceptionHelper.ThrowIfNull(retryState);
        var bytes = Add(GuidBytes, ObjectHeaderBytes);
        bytes = Add(bytes, DateTimeOffsetBytes);
        bytes = Add(bytes, retryState.DueUtc.HasValue ? DateTimeOffsetBytes : NullableMarkerBytes);
        bytes = Add(bytes, retryState.PreviousDelay.HasValue ? TimeSpanBytes : NullableMarkerBytes);
        bytes = Add(bytes, IntBytes);
        bytes = Add(bytes, IntBytes);
        return Add(bytes, StringBytes(retryState.CredentialsVersion));
    }

    /// <summary>Computes retained input bytes for compaction.</summary>
    /// <param name="request">The request.</param>
    /// <returns>The retained bytes.</returns>
    internal long CompactionBytes(CompactionRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        var streamBytes = request.StreamId.HasValue ? StreamIdBytes(request.StreamId.Value) : NullableMarkerBytes;
        return Add(ObjectHeaderBytes, Add(streamBytes, DateTimeOffsetBytes + LongBytes));
    }

    /// <summary>Computes retained input bytes for payload quarantine.</summary>
    /// <param name="request">The normalized request.</param>
    /// <returns>The retained bytes.</returns>
    internal long QuarantineBytes(SqliteNormalizedPayloadQuarantineRequest request)
    {
        var normalizedRequest = request.Request;
        var bytes = Add(ObjectHeaderBytes, StreamIdBytes(normalizedRequest.StreamId));
        bytes = Add(bytes, normalizedRequest.SubscriptionId.HasValue ? GuidBytes : NullableMarkerBytes);
        bytes = Add(bytes, normalizedRequest.OperationId.HasValue ? GuidBytes : NullableMarkerBytes);
        bytes = Add(bytes, normalizedRequest.EventId.HasValue ? GuidBytes : NullableMarkerBytes);
        bytes = Add(bytes, IntBytes + IntBytes + DateTimeOffsetBytes);
        bytes = Add(bytes, StringBytes(normalizedRequest.ReasonCode));
        bytes = Add(bytes, StringBytes(normalizedRequest.Cursor));
        return Add(bytes, QuarantineEvidenceBytes(request.Evidence));
    }

    /// <summary>Computes retained input bytes for a stream identifier.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal long StreamIdBytes(StreamId streamId) => Add(ObjectHeaderBytes, StringBytes(streamId.Value));

    /// <summary>Computes retained input bytes for an operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The retained bytes.</returns>
    private long SyncOperationBytes(SyncOperation operation)
    {
        var bytes = Add(ObjectHeaderBytes, GuidBytes);
        bytes = Add(bytes, StreamIdBytes(operation.StreamId));
        bytes = Add(bytes, LongBytes + DateTimeOffsetBytes + IntBytes);
        bytes = Add(bytes, StringBytes(operation.BaseVersion));
        bytes = Add(bytes, PayloadBytes(operation.Payload));
        bytes = Add(bytes, OperationPolicyBytes(operation.Policy));
        return Add(bytes, DictionaryBytes(operation.Metadata));
    }

    /// <summary>Computes retained input bytes for a snapshot mutation.</summary>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The retained bytes.</returns>
    private long SnapshotMutationBytes(SnapshotMutation snapshotMutation)
    {
        var bytes = Add(ObjectHeaderBytes, StreamIdBytes(snapshotMutation.StreamId));
        bytes = Add(bytes, PayloadBytes(snapshotMutation.State));
        bytes = Add(bytes, snapshotMutation.AuthoritativeState is null ? NullableMarkerBytes : PayloadBytes(snapshotMutation.AuthoritativeState));
        return Add(bytes, IntBytes + LongBytes);
    }

    /// <summary>Computes retained input bytes for a remote event.</summary>
    /// <param name="remoteEvent">The remote event.</param>
    /// <returns>The retained bytes.</returns>
    private long RemoteEventBytes(RemoteEvent remoteEvent)
    {
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        var bytes = Add(ObjectHeaderBytes, GuidBytes);
        bytes = Add(bytes, StreamIdBytes(remoteEvent.StreamId));
        bytes = Add(bytes, StringBytes(remoteEvent.ServerCursor));
        bytes = Add(bytes, DateTimeOffsetBytes);
        bytes = Add(bytes, remoteEvent.CausedByOperationId.HasValue ? GuidBytes : NullableMarkerBytes);
        bytes = Add(bytes, remoteEvent.Origin is null ? NullableMarkerBytes : Add(ObjectHeaderBytes, Add(StringBytes(remoteEvent.Origin.ClientId), GuidBytes)));
        bytes = Add(bytes, PayloadBytes(remoteEvent.Payload));
        return Add(bytes, DictionaryBytes(remoteEvent.Metadata));
    }

    /// <summary>Computes retained input bytes for a remote operation completion.</summary>
    /// <param name="completion">The completion declaration.</param>
    /// <returns>The retained bytes.</returns>
    private long RemoteOperationCompletionBytes(RemoteOperationCompletion completion)
    {
        ArgumentExceptionHelper.ThrowIfNull(completion);
        var bytes = Add(ObjectHeaderBytes, Add(StringBytes(completion.Origin.ClientId), GuidBytes));
        bytes = Add(bytes, ObjectHeaderBytes + IntBytes);
        return Add(bytes, GuidBytes * (long)completion.EventIds.Count);
    }

    /// <summary>Computes retained input bytes for an operation sync result.</summary>
    /// <param name="result">The operation result.</param>
    /// <returns>The retained bytes.</returns>
    private long OperationSyncResultBytes(OperationSyncResult result)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        var bytes = Add(ObjectHeaderBytes, GuidBytes + IntBytes);
        bytes = Add(bytes, StringBytes(result.ReasonCode));
        return Add(bytes, StringBytes(result.ServerVersion));
    }

    /// <summary>Computes retained input bytes for a payload envelope.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The retained bytes.</returns>
    private long PayloadBytes(PayloadEnvelope payload)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        var bytes = Add(ObjectHeaderBytes, StringBytes(payload.ContractId));
        bytes = Add(bytes, IntBytes);
        bytes = Add(bytes, StringBytes(payload.ContentType));
        bytes = Add(bytes, payload.PayloadLength);
        return Add(bytes, StringBytes(payload.PayloadHash));
    }

    /// <summary>Computes retained input bytes for quarantine evidence.</summary>
    /// <param name="evidence">The evidence.</param>
    /// <returns>The retained bytes.</returns>
    private long QuarantineEvidenceBytes(LocalPayloadQuarantineEvidence evidence)
    {
        ArgumentExceptionHelper.ThrowIfNull(evidence);
        var bytes = Add(ObjectHeaderBytes, StringBytes(evidence.ContractId));
        bytes = Add(bytes, evidence.SchemaVersion.HasValue ? IntBytes : NullableMarkerBytes);
        bytes = Add(bytes, StringBytes(evidence.ContentType));
        bytes = Add(bytes, IntBytes);
        bytes = Add(bytes, StringBytes(evidence.PayloadHash));
        return Add(bytes, evidence.PayloadPrefix.Length);
    }

    /// <summary>Computes retained input bytes for operation policy.</summary>
    /// <param name="policy">The policy.</param>
    /// <returns>The retained bytes.</returns>
    private long OperationPolicyBytes(OperationPolicy policy)
    {
        ArgumentExceptionHelper.ThrowIfNull(policy);
        return Add(ObjectHeaderBytes, IntBytes + IntBytes + IntBytes + IntBytes);
    }

    /// <summary>Computes retained input bytes for dictionary content.</summary>
    /// <param name="metadata">The dictionary.</param>
    /// <returns>The retained bytes.</returns>
    private long DictionaryBytes(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        var bytes = Add(ObjectHeaderBytes, IntBytes);
        foreach (var pair in metadata)
        {
            bytes = Add(bytes, StringBytes(pair.Key));
            bytes = Add(bytes, StringBytes(pair.Value));
        }

        return bytes;
    }

    /// <summary>Computes retained input bytes for collection content.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="items">The collection.</param>
    /// <param name="itemSizer">The item sizing callback.</param>
    /// <returns>The retained bytes.</returns>
    /// <exception cref="QueueCapacityExceededException">The collection cannot fit in an empty worker.</exception>
    private long CollectionBytes<T>(IReadOnlyList<T> items, Func<T, long> itemSizer)
    {
        ArgumentExceptionHelper.ThrowIfNull(items);
        var count = items.Count;
        if (count > _capacityBytes)
        {
            throw new QueueCapacityExceededException("The SQLite command worker has reached its configured capacity.", canFitWhenEmpty: false);
        }

        var bytes = Add(ObjectHeaderBytes, IntBytes);
        for (var index = 0; index < count; index++)
        {
            bytes = Add(bytes, itemSizer(items[index]));
        }

        return bytes;
    }

    /// <summary>Computes retained input bytes for nullable text.</summary>
    /// <param name="value">The text value.</param>
    /// <returns>The retained bytes.</returns>
    private long StringBytes(string? value) =>
        value is null ? NullableMarkerBytes : Add(ObjectHeaderBytes, sizeof(char) * (long)value.Length);

    /// <summary>Adds the next logical input size within the configured worker capacity.</summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right value.</param>
    /// <returns>The accumulated logical input size.</returns>
    /// <exception cref="QueueCapacityExceededException">The next size cannot fit in an empty worker.</exception>
    private long Add(long left, long right)
    {
        if (right <= _capacityBytes - left)
        {
            return left + right;
        }

        throw new QueueCapacityExceededException("The SQLite command worker has reached its configured capacity.", canFitWhenEmpty: false);
    }
}
