// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains helper members for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>The encoded byte count for a timestamp value.</summary>
    private const int DateTimeOffsetEncodedBytes = 16;

    /// <summary>The encoded byte count for an enum value.</summary>
    private const int EnumEncodedBytes = 4;

    /// <summary>The encoded byte count for a GUID value.</summary>
    private const int GuidEncodedBytes = 16;

    /// <summary>The encoded byte count for a 32-bit integer value.</summary>
    private const int Int32EncodedBytes = 4;

    /// <summary>The encoded byte count for a 64-bit integer value.</summary>
    private const int Int64EncodedBytes = 8;

    /// <summary>The encoded byte count for a duration value.</summary>
    private const int TimeSpanEncodedBytes = 8;

    /// <summary>The maximum client identity length in UTF-16 code units.</summary>
    private const int MaximumClientIdLength = 256;

    /// <summary>The retained metadata key used to represent a client identity binding.</summary>
    private const string ClientIdentityBindingMetadataKey = "rxui.localstore.client_id";

    /// <summary>The strict UTF-8 encoding used for client identity validation.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>The stable comparison used when ordering recovered operations by client sequence.</summary>
    private static readonly Comparison<SyncOperation> OperationSequenceComparison = CompareOperationSequence;

    /// <summary>Compares operation records by client sequence.</summary>
    /// <param name="left">The first record.</param>
    /// <param name="right">The second record.</param>
    /// <returns>The comparison result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareOperationRecordSequence(OperationRecord left, OperationRecord right) =>
        left.Operation.ClientSequence.CompareTo(right.Operation.ClientSequence);

    /// <summary>Compares operations by client sequence.</summary>
    /// <param name="left">The first operation.</param>
    /// <param name="right">The second operation.</param>
    /// <returns>The comparison result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareOperationSequence(SyncOperation left, SyncOperation right) =>
        left.ClientSequence.CompareTo(right.ClientSequence);

    /// <summary>Compares stream identifiers ordinally.</summary>
    /// <param name="left">The first stream identifier.</param>
    /// <param name="right">The second stream identifier.</param>
    /// <returns>The comparison result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareStreamId(StreamId left, StreamId right) =>
        string.CompareOrdinal(left.Value, right.Value);

    /// <summary>Compares operation records by terminal time.</summary>
    /// <param name="left">The first record.</param>
    /// <param name="right">The second record.</param>
    /// <returns>The comparison result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareTerminalTime(OperationRecord left, OperationRecord right) =>
        Nullable.Compare(left.TerminalAtUtc, right.TerminalAtUtc);

    /// <summary>Adds a duration to a timestamp and rejects overflow.</summary>
    /// <param name="timestamp">The timestamp.</param>
    /// <param name="duration">The duration.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <returns>The adjusted timestamp.</returns>
    /// <exception cref="ArgumentException">The timestamp cannot be represented.</exception>
    private static DateTimeOffset CheckedAdd(DateTimeOffset timestamp, TimeSpan duration, string parameterName)
    {
        try
        {
            return timestamp.Add(duration);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ArgumentException("The in-memory lease expiry is outside the supported timestamp range.", parameterName, exception);
        }
    }

    /// <summary>Creates a status value for an operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="state">The operation state.</param>
    /// <param name="attempt">The attempt count.</param>
    /// <param name="changedAtUtc">The status timestamp.</param>
    /// <param name="reasonCode">The optional reason code.</param>
    /// <returns>The operation status.</returns>
    private static SyncOperationStatus CreateStatus(
        SyncOperation operation,
        SyncOperationState state,
        int attempt,
        DateTimeOffset changedAtUtc,
        string? reasonCode) =>
        new(operation.OperationId, operation.StreamId, state, attempt, changedAtUtc, reasonCode);

    /// <summary>Gets the durable state recorded when an attempt starts.</summary>
    /// <param name="policy">The operation policy.</param>
    /// <returns>The attempt state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperationState GetAttemptState(OperationPolicy policy) =>
        policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce ? SyncOperationState.Ambiguous : SyncOperationState.Uploading;

    /// <summary>Returns the original receipt for a duplicate commit.</summary>
    /// <param name="duplicate">The existing operation record.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="snapshotMutation">The requested snapshot mutation.</param>
    /// <returns>The original receipt.</returns>
    /// <exception cref="InvalidOperationException">The duplicate commit has different content.</exception>
    private static LocalCommitResult GetDuplicateReceipt(
        OperationRecord duplicate,
        SyncOperation operation,
        SnapshotMutation snapshotMutation)
    {
        if (InMemoryLocalStoreAdapterValidation.HasSameIntent(duplicate.Operation, operation, duplicate.SnapshotMutation, snapshotMutation))
        {
            return duplicate.Receipt;
        }

        throw new InvalidOperationException("The operation identifier has already been committed with different content.");
    }

    /// <summary>Maps a remote result kind to a durable operation state.</summary>
    /// <param name="kind">The result kind.</param>
    /// <returns>The operation state.</returns>
    private static SyncOperationState GetResultState(OperationResultKind kind) =>
        kind switch
        {
            OperationResultKind.Accepted => SyncOperationState.Synchronized,
            OperationResultKind.Conflict => SyncOperationState.Conflict,
            OperationResultKind.Rejected => SyncOperationState.Rejected,
            _ => SyncOperationState.QueuedForUpload,
        };

    /// <summary>Determines whether a state blocks later stream operations.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the state blocks the stream head.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsBlockingHead(SyncOperationState state) =>
        state is SyncOperationState.Conflict or SyncOperationState.GuaranteeExpired or SyncOperationState.Ambiguous;

    /// <summary>Determines whether a state has a definitive terminal outcome.</summary>
    /// <param name="state">The operation state.</param>
    /// <returns>Whether the state is definitive terminal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsDefinitiveTerminal(SyncOperationState state) =>
        state is SyncOperationState.Synchronized or SyncOperationState.Rejected or SyncOperationState.DeadLettered;

    /// <summary>Determines whether an operation should be returned in recovered pending operations.</summary>
    /// <param name="record">The operation record.</param>
    /// <returns>Whether the operation is pending-visible.</returns>
    private static bool ShouldRecoverPendingOperation(OperationRecord record) =>
        !IsDefinitiveTerminal(record.Status.State);

    /// <summary>Determines whether an operation should be returned in recovered replay operations.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="included">Whether authoritative receive inclusion was recorded.</param>
    /// <returns>Whether the operation is replay-visible.</returns>
    private static bool ShouldRecoverReplayOperation(OperationRecord record, bool included) =>
        !included && record.Status.State is not SyncOperationState.Rejected and not SyncOperationState.DeadLettered;

    /// <summary>Validates an optional client identity binding.</summary>
    /// <param name="clientId">The client identity.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <returns>The validated client identity.</returns>
    /// <exception cref="ArgumentException">The client identity is blank, malformed, or too long.</exception>
    private static string? ValidateClientId(string? clientId, string parameterName)
    {
        if (clientId is null)
        {
            return null;
        }

        if (clientId.Length > MaximumClientIdLength)
        {
            throw new ArgumentException("ClientId must be at most 256 UTF-16 code units.", parameterName);
        }

#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId, parameterName);
#else
        ThrowIfBlankClientId(clientId, parameterName);
#endif

        try
        {
            _ = StrictUtf8.GetByteCount(clientId);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("ClientId must be well-formed Unicode.", parameterName, exception);
        }

        return clientId;
    }

#if !NET8_0_OR_GREATER
    /// <summary>Throws when a client identity is blank on target frameworks without built-in argument validation.</summary>
    /// <param name="clientId">The client identity.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The client identity is blank.</exception>
    private static void ThrowIfBlankClientId(string clientId, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(clientId))
        {
            return;
        }

        throw new ArgumentException("ClientId must not be blank.", parameterName);
    }
#endif

    /// <summary>Determines whether a stream head must wait for ownership or a retry decision.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <returns>Whether the operation blocks leasing.</returns>
    private static bool IsBlockedForLease(OperationRecord record, DateTimeOffset nowUtc) =>
        IsBlockingHead(record.Status.State) || record.LeaseId.HasValue
        || (record.Attempt > 0 && record.Operation.Policy.DeliveryGuarantee == DeliveryGuarantee.AtMostOnce)
        || record.RetryState?.DueUtc > nowUtc;

    /// <summary>Combines two capacity usages with checked arithmetic.</summary>
    /// <param name="left">The first usage.</param>
    /// <param name="right">The second usage.</param>
    /// <returns>The combined usage.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage AddCapacity(CapacityUsage left, CapacityUsage right) =>
        new(checked(left.Records + right.Records), checked(left.EncodedBytes + right.EncodedBytes));

    /// <summary>Gets the capacity difference between two retained values.</summary>
    /// <param name="current">The current retained value.</param>
    /// <param name="replacement">The replacement retained value.</param>
    /// <returns>The replacement delta.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage CapacityDifference(CapacityUsage current, CapacityUsage replacement) =>
        new(checked(replacement.Records - current.Records), checked(replacement.EncodedBytes - current.EncodedBytes));

    /// <summary>Returns the encoded byte count for a nullable timestamp.</summary>
    /// <param name="timestamp">The timestamp.</param>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long DateTimeOffsetBytes(DateTimeOffset? timestamp) => timestamp.HasValue ? DateTimeOffsetEncodedBytes : 0;

    /// <summary>Returns the encoded byte count for a nullable GUID.</summary>
    /// <param name="value">The GUID value.</param>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GuidBytes(Guid? value) => value.HasValue ? GuidEncodedBytes : 0;

    /// <summary>Returns the retained inbox key capacity.</summary>
    /// <param name="key">The inbox key.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage InboxKeyCapacity(InboxKey key) =>
        new(1, checked(StreamIdBytes(key.StreamId) + GuidEncodedBytes + DateTimeOffsetEncodedBytes));

    /// <summary>Returns the retained receive inclusion marker capacity.</summary>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage InclusionCapacity() =>
        new(1, GuidEncodedBytes);

    /// <summary>Returns the retained lease capacity.</summary>
    /// <param name="lease">The lease record.</param>
    /// <returns>The retained capacity.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage LeaseRecordCapacity(LeaseRecord lease) =>
        LeaseRecordCapacity(lease.OperationIds.Count);

    /// <summary>Returns the retained lease capacity for a member count.</summary>
    /// <param name="operationCount">The leased operation count.</param>
    /// <returns>The retained capacity.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage LeaseRecordCapacity(int operationCount) =>
        new(
            checked(1 + operationCount),
            checked(GuidEncodedBytes + DateTimeOffsetEncodedBytes + (GuidEncodedBytes * operationCount)));

    /// <summary>Returns the retained snapshot capacity.</summary>
    /// <param name="snapshot">The optional snapshot.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage LocalSnapshotCapacity(LocalSnapshot? snapshot) =>
        snapshot is null
            ? default
            : new(
                1,
                checked(
                    StreamIdBytes(snapshot.StreamId)
                    + Int32EncodedBytes
                    + StringBytes(snapshot.ServerCursor)
                    + PayloadCapacityBytes(snapshot.State)
                    + OptionalPayloadCapacityBytes(snapshot.AuthoritativeState)
                    + Int64EncodedBytes
                    + DateTimeOffsetEncodedBytes));

    /// <summary>Returns the retained operation record capacity.</summary>
    /// <param name="record">The operation record.</param>
    /// <returns>The retained capacity.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage OperationRecordCapacity(OperationRecord record) =>
        OperationRecordCapacity(record, record.Status, record.RetryState, record.LeaseId, record.LeaseExpiresAtUtc, record.TerminalAtUtc);

    /// <summary>Returns the retained operation record capacity with proposed mutable values.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="status">The operation status.</param>
    /// <param name="retryState">The retry state.</param>
    /// <param name="leaseId">The current lease identifier.</param>
    /// <param name="leaseExpiresAtUtc">The current lease expiry.</param>
    /// <param name="terminalAtUtc">The terminal timestamp.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage OperationRecordCapacity(
        OperationRecord record,
        SyncOperationStatus status,
        RetryState? retryState,
        Guid? leaseId,
        DateTimeOffset? leaseExpiresAtUtc,
        DateTimeOffset? terminalAtUtc)
    {
        var metadata = MetadataCapacity(record.Operation.Metadata);
        var retry = RetryStateCapacity(retryState);
        var bytes = checked(
            OperationCapacityBytes(record.Operation)
            + SnapshotMutationCapacityBytes(record.SnapshotMutation)
            + LocalCommitResultCapacityBytes()
            + SyncOperationStatusCapacityBytes(status)
            + Int32EncodedBytes
            + GuidBytes(leaseId)
            + DateTimeOffsetBytes(leaseExpiresAtUtc)
            + DateTimeOffsetBytes(terminalAtUtc));
        return AddCapacity(new(checked(1 + metadata.Records), checked(bytes + metadata.EncodedBytes)), retry);
    }

    /// <summary>Returns encoded bytes for an operation and its immutable data.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The encoded byte count.</returns>
    private static long OperationCapacityBytes(SyncOperation operation) =>
        checked(
            GuidEncodedBytes
            + StreamIdBytes(operation.StreamId)
            + Int64EncodedBytes
            + DateTimeOffsetEncodedBytes
            + StringBytes(operation.BaseVersion)
            + EnumEncodedBytes
            + PayloadCapacityBytes(operation.Payload)
            + OperationPolicyCapacityBytes());

    /// <summary>Returns the retained payload envelope byte count.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The encoded byte count.</returns>
    private static long PayloadCapacityBytes(PayloadEnvelope payload) =>
        checked(StringBytes(payload.ContractId) + Int32EncodedBytes + StringBytes(payload.ContentType) + Int32EncodedBytes + payload.PayloadLength + StringBytes(payload.PayloadHash));

    /// <summary>Returns the retained optional payload envelope byte count.</summary>
    /// <param name="payload">The optional payload.</param>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long OptionalPayloadCapacityBytes(PayloadEnvelope? payload) =>
        payload is null ? 0 : PayloadCapacityBytes(payload);

    /// <summary>Returns the retained retry state capacity.</summary>
    /// <param name="retryState">The retry state.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage RetryStateCapacity(RetryState? retryState) =>
        retryState is null
            ? default
            : new(
                1,
                checked(
                    DateTimeOffsetEncodedBytes
                    + DateTimeOffsetBytes(retryState.DueUtc)
                    + TimeSpanBytes(retryState.PreviousDelay)
                    + Int32EncodedBytes
                    + EnumEncodedBytes
                    + StringBytes(retryState.CredentialsVersion)));

    /// <summary>Returns retained stream record capacity.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="stream">The stream record.</param>
    /// <returns>The retained capacity.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CapacityUsage StreamRecordCapacity(StreamId streamId, StreamRecord stream) =>
        AddCapacity(
            new(1, checked(StreamIdBytes(streamId) + GuidEncodedBytes + Int64EncodedBytes + StringBytes(stream.ServerCursor))),
            LocalSnapshotCapacity(stream.Snapshot));

    /// <summary>Returns the retained client identity binding capacity.</summary>
    /// <param name="clientId">The client identity.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage ClientIdentityBindingCapacity(string clientId) =>
        new(1, checked(StringBytes(ClientIdentityBindingMetadataKey) + StringBytes(clientId)));

    /// <summary>Returns the encoded byte count for a stream identifier.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long StreamIdBytes(StreamId streamId) => StringBytes(streamId.Value);

    /// <summary>Returns the encoded byte count for a nullable string.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The UTF-8 byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int StringBytes(string? value) => value is null ? 0 : Encoding.UTF8.GetByteCount(value);

    /// <summary>Returns the encoded byte count for a nullable duration.</summary>
    /// <param name="duration">The duration.</param>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long TimeSpanBytes(TimeSpan? duration) => duration.HasValue ? TimeSpanEncodedBytes : 0;

    /// <summary>Returns the retained store identity capacity.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage StoreIdentityCapacity(string storeIdentity) =>
        new(1, StringBytes(storeIdentity));

    /// <summary>Returns the retained metadata dictionary capacity.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <returns>The retained capacity.</returns>
    private static CapacityUsage MetadataCapacity(IReadOnlyDictionary<string, string> metadata)
    {
        var bytes = 0L;
        foreach (var pair in metadata)
        {
            bytes = checked(bytes + StringBytes(pair.Key) + StringBytes(pair.Value));
        }

        return new(metadata.Count, bytes);
    }

    /// <summary>Returns the encoded byte count for a local commit result.</summary>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long LocalCommitResultCapacityBytes() =>
        GuidEncodedBytes + Int64EncodedBytes + Int64EncodedBytes + DateTimeOffsetEncodedBytes;

    /// <summary>Returns the encoded byte count for an operation policy.</summary>
    /// <returns>The encoded byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long OperationPolicyCapacityBytes() =>
        EnumEncodedBytes + EnumEncodedBytes + Int32EncodedBytes + EnumEncodedBytes;

    /// <summary>Returns the encoded byte count for a snapshot mutation.</summary>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The encoded byte count.</returns>
    private static long SnapshotMutationCapacityBytes(SnapshotMutation snapshotMutation) =>
        checked(
            StreamIdBytes(snapshotMutation.StreamId)
            + PayloadCapacityBytes(snapshotMutation.State)
            + OptionalPayloadCapacityBytes(snapshotMutation.AuthoritativeState)
            + Int32EncodedBytes
            + Int64EncodedBytes);

    /// <summary>Returns the encoded byte count for an operation status.</summary>
    /// <param name="status">The operation status.</param>
    /// <returns>The encoded byte count.</returns>
    private static long SyncOperationStatusCapacityBytes(SyncOperationStatus status) =>
        checked(GuidEncodedBytes + StreamIdBytes(status.StreamId) + EnumEncodedBytes + Int32EncodedBytes + DateTimeOffsetEncodedBytes + StringBytes(status.ReasonCode));

    /// <summary>Validates local commit version inputs.</summary>
    /// <param name="stream">The stream record.</param>
    /// <param name="operation">The local operation.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="InvalidOperationException">The stream head or snapshot revision is stale.</exception>
    private static void ValidateCommitVersion(StreamRecord stream, SyncOperation operation, SnapshotMutation snapshotMutation)
    {
        _ = operation.ClientSequence != stream.NextClientSequence
            ? throw new InvalidOperationException("The client sequence does not match the stream head.")
            : true;
        var currentRevision = stream.Snapshot?.Revision ?? 0;
        _ = snapshotMutation.ExpectedRevision != currentRevision
            ? throw new InvalidOperationException("The snapshot revision does not match the expected revision.")
            : true;
    }

    /// <summary>Validates remote batch version inputs.</summary>
    /// <param name="stream">The stream record.</param>
    /// <param name="batch">The remote batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="InvalidOperationException">The cursor or snapshot revision is stale.</exception>
    private static void ValidateRemoteVersion(StreamRecord stream, RemoteEventBatch batch, SnapshotMutation snapshotMutation)
    {
        _ = !string.Equals(stream.ServerCursor, batch.PreviousCursor, StringComparison.Ordinal)
            ? throw new InvalidOperationException("The stream cursor does not match the remote batch previous cursor.")
            : true;
        var currentRevision = stream.Snapshot?.Revision ?? 0;
        _ = snapshotMutation.ExpectedRevision != currentRevision
            ? throw new InvalidOperationException("The snapshot revision does not match the expected revision.")
            : true;
    }

    /// <summary>Adds one operation to recovery output.</summary>
    /// <param name="streamId">The requested stream identifier.</param>
    /// <param name="record">The operation record.</param>
    /// <param name="includedOperations">The operations already included by an authoritative receive batch.</param>
    /// <param name="pending">The pending operation output.</param>
    /// <param name="replay">The replay operation output.</param>
    /// <param name="deadLetters">The dead-letter output.</param>
    /// <exception cref="InvalidOperationException">A dead-lettered record has no reason code.</exception>
    private static void AddRecoveredOperation(
        StreamId streamId,
        OperationRecord record,
        HashSet<OperationId> includedOperations,
        List<SyncOperation> pending,
        List<SyncOperation> replay,
        List<DeadLetterRecord> deadLetters)
    {
        if (record.Operation.StreamId != streamId)
        {
            return;
        }

        if (record.Status.State == SyncOperationState.DeadLettered)
        {
            var reasonCode = record.Status.ReasonCode;
            var deadLetteredAtUtc = record.TerminalAtUtc.GetValueOrDefault();
            ArgumentExceptionHelper.ThrowIfNull(reasonCode);
            deadLetters.Add(new(record.Operation, reasonCode, record.Status.Attempt, deadLetteredAtUtc));
            return;
        }

        var included = includedOperations.Contains(record.Operation.OperationId);
        if (ShouldRecoverPendingOperation(record))
        {
            pending.Add(record.Operation);
        }

        if (!ShouldRecoverReplayOperation(record, included))
        {
            return;
        }

        replay.Add(record.Operation);
    }

    /// <summary>Creates retry state for a retryable outcome.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="status">The new status.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The retry state to store.</returns>
    private static RetryState? CreateRetryState(OperationRecord record, SyncOperationStatus status, DateTimeOffset nowUtc) =>
        status.State == SyncOperationState.QueuedForUpload ? record.RetryState ?? RetryState.Start(nowUtc) : null;

    /// <summary>Returns the retained capacity delta for an operation status update.</summary>
    /// <param name="record">The operation record.</param>
    /// <param name="status">The operation status.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The retained capacity delta.</returns>
    private static CapacityUsage GetStatusCapacityDelta(OperationRecord record, SyncOperationStatus status, DateTimeOffset nowUtc)
    {
        var retryState = CreateRetryState(record, status, nowUtc);
        var terminalAtUtc = GetTerminalTimestamp(status, record.TerminalAtUtc, nowUtc);
        return CapacityDifference(
            OperationRecordCapacity(record),
            OperationRecordCapacity(record, status, retryState, null, null, terminalAtUtc));
    }

    /// <summary>Returns the terminal timestamp retained by a status transition.</summary>
    /// <param name="status">The operation status.</param>
    /// <param name="currentTerminalAtUtc">The current terminal timestamp.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The terminal timestamp to retain.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset? GetTerminalTimestamp(
        SyncOperationStatus status,
        DateTimeOffset? currentTerminalAtUtc,
        DateTimeOffset nowUtc) =>
        IsBlockingHead(status.State) || IsDefinitiveTerminal(status.State) ? nowUtc : currentTerminalAtUtc;

    /// <summary>Applies a retained capacity delta.</summary>
    /// <param name="delta">The retained capacity delta.</param>
    private void ApplyCapacity(CapacityUsage delta)
    {
        _recordCount = checked(_recordCount + delta.Records);
        _encodedBytes = checked(_encodedBytes + delta.EncodedBytes);
    }

    /// <summary>Records remote event identifiers in the inbox.</summary>
    /// <param name="batch">The applied remote batch.</param>
    /// <param name="committedAtUtc">The sampled local receipt timestamp.</param>
    private void AddInboxEntries(RemoteEventBatch batch, DateTimeOffset committedAtUtc)
    {
        for (var index = 0; index < batch.Events.Count; index++)
        {
            var key = new InboxKey(batch.StreamId, batch.Events[index].EventId);
#if NET8_0_OR_GREATER
            _ = _inbox.TryAdd(key, committedAtUtc);
#else
            if (!_inbox.ContainsKey(key))
            {
                _inbox.Add(key, committedAtUtc);
            }
#endif
        }
    }

    /// <summary>Counts events in a remote batch that are not already retained in the inbox.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The number of new events.</returns>
    private int CountNewRemoteEvents(RemoteEventBatch batch)
    {
        var count = 0;
        for (var index = 0; index < batch.Events.Count; index++)
        {
            if (!_inbox.ContainsKey(new(batch.StreamId, batch.Events[index].EventId)))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Returns retained capacity for new authoritative receive inclusion markers.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <returns>The capacity usage.</returns>
    /// <exception cref="InvalidOperationException">A matching completion is invalid for this stream or lacks authoritative state.</exception>
    private CapacityUsage GetReceiveInclusionCapacity(RemoteEventBatch batch, SnapshotMutation snapshotMutation)
    {
        var capacity = default(CapacityUsage);
        if (_clientId is null)
        {
            return capacity;
        }

        for (var index = 0; index < batch.CompletedOperations.Count; index++)
        {
            var origin = batch.CompletedOperations[index].Origin;
            if (!string.Equals(origin.ClientId, _clientId, StringComparison.Ordinal))
            {
                continue;
            }

            if (!_operations.TryGetValue(origin.OperationId, out var record))
            {
                continue;
            }

            if (record.Operation.StreamId != batch.StreamId)
            {
                throw new InvalidOperationException("A completed local operation belongs to another stream.");
            }

            if (snapshotMutation.AuthoritativeState is null)
            {
                throw new InvalidOperationException("Authoritative state is required to include a completed local operation.");
            }

            if (!_includedOperations.Contains(origin.OperationId))
            {
                capacity = AddCapacity(capacity, InclusionCapacity());
            }
        }

        return capacity;
    }

    /// <summary>Marks authoritative local completions as included after validation and capacity reservation.</summary>
    /// <param name="batch">The batch.</param>
    private void MarkIncludedOperations(RemoteEventBatch batch)
    {
        if (_clientId is null)
        {
            return;
        }

        for (var index = 0; index < batch.CompletedOperations.Count; index++)
        {
            var origin = batch.CompletedOperations[index].Origin;
            if (string.Equals(origin.ClientId, _clientId, StringComparison.Ordinal) && _operations.ContainsKey(origin.OperationId))
            {
                _ = _includedOperations.Add(origin.OperationId);
            }
        }
    }

    /// <summary>Applies sync result statuses and releases lease ownership.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="statuses">The statuses by operation identifier.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    private void ApplyStatusesAndReleaseLease(Guid leaseId, Dictionary<OperationId, SyncOperationStatus> statuses, DateTimeOffset nowUtc)
    {
        var capacity = GetSyncResultCapacityDelta(leaseId, statuses, nowUtc);
        EnsureCapacityFor(capacity);
        ApplySyncResultMutations(statuses, nowUtc);
        _ = _leases.Remove(leaseId);
        ApplyCapacity(capacity);
    }

    /// <summary>Applies sync result state after capacity has been reserved.</summary>
    /// <param name="statuses">The statuses by operation identifier.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    private void ApplySyncResultMutations(
        Dictionary<OperationId, SyncOperationStatus> statuses,
        DateTimeOffset nowUtc)
    {
        foreach (var pair in statuses)
        {
            var record = _operations[pair.Key];
            record.Status = pair.Value;
            record.RetryState = CreateRetryState(record, pair.Value, nowUtc);
            record.LeaseId = null;
            record.LeaseExpiresAtUtc = null;
            record.TerminalAtUtc = GetTerminalTimestamp(pair.Value, record.TerminalAtUtc, nowUtc);
        }
    }

    /// <summary>Returns the retained capacity delta for a sync result.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="statuses">The statuses by operation identifier.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The retained capacity delta.</returns>
    private CapacityUsage GetSyncResultCapacityDelta(
        Guid leaseId,
        Dictionary<OperationId, SyncOperationStatus> statuses,
        DateTimeOffset nowUtc)
    {
        var capacity = default(CapacityUsage);
        foreach (var pair in statuses)
        {
            var record = _operations[pair.Key];
            capacity = AddCapacity(capacity, GetStatusCapacityDelta(record, pair.Value, nowUtc));
        }

        var lease = _leases[leaseId];
        var leaseCapacity = LeaseRecordCapacity(lease);
        return AddCapacity(capacity, new(checked(-leaseCapacity.Records), checked(-leaseCapacity.EncodedBytes)));
    }

    /// <summary>Builds status updates from a remote sync result.</summary>
    /// <param name="result">The sync result.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <returns>The statuses by operation identifier.</returns>
    private Dictionary<OperationId, SyncOperationStatus> CreateStatusesFromResult(
        RemoteSyncResult result,
        DateTimeOffset nowUtc)
    {
        Dictionary<OperationId, SyncOperationStatus> statuses = [];
        for (var index = 0; index < result.Operations.Count; index++)
        {
            var resultOperation = result.Operations[index];
            var operation = _operations[resultOperation.OperationId].Operation;
            var status = CreateStatus(operation, GetResultState(resultOperation.Kind), GetOperation(operation.OperationId).Attempt, nowUtc, resultOperation.ReasonCode);
            statuses.Add(operation.OperationId, status);
        }

        return statuses;
    }

    /// <summary>Gets an active lease or throws.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <returns>The active lease.</returns>
    /// <exception cref="InvalidOperationException">The lease is inactive or expired.</exception>
    private LeaseRecord GetActiveLease(Guid leaseId, DateTimeOffset nowUtc)
    {
        if (!_leases.TryGetValue(leaseId, out var lease))
        {
            throw new InvalidOperationException("The lease is not active.");
        }

        if (lease.ExpiresAtUtc > nowUtc)
        {
            return lease;
        }

        ReleaseExpiredLease(leaseId, lease);
        throw new InvalidOperationException("The lease is expired.");
    }

    /// <summary>Gets compactable operation records.</summary>
    /// <param name="request">The compaction request.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The compactable records.</returns>
    private List<OperationRecord> GetCompactableOperations(CompactionRequest request, DateTimeOffset nowUtc)
    {
        HashSet<StreamId> protectedStreams = [];
        foreach (var pair in _operations)
        {
            if (ShouldRecoverReplayOperation(pair.Value, _includedOperations.Contains(pair.Value.Operation.OperationId)))
            {
                _ = protectedStreams.Add(pair.Value.Operation.StreamId);
            }
        }

        List<OperationRecord> records = [];
        foreach (var pair in _operations)
        {
            var record = pair.Value;
            var matchesStream = !request.StreamId.HasValue || record.Operation.StreamId == request.StreamId.Value;
            if (matchesStream && !protectedStreams.Contains(record.Operation.StreamId)
                && record.Status.ChangedAtUtc < request.RetainTerminalRecordsAfter
                && nowUtc - record.Status.ChangedAtUtc >= _retentionOptions.OutboxTerminalRetention)
            {
                records.Add(record);
            }
        }

        return records;
    }

    /// <summary>Gets leased operations in lease order.</summary>
    /// <param name="lease">The lease record.</param>
    /// <returns>The leased operations.</returns>
    private ReadOnlyCollection<SyncOperation> GetLeaseOperations(LeaseRecord lease)
    {
        List<SyncOperation> operations = [];
        for (var index = 0; index < lease.OperationIds.Count; index++)
        {
            operations.Add(_operations[lease.OperationIds[index]].Operation);
        }

        return new(operations);
    }

    /// <summary>Gets an operation record or throws.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The operation record.</returns>
    /// <exception cref="InvalidOperationException">The operation does not exist.</exception>
    private OperationRecord GetOperation(OperationId operationId) =>
        _operations.TryGetValue(operationId, out var record)
            ? record
            : throw new InvalidOperationException("The operation does not exist.");

    /// <summary>Gets one stream's operation records.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The sorted operation records.</returns>
    private List<OperationRecord> GetStreamOperations(StreamId streamId)
    {
        List<OperationRecord> records = [];
        foreach (var pair in _operations)
        {
            if (pair.Value.Operation.StreamId == streamId)
            {
                records.Add(pair.Value);
            }
        }

        records.Sort(CompareOperationRecordSequence);
        return records;
    }

    /// <summary>Gets a registered stream record or throws.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The stream record.</returns>
    /// <exception cref="InvalidOperationException">The stream is not registered.</exception>
    private StreamRecord GetStream(StreamId streamId) =>
        _streams.TryGetValue(streamId, out var stream)
            ? stream
            : throw new InvalidOperationException("The stream has not been registered.");

    /// <summary>Throws when the pending capacity delta would be exceeded.</summary>
    /// <param name="delta">The retained capacity delta.</param>
    /// <exception cref="QueueCapacityExceededException">The retained records or bytes would exceed capacity.</exception>
    private void EnsureCapacityFor(CapacityUsage delta)
    {
        if (delta.Records <= _maximumRecordCount - _recordCount && delta.EncodedBytes <= _maximumEncodedBytes - _encodedBytes)
        {
            return;
        }

        var canFitWhenEmpty = delta.Records <= _maximumRecordCount && delta.EncodedBytes <= _maximumEncodedBytes;
        throw new QueueCapacityExceededException("The in-memory local store capacity would be exceeded.", canFitWhenEmpty);
    }

    /// <summary>Validates or establishes the client identity binding for an initialized in-memory partition.</summary>
    /// <param name="clientId">The requested client identity.</param>
    /// <exception cref="InvalidOperationException">The requested binding conflicts with existing state.</exception>
    private void ValidateClientBinding(string? clientId)
    {
        if (_clientId is not null)
        {
            if (clientId is not null && string.Equals(_clientId, clientId, StringComparison.Ordinal))
            {
                return;
            }

            throw new InvalidOperationException("The in-memory local store partition is bound to another client identity.");
        }

        if (clientId is null)
        {
            return;
        }

        if (HasMutablePartitionState())
        {
            throw new InvalidOperationException("An existing unbound in-memory local store partition has state and cannot be assigned to a client identity.");
        }

        var capacity = ClientIdentityBindingCapacity(clientId);
        EnsureCapacityFor(capacity);
        _clientId = clientId;
        ApplyCapacity(capacity);
    }

    /// <summary>Determines whether an unbound in-memory partition contains state beyond empty subscription mappings.</summary>
    /// <returns>Whether the partition contains mutable state.</returns>
    /// <remarks>Every local or remote commit creates a snapshot atomically. Compaction always retains that snapshot.</remarks>
    private bool HasMutablePartitionState()
    {
        foreach (var pair in _streams)
        {
            if (pair.Value.Snapshot is not null)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Leases at most one pending operation batch.</summary>
    /// <param name="request">The lease request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, if present.</returns>
    private LeasedOperationBatch? LeasePendingOperationBatch(OutboxLeaseRequest request, CancellationToken cancellationToken)
    {
        InMemoryLocalStoreAdapterValidation.ValidateLeaseRequest(request);
        cancellationToken.ThrowIfCancellationRequested();
        var nowUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            ThrowIfReady(cancellationToken);
            ReclaimExpiredLeases(nowUtc);
            var selected = SelectOperations(request, nowUtc, cancellationToken);
            return selected.Count == 0 ? null : CreateLease(request, selected, nowUtc);
        }
    }

    /// <summary>Creates a lease for selected operation records.</summary>
    /// <param name="request">The lease request.</param>
    /// <param name="selected">The selected records.</param>
    /// <param name="nowUtc">The current timestamp.</param>
    /// <returns>The leased batch.</returns>
    private LeasedOperationBatch CreateLease(OutboxLeaseRequest request, List<OperationRecord> selected, DateTimeOffset nowUtc)
    {
        var leaseId = Guid.NewGuid();
        var expiresAtUtc = CheckedAdd(nowUtc, request.LeaseDuration, nameof(request));
        List<OperationId> operationIds = [];
        List<SyncOperation> operations = [];
        for (var index = 0; index < selected.Count; index++)
        {
            operationIds.Add(selected[index].Operation.OperationId);
            operations.Add(selected[index].Operation);
        }

        var lease = new LeaseRecord(leaseId, expiresAtUtc, operationIds);
        var capacity = LeaseRecordCapacity(lease);
        for (var index = 0; index < selected.Count; index++)
        {
            var status = CreateStatus(selected[index].Operation, SyncOperationState.QueuedForUpload, selected[index].Attempt, nowUtc, null);
            capacity = AddCapacity(
                capacity,
                CapacityDifference(
                    OperationRecordCapacity(selected[index]),
                    OperationRecordCapacity(selected[index], status, selected[index].RetryState, leaseId, expiresAtUtc, selected[index].TerminalAtUtc)));
        }

        EnsureCapacityFor(capacity);
        for (var index = 0; index < selected.Count; index++)
        {
            selected[index].LeaseId = leaseId;
            selected[index].LeaseExpiresAtUtc = expiresAtUtc;
            selected[index].Status = CreateStatus(selected[index].Operation, SyncOperationState.QueuedForUpload, selected[index].Attempt, nowUtc, null);
        }

        _leases.Add(leaseId, lease);
        ApplyCapacity(capacity);
        return new(leaseId, expiresAtUtc, operations);
    }

    /// <summary>Removes compactable operation records.</summary>
    /// <param name="removable">The removable records.</param>
    /// <param name="request">The compaction request.</param>
    /// <returns>The compaction result.</returns>
    private CompactionResult RemoveCompactedOperations(List<OperationRecord> removable, CompactionRequest request)
    {
        var recordsRemoved = 0L;
        var bytesReclaimed = 0L;
        for (var index = 0; index < removable.Count; index++)
        {
            if (_encodedBytes <= request.TargetBytes)
            {
                break;
            }

            _ = _operations.Remove(removable[index].Operation.OperationId);
            recordsRemoved++;
            var capacity = OperationRecordCapacity(removable[index]);
            if (_includedOperations.Remove(removable[index].Operation.OperationId))
            {
                capacity = AddCapacity(capacity, InclusionCapacity());
            }

            bytesReclaimed = checked(bytesReclaimed + capacity.EncodedBytes);
            ApplyCapacity(new(checked(-capacity.Records), checked(-capacity.EncodedBytes)));
        }

        return new(recordsRemoved, bytesReclaimed);
    }

    /// <summary>Reclaims expired leases.</summary>
    /// <param name="nowUtc">The current timestamp.</param>
    private void ReclaimExpiredLeases(DateTimeOffset nowUtc)
    {
        List<Guid> expiredLeaseIds = [];
        foreach (var pair in _leases)
        {
            if (pair.Value.ExpiresAtUtc <= nowUtc)
            {
                expiredLeaseIds.Add(pair.Key);
            }
        }

        for (var index = 0; index < expiredLeaseIds.Count; index++)
        {
            ReleaseExpiredLease(expiredLeaseIds[index], _leases[expiredLeaseIds[index]]);
        }
    }

    /// <summary>Releases one expired lease.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="lease">The lease record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReleaseExpiredLease(Guid leaseId, LeaseRecord lease) => ReleaseLeaseCore(leaseId, lease);

    /// <summary>Releases one retained lease and clears matching operation ownership.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="lease">The lease record.</param>
    private void ReleaseLeaseCore(Guid leaseId, LeaseRecord lease)
    {
        var leaseCapacity = LeaseRecordCapacity(lease);
        var capacity = new CapacityUsage(checked(-leaseCapacity.Records), checked(-leaseCapacity.EncodedBytes));
        for (var index = 0; index < lease.OperationIds.Count; index++)
        {
            var record = _operations[lease.OperationIds[index]];
            capacity = AddCapacity(
                capacity,
                CapacityDifference(
                    OperationRecordCapacity(record),
                    OperationRecordCapacity(record, record.Status, record.RetryState, null, null, record.TerminalAtUtc)));
        }

        for (var index = 0; index < lease.OperationIds.Count; index++)
        {
            var record = _operations[lease.OperationIds[index]];
            record.LeaseId = null;
            record.LeaseExpiresAtUtc = null;
        }

        _ = _leases.Remove(leaseId);
        ApplyCapacity(capacity);
    }

    /// <summary>Selects operation records for one lease.</summary>
    /// <param name="request">The lease request.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The selected records.</returns>
    private List<OperationRecord> SelectOperations(OutboxLeaseRequest request, DateTimeOffset nowUtc, CancellationToken cancellationToken)
    {
        var streams = CreateCandidateStreams(request);
        for (var index = 0; index < streams.Count; index++)
        {
            var selected = SelectStreamOperations(streams[index], request, nowUtc, cancellationToken);
            if (selected.Count > 0)
            {
                return selected;
            }
        }

        return [];
    }

    /// <summary>Creates candidate streams for lease selection.</summary>
    /// <param name="request">The lease request.</param>
    /// <returns>The candidate streams.</returns>
    private List<StreamId> CreateCandidateStreams(OutboxLeaseRequest request)
    {
        if (request.StreamId.HasValue)
        {
            return [request.StreamId.Value];
        }

        List<StreamId> streams = [];
        foreach (var pair in _operations)
        {
            if (!streams.Contains(pair.Value.Operation.StreamId))
            {
                streams.Add(pair.Value.Operation.StreamId);
            }
        }

        streams.Sort(CompareStreamId);
        return streams;
    }

    /// <summary>Selects eligible operations for one stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="request">The lease request.</param>
    /// <param name="nowUtc">The sampled current timestamp.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The selected records.</returns>
    private List<OperationRecord> SelectStreamOperations(
        StreamId streamId,
        OutboxLeaseRequest request,
        DateTimeOffset nowUtc,
        CancellationToken cancellationToken)
    {
        var records = GetStreamOperations(streamId);
        List<OperationRecord> selected = [];
        var bytes = 0L;
        for (var index = 0; index < records.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = records[index];
            if (IsDefinitiveTerminal(record.Status.State))
            {
                continue;
            }

            if (IsBlockedForLease(record, nowUtc))
            {
                break;
            }

            var nextBytes = checked(bytes + record.Operation.Payload.PayloadLength);
            if (selected.Count >= request.MaximumOperations || nextBytes > request.MaximumBytes)
            {
                break;
            }

            selected.Add(record);
            bytes = nextBytes;
        }

        return selected;
    }

    /// <summary>Throws when this instance is disposed or not initialized.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="InvalidOperationException">The store has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">This instance has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The operation is canceled.</exception>
    private void ThrowIfReady(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        if (_storeIdentity is not null)
        {
            return;
        }

        throw new InvalidOperationException("The in-memory local store must be initialized before use.");
    }

    /// <summary>Throws when this instance has been disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
}
