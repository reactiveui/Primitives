// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates in-memory local store input.</summary>
internal static class InMemoryLocalStoreAdapterValidation
{
    /// <summary>The maximum accepted dead-letter reason code length in UTF-8 bytes.</summary>
    private const int MaximumDeadLetterReasonBytes = 1024;

    /// <summary>Strict UTF-8 encoder for validating reason text before persistence.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Validates a snapshot mutation.</summary>
    /// <param name="snapshotMutation">The mutation.</param>
    /// <exception cref="ArgumentException">The mutation is malformed.</exception>
    /// <exception cref="ArgumentNullException">The mutation is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The format version or revision is invalid.</exception>
    internal static void ValidateSnapshotMutation(SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        ValidateStreamId(snapshotMutation.StreamId, nameof(snapshotMutation));
        ValidatePayload(snapshotMutation.State, nameof(snapshotMutation));
        if (snapshotMutation.AuthoritativeState is { } authoritativeState)
        {
            ValidatePayload(authoritativeState, nameof(snapshotMutation));
        }

        _ = snapshotMutation.FormatVersion <= 0
            ? throw new ArgumentOutOfRangeException(nameof(snapshotMutation), snapshotMutation.FormatVersion, "Snapshot format version must be positive.")
            : true;
        _ = snapshotMutation.ExpectedRevision < 0
            ? throw new ArgumentOutOfRangeException(nameof(snapshotMutation), snapshotMutation.ExpectedRevision, "Snapshot expected revision must be non-negative.")
            : true;
    }

    /// <summary>Determines whether two commit attempts describe the same immutable intent.</summary>
    /// <param name="existingOperation">The existing operation.</param>
    /// <param name="requestedOperation">The requested operation.</param>
    /// <param name="existingMutation">The existing snapshot mutation.</param>
    /// <param name="requestedMutation">The requested snapshot mutation.</param>
    /// <returns>Whether both attempts have the same canonical intent.</returns>
    internal static bool HasSameIntent(
        SyncOperation existingOperation,
        SyncOperation requestedOperation,
        SnapshotMutation existingMutation,
        SnapshotMutation requestedMutation) =>
        HasSameOperationIntent(existingOperation, requestedOperation) && HasSameSnapshotIntent(existingMutation, requestedMutation);

    /// <summary>Validates remote attempt input.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nextAttempt">The next attempt number.</param>
    /// <exception cref="ArgumentException">An identifier is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The attempt number is not positive.</exception>
    internal static void ValidateAttemptInput(Guid leaseId, OperationId operationId, int nextAttempt)
    {
        ValidateLeaseId(leaseId);
        ValidateOperationId(operationId, nameof(operationId));
        _ = nextAttempt <= 0 ? throw new ArgumentOutOfRangeException(nameof(nextAttempt), nextAttempt, "Attempt must be positive.") : true;
    }

    /// <summary>Validates local commit input.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="ArgumentException">The operation or snapshot is malformed.</exception>
    /// <exception cref="ArgumentNullException">An input is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A version is not positive.</exception>
    internal static void ValidateCommitInput(SyncOperation operation, SnapshotMutation snapshotMutation)
    {
        ValidateOperation(operation);
        ValidateSnapshotMutation(snapshotMutation);
        _ = operation.StreamId != snapshotMutation.StreamId
            ? throw new ArgumentException("The operation and snapshot mutation must target the same stream.", nameof(snapshotMutation))
            : true;
    }

    /// <summary>Validates a local dead-letter operation input.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="snapshotMutation">The replacement snapshot mutation.</param>
    /// <exception cref="ArgumentException">An input is malformed.</exception>
    /// <exception cref="ArgumentNullException">The reason or mutation is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The reason exceeds the supported size.</exception>
    internal static void ValidateDeadLetterInput(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation snapshotMutation)
    {
        ValidateLeaseId(leaseId);
        ValidateOperationId(operationId, nameof(operationId));
        ArgumentExceptionHelper.ThrowIfNull(reasonCode);
        _ = string.IsNullOrWhiteSpace(reasonCode)
            ? throw new ArgumentException("Dead-letter reason code must be non-empty.", nameof(reasonCode))
            : true;
        try
        {
            var reasonBytes = StrictUtf8.GetByteCount(reasonCode);
            _ = reasonBytes > MaximumDeadLetterReasonBytes
                ? throw new ArgumentOutOfRangeException(nameof(reasonCode), reasonBytes, "Dead-letter reason code exceeds the supported size.")
                : true;
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("Dead-letter reason code must be well-formed Unicode.", nameof(reasonCode), exception);
        }

        ValidateSnapshotMutation(snapshotMutation);
    }

    /// <summary>Validates a local dead-letter reason code.</summary>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <exception cref="ArgumentException">The reason is malformed.</exception>
    /// <exception cref="ArgumentNullException">The reason is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The reason exceeds the supported size.</exception>
    internal static void ValidateDeadLetterReasonCode(string reasonCode)
    {
        ArgumentExceptionHelper.ThrowIfNull(reasonCode);
        _ = string.IsNullOrWhiteSpace(reasonCode)
            ? throw new ArgumentException("Dead-letter reason code must be non-empty.", nameof(reasonCode))
            : true;
        var reasonBytes = StrictUtf8.GetByteCount(reasonCode);
        _ = reasonBytes > MaximumDeadLetterReasonBytes
            ? throw new ArgumentOutOfRangeException(nameof(reasonCode), reasonBytes, "Dead-letter reason code exceeds the supported size.")
            : true;
    }

    /// <summary>Validates inbox lookup input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventIds">The event identifiers.</param>
    /// <exception cref="ArgumentException">The stream or an event identifier is empty.</exception>
    /// <exception cref="ArgumentNullException">The event list is null.</exception>
    internal static void ValidateInboxLookupInput(StreamId streamId, IReadOnlyList<Guid> eventIds)
    {
        ValidateStreamId(streamId, nameof(streamId));
        ArgumentExceptionHelper.ThrowIfNull(eventIds);
        for (var index = 0; index < eventIds.Count; index++)
        {
            _ = eventIds[index] == Guid.Empty
                ? throw new ArgumentException("Event identifiers must be non-empty.", nameof(eventIds))
                : true;
        }
    }

    /// <summary>Validates a lease identifier.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <exception cref="ArgumentException">The lease identifier is empty.</exception>
    internal static void ValidateLeaseId(Guid leaseId) =>
        _ = leaseId == Guid.Empty ? throw new ArgumentException("Lease identifier must be non-empty.", nameof(leaseId)) : true;

    /// <summary>Validates lease renewal input.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="extension">The extension duration.</param>
    /// <exception cref="ArgumentException">The lease identifier is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The extension is not positive and finite.</exception>
    internal static void ValidateLeaseRenewalInput(Guid leaseId, TimeSpan extension)
    {
        ValidateLeaseId(leaseId);
        _ = extension <= TimeSpan.Zero || extension == TimeSpan.MaxValue
            ? throw new ArgumentOutOfRangeException(nameof(extension), extension, "Lease extension must be positive and finite.")
            : true;
    }

    /// <summary>Validates a lease request.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The stream identifier is malformed.</exception>
    /// <exception cref="ArgumentNullException">The request is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A lease bound is not positive and finite.</exception>
    internal static void ValidateLeaseRequest(OutboxLeaseRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        if (request.StreamId.HasValue)
        {
            ValidateStreamId(request.StreamId.Value, nameof(request));
        }

        _ = request.MaximumOperations <= 0
            ? throw new ArgumentOutOfRangeException(nameof(request), request.MaximumOperations, "Maximum operations must be positive.")
            : true;
        _ = request.MaximumBytes <= 0
            ? throw new ArgumentOutOfRangeException(nameof(request), request.MaximumBytes, "Maximum bytes must be positive.")
            : true;
        _ = request.LeaseDuration <= TimeSpan.Zero || request.LeaseDuration == TimeSpan.MaxValue
            ? throw new ArgumentOutOfRangeException(nameof(request), request.LeaseDuration, "Lease duration must be positive and finite.")
            : true;
    }

    /// <summary>Validates an operation identifier.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The operation identifier is empty.</exception>
    internal static void ValidateOperationId(OperationId operationId, string parameterName) =>
        _ = operationId.Value == Guid.Empty ? throw new ArgumentException("Operation identifier must be non-empty.", parameterName) : true;

    /// <summary>Validates recovery input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <exception cref="ArgumentException">An identifier is malformed.</exception>
    internal static void ValidateRecoveryInput(StreamId streamId, SubscriptionId subscriptionId)
    {
        ValidateStreamId(streamId, nameof(streamId));
        _ = subscriptionId.Value == Guid.Empty
            ? throw new ArgumentException("Subscription identifier must be non-empty.", nameof(subscriptionId))
            : true;
    }

    /// <summary>Validates retry scheduling input before mutation.</summary>
    /// <param name="retryState">The retry state.</param>
    /// <exception cref="ArgumentNullException">The retry state is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A retry count or delay is negative.</exception>
    /// <exception cref="ArgumentException">The authentication state is invalid.</exception>
    internal static void ValidateRetryState(RetryState retryState)
    {
        ArgumentExceptionHelper.ThrowIfNull(retryState);
        if (retryState.TransientAttemptCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryState), retryState.TransientAttemptCount, "Retry attempt count must not be negative.");
        }

        if (retryState.PreviousDelay is { } delay && delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryState), delay, "Retry delay must not be negative.");
        }

        if (retryState.AuthenticationState is RetryAuthenticationState.None or RetryAuthenticationState.RenewalRetryUsed)
        {
            return;
        }

        throw new ArgumentException("Retry authentication state must be a defined value.", nameof(retryState));
    }

    /// <summary>Validates remote apply input.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="ArgumentException">The batch or mutation is malformed.</exception>
    /// <exception cref="ArgumentNullException">An input is null.</exception>
    internal static void ValidateRemoteApplyInput(RemoteEventBatch batch, SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ValidateStreamId(batch.StreamId, nameof(batch));
        _ = batch.BatchId == Guid.Empty || string.IsNullOrWhiteSpace(batch.NextCursor)
            ? throw new ArgumentException("The remote batch must have a batch identifier and next cursor.", nameof(batch))
            : true;
        ValidateSnapshotMutation(snapshotMutation);
        _ = batch.StreamId != snapshotMutation.StreamId
            ? throw new ArgumentException("The remote batch and snapshot mutation must target the same stream.", nameof(snapshotMutation))
            : true;
        ValidateRemoteEvents(batch);
    }

    /// <summary>Validates a store identity.</summary>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The identity is empty.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ValidateStoreIdentity(string? storeIdentity, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(storeIdentity, parameterName);
        _ = string.IsNullOrWhiteSpace(storeIdentity)
            ? throw new ArgumentException("Store identity must be non-empty.", parameterName)
            : true;
    }

    /// <summary>Validates a stream identifier.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The stream identifier is empty.</exception>
    internal static void ValidateStreamId(StreamId streamId, string parameterName) =>
        _ = streamId.Value is null ? throw new ArgumentException("Stream identifier must be non-empty.", parameterName) : true;

    /// <summary>Determines whether dictionaries have the same ordinal metadata.</summary>
    /// <param name="left">The first dictionary.</param>
    /// <param name="right">The second dictionary.</param>
    /// <returns>Whether the dictionaries match.</returns>
    private static bool HasSameMetadata(IReadOnlyDictionary<string, string> left, IReadOnlyDictionary<string, string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var value) || !string.Equals(pair.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Determines whether two operations have the same canonical intent.</summary>
    /// <param name="left">The first operation.</param>
    /// <param name="right">The second operation.</param>
    /// <returns>Whether the operations match.</returns>
    private static bool HasSameOperationIntent(SyncOperation left, SyncOperation right) =>
        left.OperationId == right.OperationId
        && left.StreamId == right.StreamId
        && left.ClientSequence == right.ClientSequence
        && left.TimestampUtc == right.TimestampUtc
        && string.Equals(left.BaseVersion, right.BaseVersion, StringComparison.Ordinal)
        && left.Type == right.Type
        && left.Policy == right.Policy
        && HasSameMetadata(left.Metadata, right.Metadata)
        && PayloadEnvelopeComparison.ContentEquals(left.Payload, right.Payload);

    /// <summary>Determines whether two snapshot mutations have the same canonical intent.</summary>
    /// <param name="left">The first mutation.</param>
    /// <param name="right">The second mutation.</param>
    /// <returns>Whether the mutations match.</returns>
    private static bool HasSameSnapshotIntent(SnapshotMutation left, SnapshotMutation right) =>
        left.StreamId == right.StreamId
        && left.FormatVersion == right.FormatVersion
        && left.ExpectedRevision == right.ExpectedRevision
        && PayloadEnvelopeComparison.ContentEquals(left.State, right.State)
        && OptionalPayloadEquals(left.AuthoritativeState, right.AuthoritativeState);

    /// <summary>Determines whether two optional payload envelopes contain the same canonical content.</summary>
    /// <param name="left">The first optional payload.</param>
    /// <param name="right">The second optional payload.</param>
    /// <returns>Whether the payloads match.</returns>
    private static bool OptionalPayloadEquals(PayloadEnvelope? left, PayloadEnvelope? right) =>
        left is null ? right is null : right is not null && PayloadEnvelopeComparison.ContentEquals(left, right);

    /// <summary>Validates a synchronization operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <exception cref="ArgumentException">The operation is malformed.</exception>
    /// <exception cref="ArgumentNullException">The operation is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The client sequence is not positive.</exception>
    private static void ValidateOperation(SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ValidateOperationId(operation.OperationId, nameof(operation));
        ValidateStreamId(operation.StreamId, nameof(operation));
        ValidatePayload(operation.Payload, nameof(operation));
        operation.Policy.Validate();
        if (operation.Type is not (SyncOperationType.Append or SyncOperationType.Update or SyncOperationType.Delete or SyncOperationType.Custom))
        {
            throw new ArgumentException("Operation type must be a defined value.", nameof(operation));
        }

        _ = operation.ClientSequence <= 0
            ? throw new ArgumentOutOfRangeException(nameof(operation), operation.ClientSequence, "Client sequence must be positive.")
            : true;
    }

    /// <summary>Validates a payload envelope.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The payload is malformed.</exception>
    /// <exception cref="ArgumentNullException">The payload is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The schema version is not positive.</exception>
    private static void ValidatePayload(PayloadEnvelope payload, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        _ = string.IsNullOrWhiteSpace(payload.ContractId) || string.IsNullOrWhiteSpace(payload.ContentType) || string.IsNullOrWhiteSpace(payload.PayloadHash)
            ? throw new ArgumentException("Payload contract, content type, and hash must be non-empty.", parameterName)
            : true;
        _ = payload.SchemaVersion <= 0
            ? throw new ArgumentOutOfRangeException(parameterName, payload.SchemaVersion, "Payload schema version must be positive.")
            : true;
    }

    /// <summary>Validates one remote event.</summary>
    /// <param name="batch">The containing batch.</param>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="eventIds">The event identifiers already seen in the batch.</param>
    /// <exception cref="ArgumentException">The remote event is malformed.</exception>
    /// <exception cref="ArgumentNullException">The remote event is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The payload schema version is not positive.</exception>
    private static void ValidateRemoteEvent(RemoteEventBatch batch, RemoteEvent remoteEvent, HashSet<Guid> eventIds)
    {
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        _ = remoteEvent.EventId == Guid.Empty || !eventIds.Add(remoteEvent.EventId)
            ? throw new ArgumentException("Remote event identifiers must be non-empty and unique within a batch.", nameof(batch))
            : true;
        _ = remoteEvent.StreamId != batch.StreamId || string.IsNullOrWhiteSpace(remoteEvent.ServerCursor)
            ? throw new ArgumentException("Remote events must match the batch stream and include a server cursor.", nameof(batch))
            : true;
        _ = remoteEvent.CausedByOperationId.HasValue && remoteEvent.CausedByOperationId.Value.Value == Guid.Empty
            ? throw new ArgumentException("Remote event causal operation identifiers must be non-empty.", nameof(batch))
            : true;
        ValidatePayload(remoteEvent.Payload, nameof(batch));
    }

    /// <summary>Validates all remote events in a batch.</summary>
    /// <param name="batch">The remote batch.</param>
    private static void ValidateRemoteEvents(RemoteEventBatch batch)
    {
        HashSet<Guid> eventIds = [];
        for (var index = 0; index < batch.Events.Count; index++)
        {
            ValidateRemoteEvent(batch, batch.Events[index], eventIds);
        }
    }
}
