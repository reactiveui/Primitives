// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates loopback transport input and logical in-process bounds.</summary>
internal static class LoopbackTransportValidator
{
    /// <summary>Stores the SupportedProtocolMajor value used by loopback validation.</summary>
    private const int SupportedProtocolMajor = 1;

    /// <summary>Stores the GuidByteCount value used by loopback validation.</summary>
    private const int GuidByteCount = 16;

    /// <summary>Stores the IntByteCount value used by loopback validation.</summary>
    private const int IntByteCount = 4;

    /// <summary>Stores the LongByteCount value used by loopback validation.</summary>
    private const int LongByteCount = 8;

    /// <summary>Stores the DateTimeOffsetByteCount value used by loopback validation.</summary>
    private const int DateTimeOffsetByteCount = 16;

    /// <summary>Stores the EnumByteCount value used by loopback validation.</summary>
    private const int EnumByteCount = 4;

    /// <summary>Stores the NullableMarkerByteCount value used by loopback validation.</summary>
    private const int NullableMarkerByteCount = 1;

    /// <summary>Stores the KnownFeatures value used by loopback validation.</summary>
    private const RemoteTransportCapabilities KnownFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>Stores the ExactlyOnceFeatures value used by loopback validation.</summary>
    private const RemoteTransportCapabilities ExactlyOnceFeatures = RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge;

    /// <summary>Stores the StrictUtf8 value used by loopback validation.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Runs the ValidateOptions loopback validation step.</summary>
    /// <param name="options">The options value for ValidateOptions.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateOptions.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateOptions.</exception>
    internal static void ValidateOptions(LoopbackTransportAdapterOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options.Hub);
        ArgumentExceptionHelper.ThrowIfNull(options.Client);
        ArgumentExceptionHelper.ThrowIfNull(options.PeerCapabilities);
        ValidatePositive(options.MaximumConcurrentRequests, nameof(options.MaximumConcurrentRequests));
        ValidatePositive(options.MaximumConcurrentAcknowledgements, nameof(options.MaximumConcurrentAcknowledgements));
        ValidatePositive(options.MaximumConcurrentSubscriptions, nameof(options.MaximumConcurrentSubscriptions));
        ValidatePositive(options.MaximumReceiveEvents, nameof(options.MaximumReceiveEvents));
        ValidatePositive(options.MaximumCompletedOperations, nameof(options.MaximumCompletedOperations));
        ValidatePositive(options.MaximumMetadataEntries, nameof(options.MaximumMetadataEntries));
        ValidatePositive(options.MaximumStringBytes, nameof(options.MaximumStringBytes));
        ValidateClientIdentity(options.Client, options.MaximumStringBytes);
        ValidateCapabilities(options.PeerCapabilities);
    }

    /// <summary>Runs the ValidateConnectRequest loopback validation step.</summary>
    /// <param name="request">The request value for ValidateConnectRequest.</param>
    /// <param name="options">The options value for ValidateConnectRequest.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateConnectRequest.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateConnectRequest.</exception>
    internal static void ValidateConnectRequest(TransportConnectRequest request, LoopbackTransportAdapterOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(request.SupportedProtocolVersions);
        ArgumentExceptionHelper.ThrowIfNull(request.Client);
        ArgumentExceptionHelper.ThrowIfNull(request.RequiredGuarantees);
        ValidateClientIdentity(request.Client, options.MaximumStringBytes);
        ValidateTrustedClient(request, options);
        ValidateRequestedProtocol(request.SupportedProtocolVersions, options.PeerCapabilities.ProtocolVersion);
        foreach (var guarantee in request.RequiredGuarantees)
        {
            ValidateGuarantee(guarantee, options.PeerCapabilities);
        }
    }

    /// <summary>Validates an outbound batch after a bounded push slot has been admitted.</summary>
    /// <param name="batch">The outbound batch.</param>
    /// <param name="options">The trusted loopback bounds.</param>
    /// <exception cref="InvalidOperationException">The batch exceeds loopback bounds or contains malformed operations.</exception>
    internal static void ValidateOutgoingBatch(SyncBatch batch, LoopbackTransportAdapterOptions options)
    {
        ValidateOutgoingHeader(batch, options);
        long total = GuidByteCount + IntByteCount;
        StreamId? streamId = null;
        HashSet<OperationId> operationIds = [];
        HashSet<long> clientSequences = [];
        var previousSequence = 0L;
        foreach (var operation in batch.Operations)
        {
            CountOutgoingOperation(operation, options, ref total);
            ValidateOperationMembership(operation, ref streamId, operationIds, clientSequences, ref previousSequence);
        }

        ValidateLogicalByteBound(total, options.PeerCapabilities.MaximumBatchBytes, "The synchronization batch exceeds loopback byte bounds.");
    }

    /// <summary>Runs the ValidateSubscribeRequest loopback validation step.</summary>
    /// <param name="request">The request value for ValidateSubscribeRequest.</param>
    /// <param name="options">The options value for ValidateSubscribeRequest.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateSubscribeRequest.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateSubscribeRequest.</exception>
    internal static void ValidateSubscribeRequest(RemoteSubscribeRequest request, LoopbackTransportAdapterOptions options)
    {
        if (request.StreamId.Value is null || request.SubscriptionId.Value == Guid.Empty || request.InitialPosition is null)
        {
            throw new InvalidOperationException("The loopback subscribe request is malformed.");
        }

        _ = CountRequiredString(request.StreamId.Value, options.MaximumStringBytes, "The loopback subscribe stream is malformed.");
        _ = CountOptionalString(request.Cursor, options.MaximumStringBytes, "The loopback subscribe cursor is malformed.");
        ValidateStartPosition(request.InitialPosition, options);
    }

    /// <summary>Runs the ValidateAcknowledgement loopback validation step.</summary>
    /// <param name="acknowledgement">The acknowledgement value for ValidateAcknowledgement.</param>
    /// <param name="options">The options value for ValidateAcknowledgement.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateAcknowledgement.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateAcknowledgement.</exception>
    internal static void ValidateAcknowledgement(ReceiveAcknowledgement acknowledgement, LoopbackTransportAdapterOptions options)
    {
        if (acknowledgement.SubscriptionId.Value == Guid.Empty || acknowledgement.StreamId.Value is null)
        {
            throw new InvalidOperationException("The loopback acknowledgement is malformed.");
        }

        _ = CountRequiredString(acknowledgement.StreamId.Value, options.MaximumStringBytes, "The loopback acknowledgement stream is malformed.");
        _ = CountRequiredString(acknowledgement.Cursor, options.MaximumStringBytes, "The loopback acknowledgement cursor is malformed.");
    }

    /// <summary>Validates a received batch against loopback bounds and the active subscription cursor chain.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="options">The trusted loopback bounds.</param>
    /// <param name="streamId">The stream requested by the subscription.</param>
    /// <param name="previousCursor">The previous cursor expected by the subscription.</param>
    /// <param name="requiresPreviousCursor">Whether the next batch must match <paramref name="previousCursor"/>.</param>
    /// <exception cref="InvalidOperationException">The batch is malformed or does not belong to the active subscription.</exception>
    internal static void ValidateReceiveBatch(
        RemoteEventBatch batch,
        LoopbackTransportAdapterOptions options,
        StreamId streamId,
        string? previousCursor,
        bool requiresPreviousCursor)
    {
        try
        {
            RemoteEventBatchValidator.Validate(batch, options.MaximumReceiveEvents, options.MaximumCompletedOperations);
            ValidateReceiveBatchSubscription(batch, streamId, previousCursor, requiresPreviousCursor);
            var total = CountReceiveBatch(batch, options);
            ValidateLogicalByteBound(total, options.PeerCapabilities.MaximumBatchBytes, "The receive batch exceeds loopback byte bounds.");
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException("The hub returned a malformed receive batch.", exception);
        }
    }

    /// <summary>Validates the stream and opaque cursor continuity for one received batch.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="streamId">The stream requested by the subscription.</param>
    /// <param name="previousCursor">The previous cursor expected by the subscription.</param>
    /// <param name="requiresPreviousCursor">Whether cursor continuity has been established.</param>
    /// <exception cref="InvalidOperationException">The batch belongs to another stream or skips the expected cursor.</exception>
    private static void ValidateReceiveBatchSubscription(
        RemoteEventBatch batch,
        StreamId streamId,
        string? previousCursor,
        bool requiresPreviousCursor)
    {
        if (batch.StreamId != streamId)
        {
            throw new InvalidOperationException("The hub returned a receive batch for a different stream.");
        }

        if (!requiresPreviousCursor
            || string.Equals(batch.PreviousCursor, previousCursor, StringComparison.Ordinal)
            || string.Equals(batch.NextCursor, previousCursor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("The hub returned a receive batch outside the subscription cursor order.");
    }

    /// <summary>Runs the ValidateTrustedClient loopback validation step.</summary>
    /// <param name="request">The request value for ValidateTrustedClient.</param>
    /// <param name="options">The options value for ValidateTrustedClient.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateTrustedClient.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateTrustedClient.</exception>
    private static void ValidateTrustedClient(TransportConnectRequest request, LoopbackTransportAdapterOptions options)
    {
        if (string.Equals(request.Client.ClientId, options.Client.ClientId, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("The loopback connection request client does not match the trusted host identity.");
    }

    /// <summary>Runs the ValidateRequestedProtocol loopback validation step.</summary>
    /// <param name="range">The range value for ValidateRequestedProtocol.</param>
    /// <param name="selected">The selected value for ValidateRequestedProtocol.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateRequestedProtocol.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateRequestedProtocol.</exception>
    private static void ValidateRequestedProtocol(VersionRange range, Version selected)
    {
        ArgumentExceptionHelper.ThrowIfNull(range.Minimum);
        ArgumentExceptionHelper.ThrowIfNull(range.Maximum);
        if (range.Minimum.CompareTo(range.Maximum) > 0)
        {
            throw new InvalidOperationException("The loopback connection request protocol range is malformed.");
        }

        if (range.Minimum.CompareTo(selected) <= 0 && range.Maximum.CompareTo(selected) >= 0)
        {
            return;
        }

        throw new InvalidOperationException("The loopback peer protocol is outside the requested range.");
    }

    /// <summary>Runs the ValidateGuarantee loopback validation step.</summary>
    /// <param name="guarantee">The guarantee value for ValidateGuarantee.</param>
    /// <param name="capabilities">The capabilities value for ValidateGuarantee.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateGuarantee.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateGuarantee.</exception>
    private static void ValidateGuarantee(DeliveryGuarantee guarantee, NegotiatedCapabilities capabilities)
    {
        if (guarantee == DeliveryGuarantee.AtMostOnce)
        {
            return;
        }

        if (guarantee == DeliveryGuarantee.AtLeastOnce)
        {
            RequireAtLeastOnce(capabilities);
            return;
        }

        if (guarantee == DeliveryGuarantee.ExactlyOnce)
        {
            RequireExactlyOnce(capabilities);
            return;
        }

        throw new InvalidOperationException("The loopback connection request contains an unknown delivery guarantee.");
    }

    /// <summary>Runs the RequireAtLeastOnce loopback validation step.</summary>
    /// <param name="capabilities">The capabilities value for RequireAtLeastOnce.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during RequireAtLeastOnce.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during RequireAtLeastOnce.</exception>
    private static void RequireAtLeastOnce(NegotiatedCapabilities capabilities)
    {
        if ((capabilities.Features & RemoteTransportCapabilities.ServerIdempotency) != 0)
        {
            return;
        }

        throw new InvalidOperationException("The loopback peer does not support at-least-once idempotency.");
    }

    /// <summary>Runs the RequireExactlyOnce loopback validation step.</summary>
    /// <param name="capabilities">The capabilities value for RequireExactlyOnce.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during RequireExactlyOnce.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during RequireExactlyOnce.</exception>
    private static void RequireExactlyOnce(NegotiatedCapabilities capabilities)
    {
        if ((capabilities.Features & ExactlyOnceFeatures) == ExactlyOnceFeatures && IsPositiveFinite(capabilities.ServerIdempotencyRetention))
        {
            return;
        }

        throw new InvalidOperationException("The loopback peer does not support exactly-once delivery.");
    }

    /// <summary>Runs the ValidateCapabilities loopback validation step.</summary>
    /// <param name="capabilities">The capabilities value for ValidateCapabilities.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateCapabilities.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateCapabilities.</exception>
    private static void ValidateCapabilities(NegotiatedCapabilities capabilities)
    {
        ArgumentExceptionHelper.ThrowIfNull(capabilities.ProtocolVersion);
        if (capabilities.ProtocolVersion.Major != SupportedProtocolMajor || capabilities.ProtocolVersion.Minor < 0)
        {
            throw new InvalidOperationException("The loopback peer protocol version is unsupported.");
        }

        if ((capabilities.Features & ~KnownFeatures) != 0)
        {
            throw new InvalidOperationException("The loopback peer capabilities contain an unknown feature flag.");
        }

        ValidatePositive(capabilities.MaximumBatchOperations, nameof(capabilities.MaximumBatchOperations));
        ValidatePositiveBatchBytes(capabilities.MaximumBatchBytes);
        ValidateOptionalRetention(capabilities.ServerIdempotencyRetention, nameof(capabilities.ServerIdempotencyRetention));
        ValidateOptionalRetention(capabilities.ClientInboxRetentionRequired, nameof(capabilities.ClientInboxRetentionRequired));
        ValidateOptionalRetention(capabilities.EffectiveExactlyOnceWindow, nameof(capabilities.EffectiveExactlyOnceWindow));
    }

    /// <summary>Runs the ValidateClientIdentity loopback validation step.</summary>
    /// <param name="client">The client value for ValidateClientIdentity.</param>
    /// <param name="maximumStringBytes">The maximumStringBytes value for ValidateClientIdentity.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateClientIdentity.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateClientIdentity.</exception>
    private static void ValidateClientIdentity(ClientIdentity client, int maximumStringBytes)
    {
        _ = CountRequiredString(client.ClientId, maximumStringBytes, "Client identity is malformed.");
        _ = CountOptionalString(client.TenantHint, maximumStringBytes, "Tenant hint is malformed.");
    }

    /// <summary>Runs the ValidatePositive loopback validation step.</summary>
    /// <param name="value">The value parameter for ValidatePositive.</param>
    /// <param name="name">The name value for ValidatePositive.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidatePositive.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidatePositive.</exception>
    private static void ValidatePositive(int value, string name)
    {
        if (value > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be positive.");
    }

    /// <summary>Runs the ValidatePositiveBatchBytes loopback validation step.</summary>
    /// <param name="maximumBatchBytes">The maximumBatchBytes value for ValidatePositiveBatchBytes.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidatePositiveBatchBytes.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidatePositiveBatchBytes.</exception>
    private static void ValidatePositiveBatchBytes(long maximumBatchBytes)
    {
        if (maximumBatchBytes > 0)
        {
            return;
        }

        throw new InvalidOperationException("Maximum batch bytes must be positive.");
    }

    /// <summary>Runs the ValidateOptionalRetention loopback validation step.</summary>
    /// <param name="retention">The retention value for ValidateOptionalRetention.</param>
    /// <param name="name">The name value for ValidateOptionalRetention.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateOptionalRetention.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateOptionalRetention.</exception>
    private static void ValidateOptionalRetention(TimeSpan? retention, string name)
    {
        if (!retention.HasValue || IsPositiveFinite(retention))
        {
            return;
        }

        throw new InvalidOperationException($"{name} must be positive and finite when specified.");
    }

    /// <summary>Runs the IsPositiveFinite loopback validation step.</summary>
    /// <param name="retention">The retention value for IsPositiveFinite.</param>
    /// <returns>The IsPositiveFinite result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during IsPositiveFinite.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during IsPositiveFinite.</exception>
    private static bool IsPositiveFinite(TimeSpan? retention)
    {
        if (retention is not { } value)
        {
            return false;
        }

        return value > TimeSpan.Zero && value < TimeSpan.MaxValue;
    }

    /// <summary>Runs the CountRequiredString loopback validation step.</summary>
    /// <param name="value">The value parameter for CountRequiredString.</param>
    /// <param name="maximumStringBytes">The maximumStringBytes value for CountRequiredString.</param>
    /// <param name="message">The message value for CountRequiredString.</param>
    /// <returns>The CountRequiredString result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountRequiredString.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountRequiredString.</exception>
    private static int CountRequiredString(string? value, int maximumStringBytes, string message)
    {
        if (value is null || string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException(message);
        }

        return CountString(value, maximumStringBytes, message);
    }

    /// <summary>Runs the CountOptionalString loopback validation step.</summary>
    /// <param name="value">The value parameter for CountOptionalString.</param>
    /// <param name="maximumStringBytes">The maximumStringBytes value for CountOptionalString.</param>
    /// <param name="message">The message value for CountOptionalString.</param>
    /// <returns>The CountOptionalString result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountOptionalString.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountOptionalString.</exception>
    private static int CountOptionalString(string? value, int maximumStringBytes, string message) =>
        value is null ? 0 : CountString(value, maximumStringBytes, message);

    /// <summary>Runs the CountString loopback validation step.</summary>
    /// <param name="value">The value parameter for CountString.</param>
    /// <param name="maximumStringBytes">The maximumStringBytes value for CountString.</param>
    /// <param name="message">The message value for CountString.</param>
    /// <returns>The CountString result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountString.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountString.</exception>
    private static int CountString(string value, int maximumStringBytes, string message)
    {
        try
        {
            var bytes = StrictUtf8.GetByteCount(value);
            if (bytes <= maximumStringBytes)
            {
                return bytes;
            }
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidOperationException(message, exception);
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Runs the CountMetadata loopback validation step.</summary>
    /// <param name="metadata">The metadata value for CountMetadata.</param>
    /// <param name="options">The options value for CountMetadata.</param>
    /// <param name="message">The message value for CountMetadata.</param>
    /// <returns>The CountMetadata result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountMetadata.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountMetadata.</exception>
    private static long CountMetadata(IReadOnlyDictionary<string, string>? metadata, LoopbackTransportAdapterOptions options, string message)
    {
        if (metadata is null || metadata.Count > options.MaximumMetadataEntries)
        {
            throw new InvalidOperationException(message);
        }

        var total = IntByteCount;
        foreach (var pair in metadata)
        {
            total += CountRequiredString(pair.Key, options.MaximumStringBytes, message);
            total += CountRequiredString(pair.Value, options.MaximumStringBytes, message);
        }

        return total;
    }

    /// <summary>Runs the CountPayload loopback validation step.</summary>
    /// <param name="payload">The payload value for CountPayload.</param>
    /// <param name="options">The options value for CountPayload.</param>
    /// <param name="message">The message value for CountPayload.</param>
    /// <returns>The CountPayload result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountPayload.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountPayload.</exception>
    private static long CountPayload(PayloadEnvelope? payload, LoopbackTransportAdapterOptions options, string message)
    {
        if (payload is null || payload.SchemaVersion <= 0)
        {
            throw new InvalidOperationException(message);
        }

        long total = IntByteCount;
        total += CountRequiredString(payload.ContractId, options.MaximumStringBytes, message);
        total += CountRequiredString(payload.ContentType, options.MaximumStringBytes, message);
        total += CountRequiredString(payload.PayloadHash, options.MaximumStringBytes, message);
        total += payload.PayloadLength;
        return total;
    }

    /// <summary>Runs the ValidateLogicalByteBound loopback validation step.</summary>
    /// <param name="total">The total value for ValidateLogicalByteBound.</param>
    /// <param name="maximumBytes">The maximumBytes value for ValidateLogicalByteBound.</param>
    /// <param name="message">The message value for ValidateLogicalByteBound.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateLogicalByteBound.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateLogicalByteBound.</exception>
    private static void ValidateLogicalByteBound(long total, long maximumBytes, string message)
    {
        if (total <= maximumBytes)
        {
            return;
        }

        throw new InvalidOperationException(message);
    }

    /// <summary>Runs the ValidateOutgoingHeader loopback validation step.</summary>
    /// <param name="batch">The batch value for ValidateOutgoingHeader.</param>
    /// <param name="options">The options value for ValidateOutgoingHeader.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateOutgoingHeader.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateOutgoingHeader.</exception>
    private static void ValidateOutgoingHeader(SyncBatch batch, LoopbackTransportAdapterOptions options)
    {
        if (batch.BatchId != Guid.Empty && batch.Operations.Count > 0 && batch.Operations.Count <= options.PeerCapabilities.MaximumBatchOperations)
        {
            return;
        }

        throw new InvalidOperationException("The synchronization batch exceeds loopback admission bounds.");
    }

    /// <summary>Runs the CountOutgoingOperation loopback validation step.</summary>
    /// <param name="operation">The operation value for CountOutgoingOperation.</param>
    /// <param name="options">The options value for CountOutgoingOperation.</param>
    /// <param name="total">The total value for CountOutgoingOperation.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountOutgoingOperation.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountOutgoingOperation.</exception>
    private static void CountOutgoingOperation(SyncOperation? operation, LoopbackTransportAdapterOptions options, ref long total)
    {
        if (operation is null || operation.OperationId.Value == Guid.Empty || operation.StreamId.Value is null || operation.ClientSequence <= 0)
        {
            throw new InvalidOperationException("The synchronization batch contains a malformed operation.");
        }

        operation.Policy.Validate();
        ValidateOperationType(operation.Type);
        total += GuidByteCount;
        total += CountRequiredString(operation.StreamId.Value, options.MaximumStringBytes, "The synchronization batch contains a malformed stream identifier.");
        total += LongByteCount + DateTimeOffsetByteCount + EnumByteCount + NullableMarkerByteCount;
        total += CountOptionalString(operation.BaseVersion, options.MaximumStringBytes, "The synchronization batch contains an oversized base version.");
        total += CountPayload(operation.Payload, options, "The synchronization batch contains a malformed payload.");
        total += CountMetadata(operation.Metadata, options, "The synchronization batch contains malformed metadata.");
    }

    /// <summary>Runs the ValidateOperationType loopback validation step.</summary>
    /// <param name="type">The type value for ValidateOperationType.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateOperationType.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateOperationType.</exception>
    private static void ValidateOperationType(SyncOperationType type)
    {
        if (type is SyncOperationType.Append or SyncOperationType.Update or SyncOperationType.Delete)
        {
            return;
        }

        throw new InvalidOperationException("The synchronization batch contains an unknown operation type.");
    }

    /// <summary>Runs the ValidateOperationMembership loopback validation step.</summary>
    /// <param name="operation">The operation value for ValidateOperationMembership.</param>
    /// <param name="streamId">The streamId value for ValidateOperationMembership.</param>
    /// <param name="operationIds">The operationIds value for ValidateOperationMembership.</param>
    /// <param name="clientSequences">The clientSequences value for ValidateOperationMembership.</param>
    /// <param name="previousSequence">The previousSequence value for ValidateOperationMembership.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateOperationMembership.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateOperationMembership.</exception>
    private static void ValidateOperationMembership(
        SyncOperation operation,
        ref StreamId? streamId,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences,
        ref long previousSequence)
    {
        if (streamId is not null && streamId.Value != operation.StreamId)
        {
            throw new InvalidOperationException("The synchronization batch contains mixed streams.");
        }

        streamId ??= operation.StreamId;
        if (!operationIds.Add(operation.OperationId) || !clientSequences.Add(operation.ClientSequence) || operation.ClientSequence < previousSequence)
        {
            throw new InvalidOperationException("The synchronization batch contains duplicate or unordered operations.");
        }

        previousSequence = operation.ClientSequence;
    }

    /// <summary>Runs the ValidateStartPosition loopback validation step.</summary>
    /// <param name="position">The position value for ValidateStartPosition.</param>
    /// <param name="options">The options value for ValidateStartPosition.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during ValidateStartPosition.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during ValidateStartPosition.</exception>
    private static void ValidateStartPosition(StartPosition position, LoopbackTransportAdapterOptions options) =>
        _ = position.Kind == StartPositionKind.FromCursor
            ? CountRequiredString(position.Cursor, options.MaximumStringBytes, "The loopback start cursor is malformed.")
            : 0;

    /// <summary>Runs the CountReceiveBatch loopback validation step.</summary>
    /// <param name="batch">The batch value for CountReceiveBatch.</param>
    /// <param name="options">The options value for CountReceiveBatch.</param>
    /// <returns>The CountReceiveBatch result.</returns>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountReceiveBatch.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountReceiveBatch.</exception>
    private static long CountReceiveBatch(RemoteEventBatch batch, LoopbackTransportAdapterOptions options)
    {
        long total = GuidByteCount + IntByteCount + IntByteCount;
        total += CountRequiredString(batch.StreamId.Value, options.MaximumStringBytes, "The receive batch stream is malformed.");
        total += CountOptionalString(batch.PreviousCursor, options.MaximumStringBytes, "The receive batch previous cursor is malformed.");
        total += CountRequiredString(batch.NextCursor, options.MaximumStringBytes, "The receive batch next cursor is malformed.");
        foreach (var remoteEvent in batch.Events)
        {
            CountRemoteEvent(remoteEvent, options, ref total);
        }

        foreach (var completion in batch.CompletedOperations)
        {
            CountCompletion(completion, options, ref total);
        }

        return total;
    }

    /// <summary>Runs the CountRemoteEvent loopback validation step.</summary>
    /// <param name="remoteEvent">The remoteEvent value for CountRemoteEvent.</param>
    /// <param name="options">The options value for CountRemoteEvent.</param>
    /// <param name="total">The total value for CountRemoteEvent.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountRemoteEvent.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountRemoteEvent.</exception>
    private static void CountRemoteEvent(RemoteEvent? remoteEvent, LoopbackTransportAdapterOptions options, ref long total)
    {
        if (remoteEvent is null || remoteEvent.Payload is null)
        {
            throw new InvalidOperationException("The receive batch contains a malformed event.");
        }

        total += GuidByteCount;
        total += CountRequiredString(remoteEvent.StreamId.Value, options.MaximumStringBytes, "The receive event stream is malformed.");
        total += CountRequiredString(remoteEvent.ServerCursor, options.MaximumStringBytes, "The receive event cursor is malformed.");
        total += DateTimeOffsetByteCount + NullableMarkerByteCount;
        if (remoteEvent.CausedByOperationId.HasValue)
        {
            total += GuidByteCount;
        }

        if (remoteEvent.Origin is not null)
        {
            CountOrigin(remoteEvent.Origin, options, ref total);
        }

        total += CountPayload(remoteEvent.Payload, options, "The receive batch contains a malformed payload.");
        total += CountMetadata(remoteEvent.Metadata, options, "The receive batch contains malformed metadata.");
    }

    /// <summary>Runs the CountCompletion loopback validation step.</summary>
    /// <param name="completion">The completion value for CountCompletion.</param>
    /// <param name="options">The options value for CountCompletion.</param>
    /// <param name="total">The total value for CountCompletion.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountCompletion.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountCompletion.</exception>
    private static void CountCompletion(RemoteOperationCompletion completion, LoopbackTransportAdapterOptions options, ref long total)
    {
        CountOrigin(completion.Origin, options, ref total);
        total += IntByteCount;
        for (var index = 0; index < completion.EventIds.Count; index++)
        {
            total += GuidByteCount;
        }
    }

    /// <summary>Runs the CountOrigin loopback validation step.</summary>
    /// <param name="origin">The origin value for CountOrigin.</param>
    /// <param name="options">The options value for CountOrigin.</param>
    /// <param name="total">The total value for CountOrigin.</param>
    /// <exception cref="ArgumentNullException">A required reference is missing during CountOrigin.</exception>
    /// <exception cref="InvalidOperationException">Validation fails during CountOrigin.</exception>
    private static void CountOrigin(RemoteEventOrigin origin, LoopbackTransportAdapterOptions options, ref long total)
    {
        total += CountRequiredString(origin.ClientId, options.MaximumStringBytes, "The receive origin client identity is malformed.");
        total += GuidByteCount;
    }
}
