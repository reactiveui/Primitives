// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>The base64 block input byte count.</summary>
    private const int Base64BlockInputBytes = 3;

    /// <summary>The base64 block output character count.</summary>
    private const int Base64BlockOutputCharacters = 4;

    /// <summary>The base64 rounding byte count.</summary>
    private const int Base64RoundingBytes = 2;

    /// <summary>The fixed per-operation estimate used before exact bounded JSON writing.</summary>
    private const long OperationEstimateOverheadBytes = 256;

    /// <summary>The supported protocol major version.</summary>
    private const int SupportedProtocolMajor = 1;

    /// <summary>The known remote transport feature flags.</summary>
    private const RemoteTransportCapabilities KnownFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.SnapshotRecovery
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>The decimal number base.</summary>
    private const int DecimalRadix = 10;

    /// <summary>The number of hexadecimal digits in a percent-encoded byte.</summary>
    private const int PercentEncodedByteHexDigits = 2;

    /// <summary>The stream identifier protocol property name.</summary>
    private const string StreamIdPropertyName = "streamId";

    /// <summary>The subscription identifier protocol property name.</summary>
    private const string SubscriptionIdPropertyName = "subscriptionId";

    /// <summary>The cursor protocol property name.</summary>
    private const string CursorPropertyName = "cursor";

    /// <summary>The start position kind protocol property name.</summary>
    private const string PositionKindPropertyName = "positionKind";

    /// <summary>The timestamp protocol property name.</summary>
    private const string TimestampPropertyName = "timestamp";

    /// <summary>The sequence protocol property name.</summary>
    private const string SequencePropertyName = "sequence";

    /// <summary>The initial cursor protocol property name.</summary>
    private const string InitialCursorPropertyName = "initialCursor";

    /// <summary>The batch identifier protocol property name.</summary>
    private const string BatchIdPropertyName = "batchId";

    /// <summary>The operation identifier protocol property name.</summary>
    private const string OperationIdPropertyName = "operationId";

    /// <summary>The payload protocol property name.</summary>
    private const string PayloadPropertyName = "payload";

    /// <summary>The origin protocol property name.</summary>
    private const string OriginPropertyName = "origin";

    /// <summary>The server cursor protocol property name.</summary>
    private const string ServerCursorPropertyName = "serverCursor";

    /// <summary>The JSON string type validation message.</summary>
    private const string ExpectedJsonStringMessage = "Expected a JSON string.";

    /// <summary>The JSON number type validation message.</summary>
    private const string ExpectedJsonNumberMessage = "Expected a JSON number.";

    /// <summary>The strict UTF-8 encoding.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>The protocol limits.</summary>
    private readonly HttpProtocolLimits _limits;

    /// <summary>The snapshot recovery protocol limits.</summary>
    private readonly SnapshotRecoveryLimits _snapshotRecoveryLimits;

    /// <summary>Initializes a new instance of the <see cref="HttpProtocolCodec"/> class.</summary>
    /// <param name="options">The adapter options.</param>
    internal HttpProtocolCodec(HttpRemoteTransportOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ArgumentExceptionHelper.ThrowIfNull(options.SnapshotRecoveryLimits);
        _limits = HttpProtocolLimits.FromOptions(options).Complete();
        _snapshotRecoveryLimits = options.SnapshotRecoveryLimits;
        _snapshotRecoveryLimits.Validate();
    }

    /// <summary>Initializes a new instance of the <see cref="HttpProtocolCodec"/> class.</summary>
    /// <param name="limits">The protocol limits.</param>
    internal HttpProtocolCodec(HttpProtocolLimits limits)
        : this(limits, new SnapshotRecoveryLimits())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpProtocolCodec"/> class.</summary>
    /// <param name="limits">The protocol limits.</param>
    /// <param name="snapshotRecoveryLimits">The snapshot recovery limits.</param>
    internal HttpProtocolCodec(HttpProtocolLimits limits, SnapshotRecoveryLimits snapshotRecoveryLimits)
    {
        ArgumentExceptionHelper.ThrowIfNull(limits);
        ArgumentExceptionHelper.ThrowIfNull(snapshotRecoveryLimits);
        _limits = limits.Complete();
        _snapshotRecoveryLimits = snapshotRecoveryLimits;
        _snapshotRecoveryLimits.Validate();
    }

    /// <summary>Serializes a connect request.</summary>
    /// <param name="request">The connect request.</param>
    /// <returns>The request bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The request is malformed or violates configured limits.</exception>
    internal byte[] SerializeConnectRequest(TransportConnectRequest request)
    {
        ValidateConnectRequest(request);
        var guarantees = new int[request.RequiredGuarantees.Count];
        var index = 0;
        foreach (var guarantee in request.RequiredGuarantees)
        {
            guarantees[index] = (int)guarantee;
            index++;
        }

        HttpProtocolJsonContext.ConnectRequestWire dto = new()
        {
            MinimumProtocolVersion = request.SupportedProtocolVersions.Minimum.ToString(),
            MaximumProtocolVersion = request.SupportedProtocolVersions.Maximum.ToString(),
            ClientId = request.Client.ClientId,
            TenantHint = request.Client.TenantHint,
            RequiredGuarantees = guarantees,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.ConnectRequestWireInfo, _limits.MaximumRequestBytes);
    }

    /// <summary>Deserializes a connect request.</summary>
    /// <param name="bytes">The request bytes.</param>
    /// <returns>The connect request.</returns>
    /// <exception cref="HttpRemoteTransportException">The request is malformed or violates configured limits.</exception>
    internal TransportConnectRequest DeserializeConnectRequest(byte[] bytes)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.ConnectRequestWireInfo, _limits.MaximumRequestBytes, ValidateConnectRequestElement);
        if (!Version.TryParse(dto.MinimumProtocolVersion, out var minimum)
            || !Version.TryParse(dto.MaximumProtocolVersion, out var maximum)
            || minimum.CompareTo(maximum) > 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        ValidateProtocolString(dto.ClientId);
        ValidateOptionalProtocolString(dto.TenantHint);
        var count = dto.RequiredGuarantees.Length;
        var guarantees = new DeliveryGuarantee[count];
        for (var index = 0; index < count; index++)
        {
            guarantees[index] = ToDeliveryGuarantee(dto.RequiredGuarantees[index]);
        }

        var request = new TransportConnectRequest(new(minimum, maximum), new(dto.ClientId, dto.TenantHint), guarantees);
        ValidateConnectRequest(request);
        return request;
    }

    /// <summary>Serializes a connect response.</summary>
    /// <param name="capabilities">The negotiated capabilities.</param>
    /// <returns>The response bytes.</returns>
    internal byte[] SerializeConnectResponse(NegotiatedCapabilities capabilities)
    {
        ValidateCapabilities(capabilities);
        HttpProtocolJsonContext.ConnectResponseWire dto = new()
        {
            ProtocolVersion = capabilities.ProtocolVersion.ToString(),
            Features = (int)capabilities.Features,
            MaximumBatchOperations = capabilities.MaximumBatchOperations,
            MaximumBatchBytes = capabilities.MaximumBatchBytes,
            ServerIdempotencyRetentionMilliseconds = ToMilliseconds(capabilities.ServerIdempotencyRetention),
            ClientInboxRetentionRequiredMilliseconds = ToMilliseconds(capabilities.ClientInboxRetentionRequired),
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.ConnectResponseWireInfo, _limits.MaximumResponseBytes);
    }

    /// <summary>Deserializes a connect response.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <returns>The negotiated capabilities.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or violates protocol bounds.</exception>
    internal NegotiatedCapabilities DeserializeConnectResponse(byte[] bytes)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.ConnectResponseWireInfo, _limits.MaximumResponseBytes, ValidateConnectResponseElement);
        return TranslateProtocolExceptions(
            () =>
            {
                if (!Version.TryParse(dto.ProtocolVersion, out var version))
                {
                    throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
                }

                var features = ToCapabilities(dto.Features);
                var capabilities = new NegotiatedCapabilities(
                    version,
                    features,
                    dto.MaximumBatchOperations,
                    dto.MaximumBatchBytes,
                    HttpProtocolCodecHelper.ToTimeSpan(dto.ServerIdempotencyRetentionMilliseconds),
                    HttpProtocolCodecHelper.ToTimeSpan(dto.ClientInboxRetentionRequiredMilliseconds));
                ValidateCapabilities(capabilities);
                return capabilities;
            });
    }

    /// <summary>Serializes a push request.</summary>
    /// <param name="batch">The synchronization batch.</param>
    /// <returns>The request bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The batch cannot be encoded within configured bounds.</exception>
    internal byte[] SerializePushRequest(SyncBatch batch)
    {
        ValidateBatch(batch);
        var count = batch.Operations.Count;
        var operations = new HttpProtocolJsonContext.SyncOperationWire[count];
        long requestBudget = 0;
        for (var index = 0; index < count; index++)
        {
            var operation = batch.Operations[index];
            requestBudget = checked(requestBudget + EstimateOperationBytes(operation));
            if (requestBudget > _limits.MaximumRequestBytes)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            operations[index] = ToDto(operation);
        }

        HttpProtocolJsonContext.PushRequestWire request = new() { BatchId = batch.BatchId, Operations = operations };
        return Serialize(request, HttpProtocolJsonContext.Default.PushRequestWireInfo, _limits.MaximumRequestBytes);
    }

    /// <summary>Deserializes a push request.</summary>
    /// <param name="bytes">The request bytes.</param>
    /// <returns>The synchronization batch.</returns>
    internal SyncBatch DeserializePushRequest(byte[] bytes)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.PushRequestWireInfo, _limits.MaximumRequestBytes, ValidatePushRequestElement);
        return TranslateProtocolExceptions(
            () =>
            {
                var count = dto.Operations.Length;
                var operations = new SyncOperation[count];
                for (var index = 0; index < count; index++)
                {
                    operations[index] = ToOperation(dto.Operations[index]);
                }

                var batch = new SyncBatch(dto.BatchId, operations);
                ValidateBatch(batch);
                return batch;
            });
    }

    /// <summary>Serializes a push response.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <param name="result">The synchronization result.</param>
    /// <returns>The response bytes.</returns>
    internal byte[] SerializePushResponse(SyncBatch batch, RemoteSyncResult result)
    {
        ValidateResult(batch, result);
        var count = result.Operations.Count;
        var operations = new HttpProtocolJsonContext.OperationSyncResultWire[count];
        for (var index = 0; index < count; index++)
        {
            operations[index] = ToDto(result.Operations[index]);
        }

        HttpProtocolJsonContext.PushResponseWire response = new() { BatchId = result.BatchId, Operations = operations, ServerCursor = result.ServerCursor };
        return Serialize(response, HttpProtocolJsonContext.Default.PushResponseWireInfo, _limits.MaximumResponseBytes);
    }

    /// <summary>Deserializes a push response and validates it against the pushed batch.</summary>
    /// <param name="batch">The pushed batch.</param>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="retryAfter">The HTTP retry hint.</param>
    /// <returns>The remote synchronization result.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or does not match the pushed batch.</exception>
    internal RemoteSyncResult DeserializePushResponse(SyncBatch batch, byte[] bytes, TimeSpan? retryAfter)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.PushResponseWireInfo, _limits.MaximumResponseBytes, ValidatePushResponseElement);
        var count = dto.Operations.Length;
        var operations = new OperationSyncResult[count];
        for (var index = 0; index < count; index++)
        {
            operations[index] = HttpProtocolCodecHelper.ToOperationResult(dto.Operations[index]);
        }

        RemoteSyncResult result = new(dto.BatchId, operations, dto.ServerCursor, retryAfter);
        ValidateResult(batch, result);
        return result;
    }

    /// <summary>Serializes an acknowledgement.</summary>
    /// <param name="acknowledgement">The acknowledgement.</param>
    /// <returns>The request bytes.</returns>
    internal byte[] SerializeAcknowledgement(ReceiveAcknowledgement acknowledgement)
    {
        ValidateAcknowledgement(acknowledgement);
        var dto = new HttpProtocolJsonContext.AcknowledgeRequestWire
        {
            SubscriptionId = acknowledgement.SubscriptionId.Value,
            StreamId = acknowledgement.StreamId.Value,
            Cursor = acknowledgement.Cursor,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo, _limits.MaximumRequestBytes);
    }

    /// <summary>Deserializes an acknowledgement request.</summary>
    /// <param name="bytes">The request bytes.</param>
    /// <returns>The acknowledgement.</returns>
    internal ReceiveAcknowledgement DeserializeAcknowledgement(byte[] bytes)
    {
        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.AcknowledgeRequestWireInfo, _limits.MaximumRequestBytes, ValidateAcknowledgementElement);
        return TranslateProtocolExceptions(
            () =>
            {
                var acknowledgement = new ReceiveAcknowledgement(new(dto.SubscriptionId), new(dto.StreamId), dto.Cursor);
                ValidateAcknowledgement(acknowledgement);
                return acknowledgement;
            });
    }

    /// <summary>Parses a subscribe request query string.</summary>
    /// <param name="query">The encoded query string.</param>
    /// <returns>The subscribe request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RemoteSubscribeRequest ParseSubscribeRequest(string query) =>
        ParseSubscribeRequestWithQueryFields(query).Request;

    /// <summary>Parses a subscribe request query string and preserves its exact decoded fields for replay canonicalization.</summary>
    /// <param name="query">The encoded query string.</param>
    /// <returns>The subscribe request and decoded query fields.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal HttpSubscribeRequestParseResult ParseSubscribeRequestWithQueryFields(string query) =>
        TranslateProtocolExceptions(
            () =>
            {
                var values = ParseQuery(query);
                var request = CreateSubscribeRequest(values);
                return new HttpSubscribeRequestParseResult(request, CreateQueryFields(values));
            });

    /// <summary>Deserializes a subscribe response into complete batches.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="currentCursor">The caller's current receive cursor, when one has been durably applied.</param>
    /// <returns>The remote event batches.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RemoteEventBatch[] DeserializeSubscribeResponse(byte[] bytes, string? currentCursor = null) =>
        DeserializeSubscribeResponse(bytes, expectedStreamId: null, currentCursor);

    /// <summary>Deserializes a subscribe response into complete batches for the requested stream.</summary>
    /// <param name="bytes">The response bytes.</param>
    /// <param name="expectedStreamId">The requested stream identifier, when the caller binds the response to a stream.</param>
    /// <param name="currentCursor">The caller's current receive cursor, when one has been durably applied.</param>
    /// <returns>The remote event batches.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal RemoteEventBatch[] DeserializeSubscribeResponse(byte[] bytes, StreamId? expectedStreamId, string? currentCursor = null)
    {
        if (bytes.Length is 0)
        {
            return [];
        }

        var dto = Deserialize(bytes, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo, _limits.MaximumResponseBytes, ValidateSubscribeResponseElement);
        return TranslateProtocolExceptions(
            () =>
            {
                var cursor = currentCursor;
                var count = dto.Batches.Length;
                var batches = new RemoteEventBatch[count];
                for (var index = 0; index < count; index++)
                {
                    batches[index] = ToBatch(dto.Batches[index]);
                    ValidateStream(batches[index], expectedStreamId);
                    ValidateReceiveBatch(batches[index]);
                    ValidateCursorContinuity(batches[index], ref cursor);
                }

                return batches;
            });
    }

    /// <summary>Serializes an ordered subscribe response.</summary>
    /// <param name="batches">The complete receive batches.</param>
    /// <returns>The response bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed or violates configured limits.</exception>
    internal byte[] SerializeSubscribeResponse(IReadOnlyList<RemoteEventBatch> batches)
    {
        ArgumentExceptionHelper.ThrowIfNull(batches);
        var count = batches.Count;
        if (count > _limits.MaximumBatchOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var dtoBatches = new HttpProtocolJsonContext.RemoteEventBatchWire[count];
        for (var index = 0; index < count; index++)
        {
            var batch = batches[index];
            ValidateReceiveBatch(batch);
            dtoBatches[index] = ToDto(batch);
        }

        var response = new HttpProtocolJsonContext.SubscribeResponseWire { Batches = dtoBatches };
        return Serialize(response, HttpProtocolJsonContext.Default.SubscribeResponseWireInfo, _limits.MaximumResponseBytes);
    }
}
