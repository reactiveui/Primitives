// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes bounded snapshot recovery HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>The logical byte width counted for Int32 and enum scalar values.</summary>
    private const long SnapshotInt32LogicalBytes = 4;

    /// <summary>The logical byte width counted for Int64 scalar values.</summary>
    private const long SnapshotInt64LogicalBytes = 8;

    /// <summary>The logical byte width counted for Guid scalar values.</summary>
    private const long SnapshotGuidLogicalBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset scalar values.</summary>
    private const long SnapshotDateTimeOffsetLogicalBytes = 16;

    /// <summary>The logical byte width counted for an operation policy.</summary>
    private const long SnapshotOperationPolicyLogicalBytes = SnapshotInt32LogicalBytes * 4;

    /// <summary>The replay operations JSON property name.</summary>
    private const string SnapshotReplayOperationsPropertyName = "replayOperations";

    /// <summary>Serializes a snapshot recovery request.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The request bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The request is malformed or exceeds configured limits.</exception>
    internal byte[] SerializeSnapshotRecoveryRequest(RemoteSnapshotRecoveryRequest request)
    {
        ValidateSnapshotRecoveryRequest(request);
        HttpProtocolJsonContext.SnapshotRecoveryRequestWire dto = new()
        {
            StreamId = request.StreamId.Value,
            SubscriptionId = request.SubscriptionId.Value,
            ExpiredCursor = request.ExpiredCursor,
            ClientStateContractId = request.ClientStateContractId,
            ClientStateSchemaVersion = request.ClientStateSchemaVersion,
            SnapshotFormatVersion = request.SnapshotFormatVersion,
            PendingOperations = ToSyncOperationDtos(request.PendingOperations),
            ReplayOperations = ToSyncOperationDtos(request.ReplayOperations),
            MaximumResponseBytes = request.MaximumResponseBytes,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.SnapshotRecoveryRequestWireInfo, _limits.MaximumRequestBytes);
    }

    /// <summary>Deserializes a snapshot recovery request.</summary>
    /// <param name="bytes">The request bytes.</param>
    /// <returns>The recovery request.</returns>
    /// <exception cref="HttpRemoteTransportException">The request is malformed or exceeds configured limits.</exception>
    internal RemoteSnapshotRecoveryRequest DeserializeSnapshotRecoveryRequest(byte[] bytes)
    {
        var dto = Deserialize(
            bytes,
            HttpProtocolJsonContext.Default.SnapshotRecoveryRequestWireInfo,
            _limits.MaximumRequestBytes,
            ValidateSnapshotRecoveryRequestElement);
        return TranslateProtocolExceptions(
            () =>
            {
                var request = new RemoteSnapshotRecoveryRequest
                {
                    StreamId = new(dto.StreamId),
                    SubscriptionId = new(dto.SubscriptionId),
                    ExpiredCursor = dto.ExpiredCursor,
                    ClientStateContractId = dto.ClientStateContractId,
                    ClientStateSchemaVersion = dto.ClientStateSchemaVersion,
                    SnapshotFormatVersion = dto.SnapshotFormatVersion,
                    PendingOperations = ToOperations(dto.PendingOperations),
                    ReplayOperations = ToOperations(dto.ReplayOperations),
                    MaximumResponseBytes = dto.MaximumResponseBytes,
                };
                ValidateSnapshotRecoveryRequest(request);
                return request;
            });
    }

    /// <summary>Serializes a snapshot recovery response.</summary>
    /// <param name="request">The original recovery request.</param>
    /// <param name="result">The recovery result.</param>
    /// <returns>The response bytes.</returns>
    /// <exception cref="HttpRemoteTransportException">The result is malformed, unbound, or exceeds configured limits.</exception>
    internal byte[] SerializeSnapshotRecoveryResponse(RemoteSnapshotRecoveryRequest request, RemoteSnapshotRecoveryResult result)
    {
        ValidateSnapshotRecoveryResponse(request, result);
        HttpProtocolJsonContext.SnapshotRecoveryResponseWire dto = new()
        {
            Status = (int)result.Status,
            Checkpoint = result.Checkpoint is null ? null : ToSnapshotCheckpointDto(result.Checkpoint),
            OperationDispositions = ToSnapshotDispositionDtos(result.OperationDispositions),
            ReasonCode = result.ReasonCode,
        };
        return Serialize(dto, HttpProtocolJsonContext.Default.SnapshotRecoveryResponseWireInfo, _limits.MaximumResponseBytes);
    }

    /// <summary>Deserializes a snapshot recovery response and validates it against the original request.</summary>
    /// <param name="request">The original recovery request.</param>
    /// <param name="bytes">The response bytes.</param>
    /// <returns>The recovery result.</returns>
    /// <exception cref="HttpRemoteTransportException">The response is malformed, unbound, or exceeds configured limits.</exception>
    internal RemoteSnapshotRecoveryResult DeserializeSnapshotRecoveryResponse(RemoteSnapshotRecoveryRequest request, byte[] bytes)
    {
        ValidateSnapshotRecoveryRequest(request);
        var dto = Deserialize(
            bytes,
            HttpProtocolJsonContext.Default.SnapshotRecoveryResponseWireInfo,
            _limits.MaximumResponseBytes,
            element => ValidateSnapshotRecoveryResponseElement(request, element));
        return TranslateProtocolExceptions(
            () =>
            {
                var result = new RemoteSnapshotRecoveryResult
                {
                    Status = ToSnapshotStatus(dto.Status),
                    Checkpoint = dto.Checkpoint is null ? null : ToSnapshotCheckpoint(dto.Checkpoint),
                    OperationDispositions = ToSnapshotDispositions(dto.OperationDispositions),
                    ReasonCode = dto.ReasonCode,
                };
                ValidateSnapshotRecoveryResponse(request, result);
                return result;
            });
    }
}
