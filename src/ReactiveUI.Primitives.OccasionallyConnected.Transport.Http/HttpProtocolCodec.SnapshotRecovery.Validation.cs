// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Validates snapshot recovery domain objects before transport materialization.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Counts a required snapshot protocol string.</summary>
    /// <param name="value">The string value.</param>
    /// <param name="maximumUtf8Bytes">The byte limit.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static long CountSnapshotRequiredString(string value, int maximumUtf8Bytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(value);
        return CountSnapshotStringBytes(value, maximumUtf8Bytes);
    }

    /// <summary>Counts an optional snapshot protocol string.</summary>
    /// <param name="value">The string value.</param>
    /// <param name="maximumUtf8Bytes">The byte limit.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static long CountSnapshotOptionalString(string? value, int maximumUtf8Bytes) =>
        value is null ? 0 : CountSnapshotStringBytes(value, maximumUtf8Bytes);

    /// <summary>Counts a string and enforces its explicit UTF-8 byte limit.</summary>
    /// <param name="value">The value.</param>
    /// <param name="maximumUtf8Bytes">The byte limit.</param>
    /// <returns>The UTF-8 byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="value"/> is malformed or too large.</exception>
    private static long CountSnapshotStringBytes(string value, int maximumUtf8Bytes)
    {
        int bytes;
        try
        {
            bytes = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw CreateProtocolViolation(exception);
        }

        if (bytes <= maximumUtf8Bytes)
        {
            return bytes;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Throws when snapshot logical accounting exceeds one configured limit.</summary>
    /// <param name="logicalBytes">The counted bytes.</param>
    /// <param name="maximumLogicalBytes">The maximum logical bytes.</param>
    /// <exception cref="HttpRemoteTransportException"><paramref name="logicalBytes"/> exceeds <paramref name="maximumLogicalBytes"/>.</exception>
    private static void ThrowIfSnapshotLogicalBytesExceeded(long logicalBytes, long maximumLogicalBytes)
    {
        if (logicalBytes <= maximumLogicalBytes)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Creates an HTTP failure for a snapshot recovery validation exception.</summary>
    /// <param name="exception">The validation exception.</param>
    /// <param name="responseBinding">Whether the validation belongs to a response binding.</param>
    /// <returns>The HTTP transport exception.</returns>
    private static HttpRemoteTransportException CreateSnapshotValidationException(ArgumentException exception, bool responseBinding)
    {
        _ = exception;
        var kind = responseBinding
            ? HttpTransportFailureKind.ValidationRejected
            : HttpTransportFailureKind.ProtocolViolation;
        return new(kind);
    }

    /// <summary>Preflights a snapshot recovery request for explicit snapshot limits before Core validation.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="HttpRemoteTransportException"><paramref name="request"/> is malformed or exceeds configured limits.</exception>
    private void PreflightSnapshotRecoveryRequest(RemoteSnapshotRecoveryRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        if (request.MaximumResponseBytes > _snapshotRecoveryLimits.MaximumLogicalBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = CountSnapshotRequestHeader(request);
        logicalBytes = checked(logicalBytes + CountSnapshotOperations(request.PendingOperations, request.ReplayOperations, logicalBytes));
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
    }

    /// <summary>Preflights a snapshot recovery response for explicit snapshot limits before Core validation.</summary>
    /// <param name="request">The request that bounds the response.</param>
    /// <param name="result">The response result.</param>
    private void PreflightSnapshotRecoveryResponse(RemoteSnapshotRecoveryRequest request, RemoteSnapshotRecoveryResult result)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        var logicalBytes = SnapshotInt32LogicalBytes + CountSnapshotOptionalString(result.ReasonCode, _snapshotRecoveryLimits.MaximumReasonCodeUtf8Bytes);
        if (result.Status != RemoteSnapshotRecoveryStatus.Recovered)
        {
            logicalBytes = checked(logicalBytes + SnapshotInt32LogicalBytes);
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes);
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
            return;
        }

        if (result.Checkpoint is not null)
        {
            logicalBytes = checked(logicalBytes + CountSnapshotCheckpoint(result.Checkpoint));
        }

        logicalBytes = checked(logicalBytes + CountSnapshotDispositions(result.OperationDispositions));
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes);
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
    }

    /// <summary>Counts request header logical bytes and explicit response capacity bounds.</summary>
    /// <param name="request">The request.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotRequestHeader(RemoteSnapshotRecoveryRequest request)
    {
        var logicalBytes = CountSnapshotRequiredString(request.StreamId.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes);
        logicalBytes = checked(logicalBytes + SnapshotGuidLogicalBytes);
        logicalBytes = checked(logicalBytes + CountSnapshotOptionalString(request.ExpiredCursor, _snapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(request.ClientStateContractId, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        return checked(logicalBytes + SnapshotInt32LogicalBytes + SnapshotInt32LogicalBytes + SnapshotInt64LogicalBytes);
    }

    /// <summary>Counts pending and replay operation logical bytes.</summary>
    /// <param name="operations">The pending operations.</param>
    /// <param name="replayOperations">The replay operations.</param>
    /// <param name="requestHeaderLogicalBytes">The request header logical bytes already charged.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException">The combined operation set is malformed or exceeds configured limits.</exception>
    private long CountSnapshotOperations(
        IReadOnlyList<SyncOperation> operations,
        IReadOnlyList<SyncOperation> replayOperations,
        long requestHeaderLogicalBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(operations);
        ArgumentExceptionHelper.ThrowIfNull(replayOperations);
        var totalCount = (long)operations.Count + replayOperations.Count;
        if (totalCount > _snapshotRecoveryLimits.MaximumPendingOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes + SnapshotInt32LogicalBytes;
        ThrowIfSnapshotLogicalBytesExceeded(checked(requestHeaderLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        for (var index = 0; index < operations.Count; index++)
        {
            logicalBytes = checked(logicalBytes + CountSnapshotOperation(operations[index], checked(requestHeaderLogicalBytes + logicalBytes)));
            ThrowIfSnapshotLogicalBytesExceeded(checked(requestHeaderLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        }

        for (var index = 0; index < replayOperations.Count; index++)
        {
            logicalBytes = checked(logicalBytes + CountSnapshotOperation(replayOperations[index], checked(requestHeaderLogicalBytes + logicalBytes)));
            ThrowIfSnapshotLogicalBytesExceeded(checked(requestHeaderLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        }

        return logicalBytes;
    }

    /// <summary>Counts one pending operation's logical bytes.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="currentLogicalBytes">The logical byte count already charged to the request.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotOperation(SyncOperation operation, long currentLogicalBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        var logicalBytes = SnapshotGuidLogicalBytes;
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(operation.StreamId.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes));
        logicalBytes = checked(logicalBytes + SnapshotInt64LogicalBytes + SnapshotDateTimeOffsetLogicalBytes + SnapshotInt32LogicalBytes);
        logicalBytes = checked(logicalBytes + SnapshotOperationPolicyLogicalBytes);
        ThrowIfSnapshotLogicalBytesExceeded(checked(currentLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        logicalBytes = checked(logicalBytes + CountSnapshotPayload(operation.Payload));
        logicalBytes = checked(logicalBytes + CountSnapshotOptionalString(operation.BaseVersion, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        logicalBytes = checked(logicalBytes + CountSnapshotMetadata(operation.Metadata));
        return logicalBytes;
    }

    /// <summary>Counts one payload envelope's logical bytes.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="payload"/> exceeds configured limits.</exception>
    private long CountSnapshotPayload(PayloadEnvelope payload)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        if (payload.PayloadLength > _snapshotRecoveryLimits.MaximumPayloadBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes + SnapshotInt64LogicalBytes;
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(payload.ContractId, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(payload.ContentType, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(payload.PayloadHash, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        return checked(logicalBytes + payload.PayloadLength);
    }

    /// <summary>Counts operation metadata logical bytes.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="metadata"/> exceeds configured limits.</exception>
    private long CountSnapshotMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        if (metadata.Count > _snapshotRecoveryLimits.MaximumMetadataEntries)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes;
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumMetadataBytes);
        foreach (var pair in metadata)
        {
            logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(pair.Key, _snapshotRecoveryLimits.MaximumMetadataBytes));
            logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(pair.Value, _snapshotRecoveryLimits.MaximumMetadataBytes));
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumMetadataBytes);
        }

        return logicalBytes;
    }

    /// <summary>Counts checkpoint logical bytes.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotCheckpoint(RemoteSnapshotCheckpoint checkpoint)
    {
        ArgumentExceptionHelper.ThrowIfNull(checkpoint);
        var logicalBytes = CountSnapshotRequiredString(checkpoint.StreamId.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes);
        logicalBytes = checked(logicalBytes + SnapshotGuidLogicalBytes);
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(checkpoint.FrontierCursor, _snapshotRecoveryLimits.MaximumCursorUtf8Bytes));
        logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(checkpoint.ServerVersion, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
        logicalBytes = checked(logicalBytes + SnapshotInt32LogicalBytes + SnapshotDateTimeOffsetLogicalBytes);
        return checked(logicalBytes + CountSnapshotPayload(checkpoint.ClientState));
    }

    /// <summary>Counts snapshot disposition logical bytes.</summary>
    /// <param name="dispositions">The dispositions.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="dispositions"/> is malformed or exceeds configured limits.</exception>
    private long CountSnapshotDispositions(IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        ArgumentExceptionHelper.ThrowIfNull(dispositions);
        if (dispositions.Count > _snapshotRecoveryLimits.MaximumPendingOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes;
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            ArgumentExceptionHelper.ThrowIfNull(disposition);
            logicalBytes = checked(logicalBytes + SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes);
            if (disposition.Result is not null)
            {
                logicalBytes = checked(logicalBytes + CountSnapshotOperationResult(disposition.Result));
            }
        }

        return logicalBytes;
    }

    /// <summary>Counts operation result logical bytes.</summary>
    /// <param name="result">The operation result.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotOperationResult(OperationSyncResult result)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        var logicalBytes = SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes;
        logicalBytes = checked(logicalBytes + CountSnapshotOptionalString(result.ReasonCode, _snapshotRecoveryLimits.MaximumReasonCodeUtf8Bytes));
        return checked(logicalBytes + CountSnapshotOptionalString(result.ServerVersion, _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
    }

    /// <summary>Validates a snapshot recovery request through the core contract validator.</summary>
    /// <param name="request">The request.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateSnapshotRecoveryRequest(RemoteSnapshotRecoveryRequest request)
    {
        try
        {
            PreflightSnapshotRecoveryRequest(request);
            SnapshotRecoveryValidator.Validate(request, _snapshotRecoveryLimits);
        }
        catch (ArgumentException exception)
        {
            throw CreateSnapshotValidationException(exception, responseBinding: false);
        }
    }

    /// <summary>Validates a snapshot recovery response through the core contract validator.</summary>
    /// <param name="request">The request.</param>
    /// <param name="result">The result.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateSnapshotRecoveryResponse(RemoteSnapshotRecoveryRequest request, RemoteSnapshotRecoveryResult result)
    {
        try
        {
            PreflightSnapshotRecoveryRequest(request);
            PreflightSnapshotRecoveryResponse(request, result);
            SnapshotRecoveryValidator.Validate(request, result, _snapshotRecoveryLimits);
        }
        catch (ArgumentException exception)
        {
            throw CreateSnapshotValidationException(exception, responseBinding: true);
        }
    }
}
