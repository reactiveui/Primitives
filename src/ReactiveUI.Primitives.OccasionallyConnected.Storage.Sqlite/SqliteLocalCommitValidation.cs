// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Validates SQLite local commit store input.</summary>
internal static class SqliteLocalCommitValidation
{
    /// <summary>The SHA-256 payload hash prefix used by canonical JSON envelopes.</summary>
    private const string Sha256PayloadHashPrefix = "sha256-";

    /// <summary>The length of a canonical SHA-256 payload hash.</summary>
    private const int Sha256PayloadHashLength = 51;

    /// <summary>The maximum accepted dead-letter reason code length in UTF-8 bytes.</summary>
    private const int MaximumDeadLetterReasonBytes = 1024;

    /// <summary>Validates initialization input.</summary>
    /// <param name="initialization">The initialization requirements.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateInitialization(LocalStoreInitialization initialization)
    {
        ThrowIfBlank(initialization.StoreIdentity, nameof(initialization), "StoreIdentity must be non-empty.");
        if (initialization.RequiredSchemaVersion == SqliteStoreSchema.LocalCommitSchemaVersion)
        {
            return;
        }

        throw new InvalidOperationException("The requested SQLite local commit schema version is not supported.");
    }

    /// <summary>Validates commit input.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateCommitInput(SyncOperation operation, SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        ValidateStreamId(operation.StreamId, nameof(operation));
        ValidateStreamId(snapshotMutation.StreamId, nameof(snapshotMutation));
        if (operation.StreamId != snapshotMutation.StreamId)
        {
            throw new ArgumentException("The operation and snapshot mutation must target the same stream.", nameof(snapshotMutation));
        }

        ValidateOperationId(operation.OperationId, nameof(operation));
        if (operation.ClientSequence <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(operation), operation.ClientSequence, "ClientSequence must be positive.");
        }

        ValidateOperationType(operation.Type);
        if (operation.ClientSequence == long.MaxValue)
        {
            throw new InvalidOperationException("The next durable client sequence would overflow.");
        }

        ValidatePayload(operation.Payload, nameof(operation));
        operation.Policy.Validate();
        ValidateMetadata(operation.Metadata);
        ValidateSnapshotMutation(snapshotMutation);
    }

    /// <summary>Validates recovery input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateRecoveryInput(StreamId streamId, SubscriptionId subscriptionId)
    {
        ValidateStreamId(streamId, nameof(streamId));
        if (subscriptionId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("SubscriptionId must be non-empty.", nameof(subscriptionId));
    }

    /// <summary>Validates remote inbox lookup input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="eventIds">The remote event identifiers.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    internal static void ValidateInboxLookupInput(StreamId streamId, IReadOnlyList<Guid> eventIds)
    {
        ValidateStreamId(streamId, nameof(streamId));
        ArgumentExceptionHelper.ThrowIfNull(eventIds);
        for (var index = 0; index < eventIds.Count; index++)
        {
            if (eventIds[index] == Guid.Empty)
            {
                throw new ArgumentException("Remote event identifiers must be non-empty.", nameof(eventIds));
            }
        }
    }

    /// <summary>Validates remote apply input.</summary>
    /// <param name="batch">The remote event batch.</param>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateRemoteApplyInput(RemoteEventBatch batch, SnapshotMutation snapshotMutation)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(snapshotMutation);
        if (batch.BatchId == Guid.Empty)
        {
            throw new ArgumentException("Remote batch id must be non-empty.", nameof(batch));
        }

        ValidateStreamId(batch.StreamId, nameof(batch));
        ValidateStreamId(snapshotMutation.StreamId, nameof(snapshotMutation));
        if (batch.PreviousCursor is not null)
        {
            ThrowIfBlank(batch.PreviousCursor, nameof(batch), "Remote batch previous cursor must be non-empty when supplied.");
        }

        if (batch.StreamId != snapshotMutation.StreamId)
        {
            throw new ArgumentException("The remote batch and snapshot mutation must target the same stream.", nameof(snapshotMutation));
        }

        ThrowIfBlank(batch.NextCursor, nameof(batch), "Remote batch next cursor must be non-empty.");
        ValidateSnapshotMutation(snapshotMutation);
        HashSet<Guid> seen = [];
        for (var index = 0; index < batch.Events.Count; index++)
        {
            ValidateRemoteEvent(batch, batch.Events[index], seen);
        }
    }

    /// <summary>Validates lease acquisition input.</summary>
    /// <param name="request">The lease request.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    internal static void ValidateLeaseRequest(OutboxLeaseRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        if (request.StreamId is { } streamId)
        {
            ValidateStreamId(streamId, nameof(request));
        }

        if (request.MaximumOperations <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.MaximumOperations, "MaximumOperations must be positive.");
        }

        if (request.MaximumBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.MaximumBytes, "MaximumBytes must be positive.");
        }

        ThrowIfNotPositive(request.LeaseDuration, nameof(request), "LeaseDuration must be positive.");
    }

    /// <summary>Validates compaction input.</summary>
    /// <param name="request">The compaction request.</param>
    /// <param name="retention">The retention policy.</param>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateCompactionInput(CompactionRequest request, RetentionOptions retention)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(retention);
        if (request.StreamId is { } streamId)
        {
            ValidateStreamId(streamId, nameof(request));
        }

        if (request.TargetBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), request.TargetBytes, "TargetBytes must not be negative.");
        }

        retention.Validate();
    }

    /// <summary>Validates a lease renewal request.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="extension">The extension duration.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateLeaseRenewalInput(Guid leaseId, TimeSpan extension)
    {
        ValidateLeaseId(leaseId);
        ThrowIfNotPositive(extension, nameof(extension), "Lease extension must be positive.");
    }

    /// <summary>Validates a lease identifier.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    internal static void ValidateLeaseId(Guid leaseId)
    {
        if (leaseId != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("Lease id must be non-empty.", nameof(leaseId));
    }

    /// <summary>Validates attempt barrier input.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="nextAttempt">The next attempt number.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The attempt number is not positive.</exception>
    internal static void ValidateAttemptBarrierInput(Guid leaseId, OperationId operationId, int nextAttempt)
    {
        ValidateLeaseId(leaseId);
        ValidateOperationId(operationId, nameof(operationId));
        if (nextAttempt > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(nextAttempt), nextAttempt, "Attempt number must be positive.");
    }

    /// <summary>Validates sync result application input.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="result">The sync result.</param>
    /// <exception cref="ArgumentException">The lease identifier is invalid.</exception>
    /// <exception cref="ArgumentNullException">The result is null.</exception>
    internal static void ValidateSyncResultInput(Guid leaseId, RemoteSyncResult result)
    {
        ValidateLeaseId(leaseId);
        ArgumentExceptionHelper.ThrowIfNull(result);
    }

    /// <summary>Validates dead-letter reconciliation input.</summary>
    /// <param name="leaseId">The owning lease identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="snapshotMutation">The replacement mutation.</param>
    /// <exception cref="ArgumentException">An input is malformed.</exception>
    /// <exception cref="ArgumentNullException">A required input is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The reason exceeds the supported size.</exception>
    internal static void ValidateDeadLetterInput(
        Guid leaseId,
        OperationId operationId,
        string reasonCode,
        SnapshotMutation snapshotMutation)
    {
        ValidateLeaseId(leaseId);
        ValidateOperationId(operationId, nameof(operationId));
        ValidateDeadLetterReasonCode(reasonCode);
        ValidateSnapshotMutation(snapshotMutation);
    }

    /// <summary>Validates a dead-letter reason code.</summary>
    /// <param name="reasonCode">The reason code.</param>
    /// <exception cref="ArgumentException">The reason code is blank.</exception>
    /// <exception cref="ArgumentNullException">The reason code is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The reason code exceeds the supported size.</exception>
    internal static void ValidateDeadLetterReasonCode(string reasonCode)
    {
        ArgumentExceptionHelper.ThrowIfNull(reasonCode);
        ThrowIfBlank(reasonCode, nameof(reasonCode), "Dead-letter reason code must be non-empty.");
        var reasonBytes = Encoding.UTF8.GetByteCount(reasonCode);
        if (reasonBytes <= MaximumDeadLetterReasonBytes)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(reasonCode), reasonBytes, "Dead-letter reason code exceeds the supported size.");
    }

    /// <summary>Validates retry state persistence input.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="retryState">The retry state.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="ArgumentNullException">The retry state is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateRetryStateInput(OperationId operationId, RetryState retryState)
    {
        ValidateOperationId(operationId, nameof(operationId));
        ArgumentExceptionHelper.ThrowIfNull(retryState);
        if (retryState.TransientAttemptCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryState), retryState.TransientAttemptCount, "Retry attempt count must not be negative.");
        }

        if (retryState.PreviousDelay is { } delay && delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryState), delay, "Retry delay must not be negative.");
        }

        _ = retryState.AuthenticationState switch
        {
            RetryAuthenticationState.None or RetryAuthenticationState.RenewalRetryUsed => true,
            _ => throw new ArgumentException("Retry authentication state must be a defined value.", nameof(retryState)),
        };
    }

    /// <summary>Validates snapshot mutation input.</summary>
    /// <param name="snapshotMutation">The snapshot mutation.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateSnapshotMutation(SnapshotMutation snapshotMutation)
    {
        if (snapshotMutation.ExpectedRevision < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(snapshotMutation), snapshotMutation.ExpectedRevision, "ExpectedRevision must not be negative.");
        }

        if (snapshotMutation.FormatVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(snapshotMutation), snapshotMutation.FormatVersion, "FormatVersion must be positive.");
        }

        if (snapshotMutation.ExpectedRevision == long.MaxValue)
        {
            throw new InvalidOperationException("The next durable snapshot revision would overflow.");
        }

        ValidatePayload(snapshotMutation.State, nameof(snapshotMutation));
        if (snapshotMutation.AuthoritativeState is null)
        {
            return;
        }

        ValidateAuthoritativePayload(snapshotMutation.AuthoritativeState, nameof(snapshotMutation));
    }

    /// <summary>Validates an authoritative payload envelope before writing or after reading.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidateAuthoritativePayload(PayloadEnvelope payload, string parameterName)
    {
        ValidatePayload(payload, parameterName);
        if (!payload.PayloadHash.StartsWith(Sha256PayloadHashPrefix, StringComparison.Ordinal))
        {
            return;
        }

        var computedHash = ComputePayloadHash(payload.Payload);
        if (payload.PayloadHash.Length == Sha256PayloadHashLength
            && PayloadHashEquals(payload.PayloadHash, computedHash))
        {
            return;
        }

        throw new InvalidOperationException("The SQLite authoritative payload hash does not match the payload bytes.");
    }

    /// <summary>Validates a payload envelope before writing or after reading.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    /// <exception cref="ArgumentNullException">A required value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A numeric value is outside the supported range.</exception>
    internal static void ValidatePayload(PayloadEnvelope payload, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload, parameterName);
        ThrowIfBlank(payload.ContractId, parameterName, "Payload ContractId must be non-empty.");
        ThrowIfBlank(payload.ContentType, parameterName, "Payload ContentType must be non-empty.");
        ThrowIfBlank(payload.PayloadHash, parameterName, "PayloadHash must be non-empty.");
        if (payload.SchemaVersion > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, payload.SchemaVersion, "Payload schema version must be positive.");
    }

    /// <summary>Validates operation metadata.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        foreach (var pair in metadata)
        {
            ThrowIfBlank(pair.Key, nameof(metadata), "Metadata keys must be non-empty.");
            ArgumentExceptionHelper.ThrowIfNull(pair.Value, nameof(metadata));
        }
    }

    /// <summary>Validates operation type.</summary>
    /// <param name="operationType">The operation type.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateOperationType(SyncOperationType operationType) =>
        _ = operationType switch
        {
            SyncOperationType.Append or SyncOperationType.Update or SyncOperationType.Delete or SyncOperationType.Custom => true,
            _ => throw new ArgumentException("Operation type must be a defined value.", nameof(operationType)),
        };

    /// <summary>Validates operation id.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateOperationId(OperationId operationId, string parameterName)
    {
        if (operationId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("OperationId must be non-empty.", parameterName);
    }

    /// <summary>Validates stream id.</summary>
    /// <param name="streamId">The stream id.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ValidateStreamId(StreamId streamId, string parameterName)
    {
        if (streamId.Value is { Length: > 0 })
        {
            return;
        }

        throw new ArgumentException("StreamId must be non-empty.", parameterName);
    }

    /// <summary>Rejects unsupported non-file SQLite path forms.</summary>
    /// <param name="databasePath">The requested database path.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ThrowIfUnsupportedPath(string databasePath)
    {
        if (!string.Equals(databasePath, ":memory:", StringComparison.OrdinalIgnoreCase)
            && !databasePath.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new ArgumentException("The SQLite database path must identify a real file.", nameof(databasePath));
    }

    /// <summary>Throws when text is null, empty, or white space.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="message">The exception message.</param>
    /// <exception cref="ArgumentException">The supplied value is invalid.</exception>
    /// <exception cref="InvalidOperationException">The supplied value is not supported by the SQLite local commit schema.</exception>
    internal static void ThrowIfBlank(string? value, string parameterName, string message)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        for (var index = 0; index < value.Length; index++)
        {
            if (!char.IsWhiteSpace(value[index]))
            {
                return;
            }
        }

        throw new ArgumentException(message, parameterName);
    }

    /// <summary>Computes a canonical SHA-256 payload hash.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The formatted SHA-256 payload hash.</returns>
    private static string ComputePayloadHash(ReadOnlyMemory<byte> payload)
    {
#if NET5_0_OR_GREATER
        var hash = SHA256.HashData(payload.Span);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(payload.ToArray());
#endif
        return Sha256PayloadHashPrefix + Convert.ToBase64String(hash);
    }

    /// <summary>Determines whether two payload hashes match.</summary>
    /// <param name="left">The first hash.</param>
    /// <param name="right">The second hash.</param>
    /// <returns>Whether the hashes match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool PayloadHashEquals(string left, string right) =>
#if NET5_0_OR_GREATER
        CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(left), Encoding.UTF8.GetBytes(right));
#else
        string.Equals(left, right, StringComparison.Ordinal);
#endif

    /// <summary>Throws when a duration is not positive.</summary>
    /// <param name="value">The duration.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <param name="message">The exception message.</param>
    /// <exception cref="ArgumentOutOfRangeException">The duration is not positive.</exception>
    private static void ThrowIfNotPositive(TimeSpan value, string parameterName, string message)
    {
        if (value > TimeSpan.Zero)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, message);
    }

    /// <summary>Validates one remote event.</summary>
    /// <param name="batch">The owning batch.</param>
    /// <param name="remoteEvent">The event to validate.</param>
    /// <param name="seen">The set of event identifiers already seen in this batch.</param>
    /// <exception cref="ArgumentException">The remote event is invalid.</exception>
    /// <exception cref="ArgumentNullException">The remote event is null.</exception>
    private static void ValidateRemoteEvent(RemoteEventBatch batch, RemoteEvent remoteEvent, HashSet<Guid> seen)
    {
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        if (remoteEvent.EventId == Guid.Empty)
        {
            throw new ArgumentException("Remote event identifiers must be non-empty.", nameof(batch));
        }

        if (!seen.Add(remoteEvent.EventId))
        {
            throw new ArgumentException("Remote event identifiers must be unique within a batch.", nameof(batch));
        }

        if (remoteEvent.StreamId != batch.StreamId)
        {
            throw new ArgumentException("Remote events must target the batch stream.", nameof(batch));
        }

        ThrowIfBlank(remoteEvent.ServerCursor, nameof(batch), "Remote event cursors must be non-empty.");
        if (remoteEvent.CausedByOperationId.HasValue && remoteEvent.CausedByOperationId.Value.Value == Guid.Empty)
        {
            throw new ArgumentException("Remote event causal operation identifiers must be non-empty.", nameof(batch));
        }

        ValidatePayload(remoteEvent.Payload, nameof(batch));
        ValidateMetadata(remoteEvent.Metadata);
    }
}
