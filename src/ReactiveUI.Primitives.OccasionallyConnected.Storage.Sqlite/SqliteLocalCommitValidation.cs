// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Validates SQLite local commit store input.</summary>
internal static class SqliteLocalCommitValidation
{
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
