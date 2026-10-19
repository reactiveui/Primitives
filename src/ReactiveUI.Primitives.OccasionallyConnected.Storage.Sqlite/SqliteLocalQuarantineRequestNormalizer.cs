// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Normalizes SQLite quarantine requests before sizing or persistence.</summary>
internal static class SqliteLocalQuarantineRequestNormalizer
{
    /// <summary>The maximum quarantine request metadata length in UTF-8 bytes.</summary>
    private const int MaximumQuarantineMetadataUtf8Bytes = 4096;

    /// <summary>The strict UTF-8 encoding used for quarantine metadata validation.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Creates a normalized quarantine request that is safe to size and persist.</summary>
    /// <param name="request">The request.</param>
    /// <returns>The normalized request with non-null bounded evidence.</returns>
    /// <exception cref="ArgumentException">The request or evidence is malformed.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The evidence length is negative.</exception>
    internal static SqliteNormalizedPayloadQuarantineRequest Normalize(LocalPayloadQuarantineRequest request)
    {
        ValidateQuarantineRequest(request);
        var evidence = request.Evidence is null
            ? LocalPayloadQuarantineEvidenceFactory.FromEnvelope(
                request.Envelope,
                LocalPayloadQuarantineEvidenceFactory.DefaultMaximumEvidenceBytes)
            : LocalPayloadQuarantineEvidenceFactory.FromEvidence(
                request.Evidence,
                LocalPayloadQuarantineEvidenceFactory.DefaultMaximumEvidenceBytes);
        var normalizedRequest = request with
        {
            Evidence = evidence,
            Envelope = null,
        };

        return new(normalizedRequest, evidence);
    }

    /// <summary>Validates a quarantine request.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    /// <exception cref="ArgumentNullException">The request is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The evidence length is negative.</exception>
    private static void ValidateQuarantineRequest(LocalPayloadQuarantineRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        SqliteLocalCommitValidation.ValidateStreamId(request.StreamId, nameof(request));
        ValidateQuarantineSubscriptionId(request);
        ValidateQuarantineOperationId(request);
        ValidateQuarantineEventId(request);
        ValidateQuarantineSource(request.Source);
        ValidateQuarantineReason(request.Reason);
        ValidateQuarantineMetadata(request);
        if (request.ObservedAtUtc == default)
        {
            throw new ArgumentException("Observation timestamp must be set.", nameof(request));
        }

        if (request.Evidence is not null || request.Envelope is not null)
        {
            return;
        }

        throw new ArgumentException("Quarantine evidence or envelope must be supplied.", nameof(request));
    }

    /// <summary>Validates a quarantine subscription identifier.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The identifier is malformed.</exception>
    private static void ValidateQuarantineSubscriptionId(LocalPayloadQuarantineRequest request)
    {
        if (!request.SubscriptionId.HasValue || request.SubscriptionId.Value.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("Subscription identifier must be non-empty.", nameof(request));
    }

    /// <summary>Validates a quarantine operation identifier.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The identifier is malformed.</exception>
    private static void ValidateQuarantineOperationId(LocalPayloadQuarantineRequest request)
    {
        if (!request.OperationId.HasValue || request.OperationId.Value.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("Operation identifier must be non-empty.", nameof(request));
    }

    /// <summary>Validates a quarantine event identifier.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The identifier is malformed.</exception>
    private static void ValidateQuarantineEventId(LocalPayloadQuarantineRequest request)
    {
        if (!request.EventId.HasValue || request.EventId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("Event identifier must be non-empty.", nameof(request));
    }

    /// <summary>Validates a quarantine source.</summary>
    /// <param name="source">The source.</param>
    /// <exception cref="ArgumentException">The source is malformed.</exception>
    private static void ValidateQuarantineSource(LocalPayloadQuarantineSource source)
    {
        if (source is LocalPayloadQuarantineSource.Snapshot or LocalPayloadQuarantineSource.OutboxOperation or LocalPayloadQuarantineSource.RemoteEvent)
        {
            return;
        }

        throw new ArgumentException("Quarantine source must be a defined value.", nameof(source));
    }

    /// <summary>Validates a quarantine reason.</summary>
    /// <param name="reason">The reason.</param>
    /// <exception cref="ArgumentException">The reason is malformed.</exception>
    private static void ValidateQuarantineReason(LocalPayloadQuarantineReason reason)
    {
        if (reason is LocalPayloadQuarantineReason.SchemaRejected or LocalPayloadQuarantineReason.PayloadHashMismatch
            or LocalPayloadQuarantineReason.UpcastFailed or LocalPayloadQuarantineReason.PersistedRecordCorrupt)
        {
            return;
        }

        throw new ArgumentException("Quarantine reason must be a defined value.", nameof(reason));
    }

    /// <summary>Validates quarantine request metadata that is persisted with the marker.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The metadata is malformed or too long.</exception>
    private static void ValidateQuarantineMetadata(LocalPayloadQuarantineRequest request)
    {
        ValidateQuarantineMetadataValue(request.ReasonCode, nameof(request));
        ValidateQuarantineMetadataValue(request.Cursor, nameof(request));
    }

    /// <summary>Validates an optional quarantine metadata value.</summary>
    /// <param name="value">The metadata value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The metadata is malformed or too long.</exception>
    private static void ValidateQuarantineMetadataValue(string? value, string parameterName)
    {
        if (value is null)
        {
            return;
        }

        int byteCount;
        try
        {
            byteCount = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw new ArgumentException("Quarantine metadata must be well-formed Unicode.", parameterName, exception);
        }

        if (byteCount <= MaximumQuarantineMetadataUtf8Bytes)
        {
            return;
        }

        throw new ArgumentException("Quarantine metadata must be at most 4096 UTF-8 bytes.", parameterName);
    }
}
