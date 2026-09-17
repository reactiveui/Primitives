// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <content>Commits caller-supplied serialized operations through the local projection path.</content>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>Commits a caller-supplied serialized operation atomically with its optimistic snapshot.</summary>
    /// <param name="operation">The caller-supplied serialized operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="operation"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The operation does not match the current stream, sequence, or payload contract.</exception>
    internal async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitSerializedAsync(
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        EnterExclusive();
        try
        {
            ValidateSerializedOperationHeader(operation);
            ValidatePolicy(operation.Policy);
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var observed = Current;
            ThrowIfSequenceOverflow(operation.ClientSequence);
            ThrowIfRevisionOverflow(observed.Revision);
            if (operation.ClientSequence != observed.NextClientSequence)
            {
                throw new InvalidOperationException("Serialized operation client sequence does not match the next expected sequence.");
            }

            return await CommitObservedSerializedAsync(operation, observed, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Commits a captured input payload atomically with its optimistic snapshot.</summary>
    /// <param name="payload">The caller-supplied serialized input payload.</param>
    /// <param name="policy">The persisted operation policy.</param>
    /// <param name="baseVersion">The optional authoritative version observed by the caller.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    /// <exception cref="InvalidOperationException">The payload or metadata does not match the contract.</exception>
    internal async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitSerializedAsync(
        PayloadEnvelope payload,
        OperationPolicy policy,
        string? baseVersion,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        ArgumentExceptionHelper.ThrowIfNull(policy);
        EnterExclusive();
        try
        {
            ValidatePolicy(policy);
            SerializedOperationValidation.ValidateOptionalText(baseVersion, "Operation base version is malformed.");
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var observed = Current;
            ThrowIfSequenceOverflow(observed.NextClientSequence);
            ThrowIfRevisionOverflow(observed.Revision);
            var operationId = _options.Dependencies.OperationIdSource.New();
            ThrowIfDefaultOperationId(operationId);
            var operation = CreateOperation(
                policy,
                observed.NextClientSequence,
                operationId,
                _options.Dependencies.TimeProvider.GetUtcNow(),
                payload,
                baseVersion);
            ValidateSerializedOperationHeader(operation);
            return await CommitObservedSerializedAsync(operation, observed, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Projects and persists a serialized local operation against observed committer state.</summary>
    /// <param name="operation">The validated serialized operation.</param>
    /// <param name="observed">The state observed under exclusive ownership.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    private async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitObservedSerializedAsync(
        SyncOperation operation,
        LocalStreamCommitterState<TState> observed,
        CancellationToken cancellationToken)
    {
        var decodedInput = await DecodeLocalInputAsync(operation.Payload, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        var prepared = await PrepareProjectionStateAsync(observed, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        var nextStateValue = _options.Dependencies.Projection.ApplyLocal(prepared.State, decodedInput, operation);
        cancellationToken.ThrowIfCancellationRequested();
        return await CommitPreparedLocalAsync(
                operation,
                decodedInput,
                nextStateValue,
                prepared.Payload,
                observed,
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Validates caller-owned operation metadata before payload decoding and projection.</summary>
    /// <param name="operation">The caller-supplied serialized operation.</param>
    /// <exception cref="InvalidOperationException">The operation metadata is malformed or targets another stream.</exception>
    private void ValidateSerializedOperationHeader(SyncOperation operation)
    {
        ThrowIfDefaultOperationId(operation.OperationId);
        if (operation.StreamId != _options.StreamId)
        {
            throw new InvalidOperationException("Serialized operation belongs to a different stream.");
        }

        if (operation.TimestampUtc.Offset != TimeSpan.Zero)
        {
            throw new InvalidOperationException("Serialized operation timestamp must be UTC.");
        }

        if (operation.Type is not (SyncOperationType.Append or SyncOperationType.Update or SyncOperationType.Delete or SyncOperationType.Custom))
        {
            throw new InvalidOperationException("Serialized operation type must be a defined value.");
        }

        ValidateSerializedInputPayload(operation.Payload);
        SerializedOperationValidation.ValidateOptionalText(operation.BaseVersion, "Serialized operation base version is malformed.");
        SerializedOperationValidation.ValidateMetadata(operation.Metadata);
    }

    /// <summary>Validates the caller-owned payload envelope before deserialization.</summary>
    /// <param name="payload">The serialized input payload.</param>
    /// <exception cref="InvalidOperationException">The payload is missing or does not match the input contract.</exception>
    private void ValidateSerializedInputPayload(PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            throw new InvalidOperationException("Serialized operation payload is missing.");
        }

        var contractMatches = string.Equals(payload.ContractId, _options.Contracts.InputContractId, StringComparison.Ordinal)
            && payload.SchemaVersion > 0
            && payload.SchemaVersion <= _options.Contracts.InputSchemaVersion
            && !string.IsNullOrWhiteSpace(payload.ContentType)
            && !string.IsNullOrWhiteSpace(payload.PayloadHash);
        if (contractMatches)
        {
            return;
        }

        throw new InvalidOperationException("Serialized operation payload contract does not match the configured input contract.");
    }

    /// <summary>Validates caller-owned serialized operation metadata before projection.</summary>
    private static class SerializedOperationValidation
    {
        /// <summary>The message used when serialized operation metadata is malformed.</summary>
        private const string MalformedMetadataMessage = "Serialized operation metadata is malformed.";

        /// <summary>Strict UTF-8 encoder used to reject malformed persisted operation text.</summary>
        private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

        /// <summary>Validates caller-owned optional operation text before projection.</summary>
        /// <param name="value">The optional text.</param>
        /// <param name="message">The exception message.</param>
        /// <exception cref="InvalidOperationException">The text is malformed.</exception>
        internal static void ValidateOptionalText(string? value, string message)
        {
            if (value is null)
            {
                return;
            }

            ValidateText(value, message);
        }

        /// <summary>Validates caller-owned metadata before projection.</summary>
        /// <param name="metadata">The metadata dictionary.</param>
        /// <exception cref="InvalidOperationException">The metadata is malformed.</exception>
        internal static void ValidateMetadata(IReadOnlyDictionary<string, string>? metadata)
        {
            ArgumentExceptionHelper.ThrowIfNull(metadata);
            foreach (var pair in metadata)
            {
                ValidateRequiredText(pair.Key, MalformedMetadataMessage);
                ValidatePresentText(pair.Value, MalformedMetadataMessage);
            }
        }

        /// <summary>Validates caller-owned present operation text before projection.</summary>
        /// <param name="value">The present text.</param>
        /// <param name="message">The exception message.</param>
        /// <exception cref="InvalidOperationException">The text is missing or malformed.</exception>
        private static void ValidatePresentText(string? value, string message)
        {
            if (value is null)
            {
                throw new InvalidOperationException(message);
            }

            ValidateText(value, message);
        }

        /// <summary>Validates caller-owned required operation text before projection.</summary>
        /// <param name="value">The required text.</param>
        /// <param name="message">The exception message.</param>
        /// <exception cref="InvalidOperationException">The text is missing or malformed.</exception>
        private static void ValidateRequiredText(string value, string message)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new InvalidOperationException(message);
            }

            ValidateText(value, message);
        }

        /// <summary>Validates caller-owned operation text can be encoded losslessly as UTF-8.</summary>
        /// <param name="value">The text value.</param>
        /// <param name="message">The exception message.</param>
        /// <exception cref="InvalidOperationException">The text is malformed.</exception>
        private static void ValidateText(string value, string message)
        {
            try
            {
                _ = StrictUtf8.GetByteCount(value);
            }
            catch (EncoderFallbackException exception)
            {
                throw new InvalidOperationException(message, exception);
            }
        }
    }
}
