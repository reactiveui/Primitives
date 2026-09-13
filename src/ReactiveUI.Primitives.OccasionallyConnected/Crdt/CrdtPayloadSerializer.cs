// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Serializes built-in CRDT state and input contracts as bounded binary payloads.</summary>
[System.Diagnostics.DebuggerDisplay("{ContentType,nq}")]
public sealed class CrdtPayloadSerializer : IPayloadSerializer
{
    /// <summary>The binary CRDT content type.</summary>
    private const string BinaryContentType = "application/vnd.reactiveui.oc.crdt+binary";

    /// <summary>The CRDT bounds.</summary>
    private readonly CrdtBounds _bounds;

    /// <summary>Initializes a new instance of the <see cref="CrdtPayloadSerializer"/> class.</summary>
    public CrdtPayloadSerializer()
        : this(CrdtBounds.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CrdtPayloadSerializer"/> class.</summary>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="ArgumentNullException"><paramref name="bounds"/> is <see langword="null"/>.</exception>
    public CrdtPayloadSerializer(CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        _bounds = bounds;
    }

    /// <inheritdoc/>
    public string ContentType => BinaryContentType;

    /// <inheritdoc/>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="PayloadSchemaException">The requested contract, version, or value type is not a built-in CRDT payload.</exception>
    public ValueTask<PayloadEnvelope> SerializeAsync<T>(
        string contractId,
        int schemaVersion,
        T value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateSchema(contractId, schemaVersion);
        byte[] payload;
        if (typeof(T) == typeof(CrdtState) && string.Equals(contractId, CrdtContracts.StateContractId, StringComparison.Ordinal) && value is CrdtState state)
        {
            payload = CrdtCodec.EncodeState(state, _bounds);
            return new(new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload)));
        }

        if (typeof(T) == typeof(CrdtInput) && string.Equals(contractId, CrdtContracts.InputContractId, StringComparison.Ordinal) && value is CrdtInput input)
        {
            payload = CrdtCodec.EncodeInput(input, _bounds);
            return new(new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload)));
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The requested CRDT payload type is not allowlisted.");
    }

    /// <inheritdoc/>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="PayloadSchemaException">The envelope cannot be read as the requested CRDT type.</exception>
    public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(envelope);
        ArgumentExceptionHelper.ThrowIfNull(targetType);
        ValidateEnvelope(envelope);
        if (targetType == typeof(CrdtState) && string.Equals(envelope.ContractId, CrdtContracts.StateContractId, StringComparison.Ordinal))
        {
            return new(DecodePayload(envelope, isState: true));
        }

        if (targetType == typeof(CrdtInput) && string.Equals(envelope.ContractId, CrdtContracts.InputContractId, StringComparison.Ordinal))
        {
            return new(DecodePayload(envelope, isState: false));
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The requested CRDT payload type is not allowlisted.");
    }

    /// <summary>Validates CRDT contract metadata.</summary>
    /// <param name="contractId">The contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <exception cref="PayloadSchemaException">The contract or schema is not supported.</exception>
    private static void ValidateSchema(string contractId, int schemaVersion)
    {
        if (!string.Equals(contractId, CrdtContracts.StateContractId, StringComparison.Ordinal)
            && !string.Equals(contractId, CrdtContracts.InputContractId, StringComparison.Ordinal))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, "The CRDT payload contract is not registered.");
        }

        if (schemaVersion == CrdtContracts.SchemaVersion)
        {
            return;
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, "The CRDT schema version is not supported.");
    }

    /// <summary>Decodes validated envelope bytes with a stable schema-failure boundary.</summary>
    /// <param name="envelope">The validated envelope.</param>
    /// <param name="isState">Whether the requested value is a state.</param>
    /// <returns>The decoded owned value.</returns>
    /// <exception cref="PayloadSchemaException">The encoded value is malformed.</exception>
    private object DecodePayload(PayloadEnvelope envelope, bool isState)
    {
        try
        {
            return isState ? CrdtCodec.DecodeState(envelope.Payload, _bounds) : CrdtCodec.DecodeInput(envelope.Payload, _bounds);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or OverflowException)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.DeserializationFailed, "The CRDT payload is invalid.");
        }
    }

    /// <summary>Validates an envelope before decoding.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <exception cref="PayloadSchemaException">The envelope metadata or payload hash is invalid.</exception>
    private void ValidateEnvelope(PayloadEnvelope envelope)
    {
        ValidateSchema(envelope.ContractId, envelope.SchemaVersion);
        if (envelope.PayloadLength > _bounds.MaximumEncodedBytes)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadTooLarge, "The CRDT payload exceeds the configured byte limit.");
        }

        if (!string.Equals(envelope.ContentType, ContentType, StringComparison.Ordinal))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.ContentTypeMismatch, "The CRDT payload content type is invalid.");
        }

        var computed = JsonPayloadSerializer.ComputePayloadHash(envelope.Payload.Span);
        var storedHashBytes = Encoding.UTF8.GetBytes(envelope.PayloadHash);
        var computedHashBytes = Encoding.UTF8.GetBytes(computed);
#if NETFRAMEWORK
        if (envelope.PayloadHash.Length == computed.Length
            && FixedTimeEquals(storedHashBytes, computedHashBytes))
#else
        if (envelope.PayloadHash.Length == computed.Length
            && System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(storedHashBytes, computedHashBytes))
#endif
        {
            return;
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadHashMismatch, "The CRDT payload hash does not match.");

#if NETFRAMEWORK
        static bool FixedTimeEquals(byte[] left, byte[] right)
        {
            if (left.Length != right.Length)
            {
                return false;
            }

            var difference = 0;
            for (var index = 0; index < left.Length; index++)
            {
                difference |= left[index] ^ right[index];
            }

            return difference == 0;
        }
#endif
    }
}
