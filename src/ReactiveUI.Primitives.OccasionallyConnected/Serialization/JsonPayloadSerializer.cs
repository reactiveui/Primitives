// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Serializes allowlisted payload contracts as canonical JSON bytes.</summary>
[DebuggerDisplay("{ContentType,nq}, MaxPayloadBytes = {_maximumPayloadBytes}")]
public sealed class JsonPayloadSerializer : IPayloadSerializer
{
    /// <summary>The SHA-256 hash prefix used in payload envelopes.</summary>
    private const string Sha256Prefix = "sha256-";

    /// <summary>The length of the prefix and base64-encoded SHA-256 digest.</summary>
    private const int Sha256HashLength = 51;

    /// <summary>The message used when a payload exceeds the configured byte limit.</summary>
    private const string PayloadTooLargeMessage = "The payload exceeds the configured byte limit.";

    /// <summary>The default maximum number of payload bytes accepted by the serializer.</summary>
    private const int DefaultMaximumPayloadBytes = 1_048_576;

    /// <summary>The allowlisted schema registry.</summary>
    private readonly SchemaRegistry _schemaRegistry;

    /// <summary>The maximum number of payload bytes accepted by the serializer.</summary>
    private readonly int _maximumPayloadBytes;

    /// <summary>Initializes a new instance of the <see cref="JsonPayloadSerializer"/> class.</summary>
    /// <param name="schemaRegistry">The allowlisted schema registry.</param>
    /// <exception cref="ArgumentNullException"><paramref name="schemaRegistry"/> is <see langword="null"/>.</exception>
    public JsonPayloadSerializer(SchemaRegistry schemaRegistry)
        : this(schemaRegistry, DefaultMaximumPayloadBytes)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JsonPayloadSerializer"/> class.</summary>
    /// <param name="schemaRegistry">The allowlisted schema registry.</param>
    /// <param name="maximumPayloadBytes">The maximum number of payload bytes accepted by the serializer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="schemaRegistry"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumPayloadBytes"/> is less than one.</exception>
    public JsonPayloadSerializer(SchemaRegistry schemaRegistry, int maximumPayloadBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(schemaRegistry);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumPayloadBytes);

        _schemaRegistry = schemaRegistry.Snapshot();
        _maximumPayloadBytes = maximumPayloadBytes;
    }

    /// <inheritdoc/>
    public string ContentType => "application/json";

    /// <summary>Computes the payload hash for canonical payload bytes.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The formatted SHA-256 payload hash.</returns>
    public static string ComputePayloadHash(ReadOnlySpan<byte> payload)
    {
#if NET5_0_OR_GREATER
        var hash = SHA256.HashData(payload);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(payload.ToArray());
#endif
        return Sha256Prefix + Convert.ToBase64String(hash);
    }

    /// <summary>Serializes an allowlisted payload value.</summary>
    /// <typeparam name="T">The payload value type.</typeparam>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The positive contract schema version.</param>
    /// <param name="value">The payload value.</param>
    /// <returns>The serialized payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<PayloadEnvelope> SerializeAsync<T>(string contractId, int schemaVersion, T value) =>
        SerializeAsync(contractId, schemaVersion, value, CancellationToken.None);

    /// <inheritdoc/>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="PayloadSchemaException">The requested schema is not registered or the serialized payload is too large.</exception>
    public ValueTask<PayloadEnvelope> SerializeAsync<T>(
        string contractId,
        int schemaVersion,
        T value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var jsonTypeInfo = _schemaRegistry.GetJsonTypeInfo(contractId, schemaVersion, typeof(T));
        BoundedPayloadBufferWriter bufferWriter = new(_maximumPayloadBytes);
        try
        {
            using Utf8JsonWriter jsonWriter = new(bufferWriter);
            JsonSerializer.Serialize(jsonWriter, value, (JsonTypeInfo<T>)jsonTypeInfo);
            jsonWriter.Flush();
        }
        catch (PayloadSchemaException)
        {
            throw;
        }
        catch (JsonException exception)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.SerializationFailed, "The payload could not be serialized as the registered schema.", exception);
        }

        var payload = bufferWriter.ToArray();
        return new(new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, ComputePayloadHash(payload)));
    }

    /// <summary>Deserializes an allowlisted payload envelope.</summary>
    /// <param name="envelope">The payload envelope to deserialize.</param>
    /// <param name="targetType">The requested target type.</param>
    /// <returns>The deserialized payload value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType) =>
        DeserializeAsync(envelope, targetType, CancellationToken.None);

    /// <inheritdoc/>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="PayloadSchemaException">The envelope cannot be read as the requested schema.</exception>
    public async ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(envelope);
        ArgumentExceptionHelper.ThrowIfNull(targetType);
        _ = ValidateEnvelope(envelope);

        var targetVersion = _schemaRegistry.GetTargetSchemaVersion(envelope.ContractId, targetType);
        var current = envelope;
        if (current.SchemaVersion != targetVersion)
        {
            var chain = _schemaRegistry.GetUpcastChain(envelope.ContractId, envelope.SchemaVersion, targetVersion);
            for (var index = 0; index < chain.Count; index++)
            {
                current = await UpcastAsync(chain[index], current, cancellationToken).ConfigureAwait(false);
                _ = ValidateUpcastResult(envelope.ContractId, index == chain.Count - 1 ? targetVersion : chain[index].ToVersion, current);
            }
        }

        var jsonTypeInfo = _schemaRegistry.GetJsonTypeInfo(current.ContractId, current.SchemaVersion, targetType);
        try
        {
            var payload = ValidateEnvelope(current);
            var value = JsonSerializer.Deserialize(payload.Span, jsonTypeInfo);
            return value ?? throw new PayloadSchemaException(PayloadSchemaFailureReason.DeserializationFailed, "The payload deserialized to null.");
        }
        catch (JsonException exception)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.DeserializationFailed, "The payload is not valid for the requested schema.", exception);
        }
    }

    /// <summary>Runs an upcaster and converts upcaster failures into stable schema failures.</summary>
    /// <param name="upcaster">The upcaster to run.</param>
    /// <param name="source">The source envelope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The upcast envelope.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
    /// <exception cref="PayloadSchemaException">The upcaster failed or returned no envelope.</exception>
    private static async ValueTask<PayloadEnvelope> UpcastAsync(IPayloadUpcaster upcaster, PayloadEnvelope source, CancellationToken cancellationToken)
    {
        try
        {
            var envelope = await upcaster.UpcastAsync(source, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return envelope ?? throw new PayloadSchemaException(PayloadSchemaFailureReason.UpcasterFailed, "The upcaster returned no payload envelope.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PayloadSchemaException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UpcasterFailed, "The upcaster failed while converting the payload schema.", exception);
        }
    }

    /// <summary>Validates envelope metadata and payload integrity.</summary>
    /// <param name="envelope">The envelope to validate.</param>
    /// <returns>The validated payload copy.</returns>
    /// <exception cref="PayloadSchemaException">The envelope metadata or hash is invalid.</exception>
    private ReadOnlyMemory<byte> ValidateEnvelope(PayloadEnvelope envelope)
    {
        if (!string.Equals(envelope.ContentType, ContentType, StringComparison.Ordinal))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.ContentTypeMismatch, "The payload content type does not match the JSON serializer.");
        }

        if (!_schemaRegistry.ContainsContract(envelope.ContractId))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, "The payload contract is not registered.");
        }

        if (envelope.SchemaVersion <= 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, "The payload schema version must be positive.");
        }

        ThrowIfPayloadTooLarge(envelope.PayloadLength);
        var payload = envelope.Payload;
        var computedHash = ComputePayloadHash(payload.Span);
        if (envelope.PayloadHash is not { Length: Sha256HashLength })
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadHashMismatch, "The payload hash has an invalid encoded length.");
        }

#if NET5_0_OR_GREATER
        if (CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(envelope.PayloadHash), Encoding.UTF8.GetBytes(computedHash)))
#else
        if (string.Equals(envelope.PayloadHash, computedHash, StringComparison.Ordinal))
#endif
        {
            return payload;
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadHashMismatch, "The payload hash does not match the stored payload bytes.");
    }

    /// <summary>Rejects payloads larger than the configured byte limit.</summary>
    /// <param name="payloadLength">The payload byte length.</param>
    /// <exception cref="PayloadSchemaException"><paramref name="payloadLength"/> exceeds the configured limit.</exception>
    private void ThrowIfPayloadTooLarge(int payloadLength)
    {
        if (payloadLength <= _maximumPayloadBytes)
        {
            return;
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadTooLarge, PayloadTooLargeMessage);
    }

    /// <summary>Validates an upcast result before the next step or final deserialization.</summary>
    /// <param name="contractId">The expected stable wire contract identifier.</param>
    /// <param name="schemaVersion">The expected schema version.</param>
    /// <param name="envelope">The upcast envelope.</param>
    /// <returns>The validated upcast payload copy.</returns>
    /// <exception cref="PayloadSchemaException">The upcaster changed immutable metadata or emitted an invalid envelope.</exception>
    private ReadOnlyMemory<byte> ValidateUpcastResult(string contractId, int schemaVersion, PayloadEnvelope envelope)
    {
        if (string.Equals(envelope.ContractId, contractId, StringComparison.Ordinal)
            && envelope.SchemaVersion == schemaVersion
            && string.Equals(envelope.ContentType, ContentType, StringComparison.Ordinal))
        {
            return ValidateEnvelope(envelope);
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.UpcasterContractMismatch, "The upcaster changed immutable payload contract metadata.");
    }

    /// <summary>Writes UTF-8 JSON bytes while enforcing an exact payload byte limit.</summary>
    internal sealed class BoundedPayloadBufferWriter : IBufferWriter<byte>
    {
        /// <summary>The default initial buffer length.</summary>
        private const int InitialBufferLength = 256;

        /// <summary>The factor used when expanding the bounded serialization buffer.</summary>
        private const int BufferGrowthFactor = 2;

        /// <summary>The maximum JSON escape expansion per input character.</summary>
        private const int JsonEscapeExpansion = 6;

        /// <summary>The scratch space allowed for the JSON writer's fixed-size growth requests.</summary>
        private const int JsonWriterScratchBytes = 4096;

        /// <summary>The maximum number of bytes this writer can store.</summary>
        private readonly int _maximumLength;

        /// <summary>The bounded scratch allocation limit, separate from the committed payload limit.</summary>
        private readonly int _maximumBufferLength;

        /// <summary>The backing buffer that receives serialized bytes.</summary>
        private byte[] _buffer;

        /// <summary>The number of bytes written to <see cref="_buffer"/>.</summary>
        private int _written;

        /// <summary>Initializes a new instance of the <see cref="BoundedPayloadBufferWriter"/> class.</summary>
        /// <param name="maximumLength">The maximum number of bytes the writer can store.</param>
        public BoundedPayloadBufferWriter(int maximumLength)
        {
            _maximumLength = maximumLength;
            _maximumBufferLength = (int)Math.Min(int.MaxValue, ((long)maximumLength * JsonEscapeExpansion) + JsonWriterScratchBytes);
            _buffer = new byte[InitialBufferLength];
        }

        /// <inheritdoc/>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
        /// <exception cref="PayloadSchemaException"><paramref name="count"/> would exceed the configured limit.</exception>
        public void Advance(int count)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);
            if (count > _maximumLength - _written)
            {
                throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadTooLarge, PayloadTooLargeMessage);
            }

            _written += count;
        }

        /// <inheritdoc/>
        /// <exception cref="PayloadSchemaException"><paramref name="sizeHint"/> cannot fit within the configured limit.</exception>
        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsMemory(_written);
        }

        /// <inheritdoc/>
        /// <exception cref="PayloadSchemaException"><paramref name="sizeHint"/> cannot fit within the configured limit.</exception>
        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsSpan(_written);
        }

        /// <summary>Copies the written bytes into a right-sized array.</summary>
        /// <returns>The serialized payload bytes.</returns>
        internal byte[] ToArray()
        {
            var payload = new byte[_written];
            Array.Copy(_buffer, payload, _written);
            return payload;
        }

        /// <summary>Ensures the buffer can accept a requested write.</summary>
        /// <param name="sizeHint">The requested contiguous byte count.</param>
        /// <exception cref="PayloadSchemaException"><paramref name="sizeHint"/> cannot fit within the configured limit.</exception>
        private void EnsureCapacity(int sizeHint)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(sizeHint);
            var requiredHint = sizeHint == 0 ? 1 : sizeHint;
            if (_written == _maximumLength || requiredHint > _maximumBufferLength - _written)
            {
                throw new PayloadSchemaException(PayloadSchemaFailureReason.PayloadTooLarge, PayloadTooLargeMessage);
            }

            var requiredLength = _written + requiredHint;
            if (requiredLength <= _buffer.Length)
            {
                return;
            }

            var doubledLength = (int)Math.Min((long)_buffer.Length * BufferGrowthFactor, _maximumBufferLength);
            var nextLength = Math.Max(requiredLength, doubledLength);
            Array.Resize(ref _buffer, nextLength);
        }
    }
}
