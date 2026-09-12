// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="JsonPayloadSerializer"/>.</summary>
public sealed partial class JsonPayloadSerializerTests
{
    /// <summary>The contract identifier used by reading tests.</summary>
    private const string ReadingContract = "sensor.reading";

    /// <summary>The unknown contract identifier used by rejection tests.</summary>
    private const string UnknownContract = "sensor.unknown";

    /// <summary>The JSON content type used by payload envelopes.</summary>
    private const string JsonContentType = "application/json";

    /// <summary>The first reading schema version.</summary>
    private const int ReadingV1Version = 1;

    /// <summary>The second reading schema version.</summary>
    private const int ReadingV2Version = 2;

    /// <summary>The third reading schema version.</summary>
    private const int ReadingV3Version = 3;

    /// <summary>The sample reading identifier.</summary>
    private const string ReadingId = "alpha";

    /// <summary>The payload byte offset mutated by ownership tests.</summary>
    private const int PayloadMutationOffset = 2;

    /// <summary>The payload byte limit used by rejection tests.</summary>
    private const int TinyPayloadLimit = 10;

    /// <summary>The zero schema version used by rejection tests.</summary>
    private const int InvalidSchemaVersion = 0;

    /// <summary>The sample reading value.</summary>
    private const decimal ReadingValue = 20.5M;

    /// <summary>The canonical version-two reading JSON used by tests.</summary>
    private const string SerializedReadingV2Json = "{\"id\":\"alpha\",\"value\":20.5,\"kind\":\"Temperature\"}";

    /// <summary>Verifies serialization uses registered source-generated metadata and validates the stored hash.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SerializeAsyncWritesCanonicalEnvelopeForRegisteredSchema()
    {
        var serializer = CreateSerializer();
        ReadingV2 value = new(ReadingId, ReadingValue, ReadingKind.Temperature);

        var envelope = await serializer.SerializeAsync(ReadingContract, ReadingV2Version, value);
        var json = Encoding.UTF8.GetString(envelope.Payload.Span);

        await Assert.That(envelope.ContractId).IsEqualTo(ReadingContract);
        await Assert.That(envelope.SchemaVersion).IsEqualTo(ReadingV2Version);
        await Assert.That(envelope.ContentType).IsEqualTo(JsonContentType);
        await Assert.That(envelope.PayloadHash).IsEqualTo("sha256-0bi5I5WsFcFG/RhwpC7ekm8BS8VN9ac1usn/r4wruRc=");
        await Assert.That(json).IsEqualTo(SerializedReadingV2Json);
    }

    /// <summary>Verifies deserialization rejects mismatched hashes before reading JSON.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsCorruptPayloadHash()
    {
        var serializer = CreateSerializer();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, "{}"u8.ToArray(), "sha256-invalid");

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadHashMismatch);
    }

    /// <summary>Verifies deserialization rejects contracts that are not allowlisted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsUnknownContract()
    {
        var serializer = CreateSerializer();
        var envelope = new PayloadEnvelope(UnknownContract, ReadingV1Version, JsonContentType, "{}"u8.ToArray(), "sha256-RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=");

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies deserialization rejects target types outside the registered allowlist.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsUnregisteredTargetType()
    {
        var serializer = CreateSerializer();
        var envelope = await serializer.SerializeAsync(ReadingContract, ReadingV2Version, new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(JsonPayloadSerializerTests)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies deserialization rejects unsupported content types.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsWrongContentType()
    {
        var serializer = CreateSerializer();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, "application/octet-stream", "{}"u8.ToArray(), "sha256-RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=");

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.ContentTypeMismatch);
    }

    /// <summary>Verifies deserialization rejects nonpositive schema versions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsInvalidSchemaVersion()
    {
        var serializer = CreateSerializer();
        var payload = "{}"u8.ToArray();
        var envelope = new PayloadEnvelope(ReadingContract, InvalidSchemaVersion, JsonContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies deserialization rejects payloads that exceed the configured byte limit before hashing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsPayloadThatExceedsByteLimit()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var serializer = new JsonPayloadSerializer(registry, TinyPayloadLimit);
        var payload = CreateV2Payload();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Verifies invalid JSON produces a stable deserialization failure.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsInvalidJson()
    {
        var serializer = CreateSerializer();
        var payload = "{not-json"u8.ToArray();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.DeserializationFailed);
    }

    /// <summary>Verifies JSON null produces a stable deserialization failure.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsNullJson()
    {
        var serializer = CreateSerializer();
        var payload = "null"u8.ToArray();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.DeserializationFailed);
    }

    /// <summary>Verifies deserialization applies each contiguous upcaster before reading the requested type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncAppliesContiguousUpcastChain()
    {
        var serializer = CreateSerializer();
        var envelope = CreateV1Envelope();

        var result = await serializer.DeserializeAsync(envelope, typeof(ReadingV2));

        await Assert.That(result).IsEqualTo(new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature));
    }

    /// <summary>Verifies deserialization can apply more than one contiguous upcaster.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncAppliesMultiStepUpcastChain()
    {
        var registry = new SchemaRegistry()
            .Register(ReadingContract, ReadingV1Version, PayloadJsonContext.Default.ReadingV1)
            .Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2)
            .Register(ReadingContract, ReadingV3Version, PayloadJsonContext.Default.ReadingV3)
            .RegisterUpcaster(new ReadingV1ToV2Upcaster())
            .RegisterUpcaster(new ReadingV2ToV3Upcaster());
        JsonPayloadSerializer serializer = new(registry);
        var envelope = CreateV1Envelope();

        var result = await serializer.DeserializeAsync(envelope, typeof(ReadingV3));

        await Assert.That(result).IsEqualTo(new ReadingV3(ReadingId, ReadingValue, "Temperature"));
    }

    /// <summary>Verifies payload bytes are copied on construction and when read from an envelope.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The payload cannot be mutated through its backing array in this test.</exception>
    [Test]
    public async Task DeserializeAsyncUsesOwnedPayloadBytes()
    {
        var serializer = CreateSerializer();
        var payload = "{\"id\":\"alpha\",\"value\":20.5,\"kind\":\"temperature\"}"u8.ToArray();
        var hash = JsonPayloadSerializer.ComputePayloadHash(payload);
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, payload, hash);
        payload[PayloadMutationOffset] = (byte)'x';

        if (!MemoryMarshal.TryGetArray(envelope.Payload, out var segment) || segment.Array is not { } ownedBytes)
        {
            throw new InvalidOperationException("The test requires an array-backed payload to attempt mutation.");
        }

        ownedBytes[segment.Offset + PayloadMutationOffset] = (byte)'x';

        var result = await serializer.DeserializeAsync(envelope, typeof(ReadingV2));

        await Assert.That(result).IsEqualTo(new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature));
    }

    /// <summary>Verifies serializer construction snapshots registrations for stable concurrent reads.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncUsesRegistrySnapshotCapturedByConstructor()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var serializer = new JsonPayloadSerializer(registry);
        _ = registry.Register(UnknownContract, ReadingV1Version, PayloadJsonContext.Default.ReadingV1);
        var payload = "{\"id\":\"alpha\",\"celsius\":20.5}"u8.ToArray();
        var envelope = new PayloadEnvelope(UnknownContract, ReadingV1Version, JsonContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV1)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies serialization enforces the byte limit while JSON is written.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SerializeAsyncRejectsPayloadThatExceedsByteLimitDuringWrite()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var serializer = new JsonPayloadSerializer(registry, TinyPayloadLimit);

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.SerializeAsync(ReadingContract, ReadingV2Version, new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Verifies serialization maps JSON writer failures to a stable reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SerializeAsyncMapsJsonExceptionsToStableReason()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV1Version, PayloadJsonContext.Default.UnserializablePayload);
        var serializer = new JsonPayloadSerializer(registry);

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.SerializeAsync(ReadingContract, ReadingV1Version, new UnserializablePayload()).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.SerializationFailed);
    }

    /// <summary>Verifies failing upcasters produce stable schema failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncMapsUpcasterExceptionsToStableReason()
    {
        var serializer = CreateSerializer(new ThrowingUpcaster());
        var envelope = CreateV1Envelope();

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UpcasterFailed);
        await Assert.That(exception?.InnerException).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Verifies null upcaster results produce stable schema failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncMapsNullUpcasterResultToStableReason()
    {
        var serializer = CreateSerializer(new NullUpcaster());
        var envelope = CreateV1Envelope();

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UpcasterFailed);
    }

    /// <summary>Verifies schema exceptions from upcasters are preserved.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncPreservesUpcasterSchemaException()
    {
        var serializer = CreateSerializer(new SchemaFailingUpcaster());
        var envelope = CreateV1Envelope();

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies upcasters cannot change the payload contract identifier.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncRejectsUpcasterContractChanges()
    {
        var serializer = CreateSerializer(new ContractChangingUpcaster());
        var envelope = CreateV1Envelope();

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UpcasterContractMismatch);
    }

    /// <summary>Verifies cancellation from an upcaster is preserved.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncPreservesUpcasterCancellation()
    {
        using CancellationTokenSource source = new();
        var serializer = CreateSerializer(new CancelingUpcaster(source.Token));
        var envelope = CreateV1Envelope();
        await source.CancelAsync();

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2), source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
    }

    /// <summary>Verifies cancellation thrown during upcasting is preserved.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeserializeAsyncPreservesInFlightUpcasterCancellation()
    {
        using CancellationTokenSource source = new();
        var serializer = CreateSerializer(new InFlightCancelingUpcaster(source));
        var envelope = CreateV1Envelope();

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV2), source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
    }

    /// <summary>Verifies cancellation is observed before serialization work begins.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SerializeAsyncObservesCancellation()
    {
        var serializer = CreateSerializer();
        using CancellationTokenSource source = new();
        await source.CancelAsync();

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => serializer.SerializeAsync(ReadingContract, ReadingV2Version, new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature), source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
    }

    /// <summary>Creates a serializer configured with reading schemas.</summary>
    /// <returns>The configured serializer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static JsonPayloadSerializer CreateSerializer() => CreateSerializer(new ReadingV1ToV2Upcaster());

    /// <summary>Creates a serializer configured with reading schemas and a specific upcaster.</summary>
    /// <param name="upcaster">The upcaster to register.</param>
    /// <returns>The configured serializer.</returns>
    private static JsonPayloadSerializer CreateSerializer(IPayloadUpcaster upcaster)
    {
        var registry = new SchemaRegistry()
            .Register(ReadingContract, ReadingV1Version, PayloadJsonContext.Default.ReadingV1)
            .Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2)
            .RegisterUpcaster(upcaster);

        return new(registry);
    }

    /// <summary>Creates a valid version-one reading envelope.</summary>
    /// <returns>The version-one envelope.</returns>
    private static PayloadEnvelope CreateV1Envelope()
    {
        var sourceBytes = "{\"id\":\"alpha\",\"celsius\":20.5}"u8.ToArray();
        var sourceHash = JsonPayloadSerializer.ComputePayloadHash(sourceBytes);
        return new(ReadingContract, ReadingV1Version, JsonContentType, sourceBytes, sourceHash);
    }

    /// <summary>Creates canonical version-two reading payload bytes.</summary>
    /// <returns>The canonical version-two reading payload bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] CreateV2Payload() => "{\"id\":\"alpha\",\"value\":20.5,\"kind\":\"Temperature\"}"u8.ToArray();

    /// <summary>Upcasts reading schema v1 to v2.</summary>
    private sealed class ReadingV1ToV2Upcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken)
        {
            var reading = JsonSerializer.Deserialize(source.Payload.Span, PayloadJsonContext.Default.ReadingV1)
                ?? throw new InvalidOperationException("The test requires a version-one reading.");
            var targetBytes = JsonSerializer.SerializeToUtf8Bytes(new(reading.Id, reading.Celsius, ReadingKind.Temperature), PayloadJsonContext.Default.ReadingV2);
            var targetHash = JsonPayloadSerializer.ComputePayloadHash(targetBytes);
            return ValueTask.FromResult(source with
            {
                SchemaVersion = ReadingV2Version,
                Payload = targetBytes,
                PayloadHash = targetHash,
            });
        }
    }

    /// <summary>Upcasts reading schema v2 to v3.</summary>
    private sealed class ReadingV2ToV3Upcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV2Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV3Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken)
        {
            var reading = JsonSerializer.Deserialize(source.Payload.Span, PayloadJsonContext.Default.ReadingV2)
                ?? throw new InvalidOperationException("The test requires a version-two reading.");
            var targetBytes = JsonSerializer.SerializeToUtf8Bytes(new(reading.Id, reading.Value, reading.Kind.ToString()), PayloadJsonContext.Default.ReadingV3);
            return ValueTask.FromResult(source with
            {
                SchemaVersion = ReadingV3Version,
                Payload = targetBytes,
                PayloadHash = JsonPayloadSerializer.ComputePayloadHash(targetBytes),
            });
        }
    }

    /// <summary>Throws when asked to upcast a reading.</summary>
    private sealed class ThrowingUpcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The test upcaster failed.");
    }

    /// <summary>Returns no envelope when asked to upcast a reading.</summary>
    private sealed class NullUpcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            ValueTask.FromResult<PayloadEnvelope>(null!);
    }

    /// <summary>Throws a schema exception when asked to upcast a reading.</summary>
    private sealed class SchemaFailingUpcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The test upcaster rejected the payload type.");
    }

    /// <summary>Changes the contract identifier when asked to upcast a reading.</summary>
    private sealed class ContractChangingUpcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken)
        {
            var targetBytes = CreateV2Payload();
            return ValueTask.FromResult(source with
            {
                ContractId = UnknownContract,
                SchemaVersion = ReadingV2Version,
                Payload = targetBytes,
                PayloadHash = JsonPayloadSerializer.ComputePayloadHash(targetBytes),
            });
        }
    }

    /// <summary>Cancels when asked to upcast a reading.</summary>
    /// <param name="expectedCancellationToken">The cancellation token thrown by the upcaster.</param>
    private sealed class CancelingUpcaster(CancellationToken expectedCancellationToken) : IPayloadUpcaster
    {
        /// <summary>The cancellation token thrown by the upcaster.</summary>
        private readonly CancellationToken _expectedCancellationToken = expectedCancellationToken;

        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            throw new OperationCanceledException(_expectedCancellationToken);
    }

    /// <summary>Cancels the provided token source while upcasting.</summary>
    /// <param name="cancellationTokenSource">The token source canceled by the upcaster.</param>
    private sealed class InFlightCancelingUpcaster(CancellationTokenSource cancellationTokenSource) : IPayloadUpcaster
    {
        /// <summary>The cancellation token source canceled by the upcaster.</summary>
        private readonly CancellationTokenSource _cancellationTokenSource = cancellationTokenSource;

        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken)
        {
            _cancellationTokenSource.Cancel();
            throw new OperationCanceledException(_cancellationTokenSource.Token);
        }
    }

    /// <summary>Throws when writing an unserializable payload.</summary>
    private sealed class ThrowingSerializationConverter : JsonConverter<UnserializablePayload>
    {
        /// <inheritdoc/>
        public override UnserializablePayload? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            new();

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, UnserializablePayload value, JsonSerializerOptions options) =>
            throw new JsonException("The test payload cannot be written.");
    }

    /// <summary>Defines source-generated JSON metadata for payload test types.</summary>
    [JsonSourceGenerationOptions(
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
        Converters = [typeof(JsonStringEnumConverter<ReadingKind>)])]
    [JsonSerializable(typeof(ReadingV1))]
    [JsonSerializable(typeof(ReadingV2))]
    [JsonSerializable(typeof(ReadingV3))]
    [JsonSerializable(typeof(UnserializablePayload))]
    private sealed partial class PayloadJsonContext : JsonSerializerContext;

    /// <summary>Represents a payload whose converter throws during serialization.</summary>
    [JsonConverter(typeof(ThrowingSerializationConverter))]
    private sealed record UnserializablePayload
    {
        /// <summary>Gets the sample value.</summary>
        public string Value { get; } = "sample";
    }

    /// <summary>Represents the first reading schema.</summary>
    /// <param name="Id">The reading identifier.</param>
    /// <param name="Celsius">The reading value in Celsius.</param>
    private sealed record ReadingV1(string Id, decimal Celsius);

    /// <summary>Represents the second reading schema.</summary>
    /// <param name="Id">The reading identifier.</param>
    /// <param name="Value">The reading value.</param>
    /// <param name="Kind">The reading kind.</param>
    private sealed record ReadingV2(string Id, decimal Value, ReadingKind Kind);

    /// <summary>Represents the third reading schema.</summary>
    /// <param name="Id">The reading identifier.</param>
    /// <param name="Value">The reading value.</param>
    /// <param name="Kind">The reading kind.</param>
    private sealed record ReadingV3(string Id, decimal Value, string Kind);
}
