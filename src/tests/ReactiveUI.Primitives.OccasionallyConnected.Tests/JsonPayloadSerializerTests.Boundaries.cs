// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests payload size boundaries using actual encoded JSON.</summary>
public sealed partial class JsonPayloadSerializerTests
{
    /// <summary>The identifier length that crosses several JSON buffer growth boundaries.</summary>
    private const int LargeIdentifierLength = 16_384;

    /// <summary>Verifies cancellation is checked when an upcaster returns without observing the token itself.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationDuringUpcastPreventsDeserialization()
    {
        using var cancellation = new CancellationTokenSource();
        var serializer = CreateSerializer(new CancellationReturningUpcaster(cancellation));
        var envelope = CreateV1Envelope();
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => serializer.DeserializeAsync(envelope, typeof(ReadingV2), cancellation.Token).AsTask());
        await Assert.That(exception!.CancellationToken).IsEqualTo(cancellation.Token);
    }

    /// <summary>Verifies escaping and large values still fit their exact encoded byte limit.</summary>
    /// <param name="escaped">Whether to exercise JSON escaping.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LargePayloadFitsExactEncodedLimit(bool escaped)
    {
        var id = new string(escaped ? '\u0001' : 'a', LargeIdentifierLength);
        var value = new ReadingV2(id, ReadingValue, ReadingKind.Temperature);
        var expectedBytes = JsonSerializer.SerializeToUtf8Bytes(value, PayloadJsonContext.Default.ReadingV2);
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var serializer = new JsonPayloadSerializer(registry, expectedBytes.Length);
        var envelope = await serializer.SerializeAsync(ReadingContract, ReadingV2Version, value);
        await Assert.That(envelope.Payload.Span.SequenceEqual(expectedBytes)).IsTrue();
        var restored = (ReadingV2)await serializer.DeserializeAsync(envelope, typeof(ReadingV2));
        await Assert.That(restored).IsEqualTo(value);
    }

    /// <summary>Verifies missing hash metadata becomes a stable schema failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MissingHashMetadataIsRejectedAsSchemaFailure()
    {
        var serializer = CreateSerializer();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, CreateV2Payload(), string.Empty) with { PayloadHash = null! };
        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());
        await Assert.That(exception!.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadHashMismatch);
    }

    /// <summary>Verifies a well-formed hash belonging to different bytes cannot pass integrity validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HashForDifferentPayloadIsRejected()
    {
        var serializer = CreateSerializer();
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, CreateV2Payload(), JsonPayloadSerializer.ComputePayloadHash("different payload"u8));
        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() => serializer.DeserializeAsync(envelope, typeof(ReadingV2)).AsTask());
        await Assert.That(exception!.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadHashMismatch);
    }

    /// <summary>Verifies a small document fits an exact byte limit despite the JSON writer's larger scratch request.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncodedPayloadFitsItsExactByteLimit()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var maximumBytes = Encoding.UTF8.GetByteCount(SerializedReadingV2Json);
        var serializer = new JsonPayloadSerializer(registry, maximumBytes);
        var value = new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature);
        var envelope = await serializer.SerializeAsync(ReadingContract, ReadingV2Version, value);
        await Assert.That(envelope.Payload.Length).IsEqualTo(maximumBytes);
        await Assert.That(Encoding.UTF8.GetString(envelope.Payload.Span)).IsEqualTo(SerializedReadingV2Json);
    }

    /// <summary>Verifies the limit rejects the actual encoded document when it is one byte too large.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncodedPayloadOneByteBeyondLimitIsRejected()
    {
        var registry = new SchemaRegistry().Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2);
        var serializer = new JsonPayloadSerializer(registry, Encoding.UTF8.GetByteCount(SerializedReadingV2Json) - 1);
        var value = new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature);
        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() => serializer.SerializeAsync(ReadingContract, ReadingV2Version, value).AsTask());
        await Assert.That(exception!.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Returns valid converted bytes after cancellation has been requested.</summary>
    /// <param name="cancellation">The cancellation source to signal.</param>
    private sealed class CancellationReturningUpcaster(CancellationTokenSource cancellation) : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId => ReadingContract;

        /// <inheritdoc/>
        public int FromVersion => ReadingV1Version;

        /// <inheritdoc/>
        public int ToVersion => ReadingV2Version;

        /// <inheritdoc/>
        public async ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken)
        {
            await cancellation.CancelAsync();
            var payload = CreateV2Payload();
            return source with { SchemaVersion = ReadingV2Version, Payload = payload, PayloadHash = JsonPayloadSerializer.ComputePayloadHash(payload) };
        }
    }
}
