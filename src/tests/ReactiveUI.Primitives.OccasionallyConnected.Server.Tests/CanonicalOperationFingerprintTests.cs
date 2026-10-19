// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="CanonicalOperationFingerprint"/>.</summary>
public sealed class CanonicalOperationFingerprintTests
{
    /// <summary>The authenticated tenant used by most canonical vectors.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client used by most canonical vectors.</summary>
    private const string Client = "client";

    /// <summary>The stream identifier used by most canonical vectors.</summary>
    private const string Stream = "stream-a";

    /// <summary>The base stream version used by most canonical vectors.</summary>
    private const string BaseVersion = "v1";

    /// <summary>The payload contract identifier used by most canonical vectors.</summary>
    private const string Contract = "contract";

    /// <summary>The payload content type used by most canonical vectors.</summary>
    private const string ContentType = "application/test";

    /// <summary>The claimed payload hash used by most canonical vectors.</summary>
    private const string PayloadHash = "hash";

    /// <summary>The default metadata key used by most canonical vectors.</summary>
    private const string MetadataKey = "key";

    /// <summary>The default metadata value used by most canonical vectors.</summary>
    private const string MetadataValue = "value";

    /// <summary>The canonical operation domain separator.</summary>
    private const string DomainVersion = "ReactiveUI.Primitives.OccasionallyConnected.CanonicalOperationFingerprint/v1";

    /// <summary>The deterministic operation identifier used by most canonical vectors.</summary>
    private const string OperationGuid = "11111111-2222-3333-4444-555555555555";

    /// <summary>An alternate operation identifier used to prove scope coverage.</summary>
    private const string AlternateOperationGuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

    /// <summary>The expected byte count for the baseline independent vector.</summary>
    private const int ExactBytes = 234;

    /// <summary>The roomy byte limit used by most tests.</summary>
    private const int MaximumBytes = 4096;

    /// <summary>The expected SHA-256 length.</summary>
    private const int FingerprintLength = 32;

    /// <summary>The baseline schema version.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The alternate schema version.</summary>
    private const int AlternateSchemaVersion = 2;

    /// <summary>The baseline client sequence.</summary>
    private const long ClientSequence = 1;

    /// <summary>The alternate priority used to prove policy coverage.</summary>
    private const int AlternatePriority = 1;

    /// <summary>The first baseline payload byte.</summary>
    private const byte PayloadFirstByte = 1;

    /// <summary>The second baseline payload byte.</summary>
    private const byte PayloadSecondByte = 2;

    /// <summary>The third baseline payload byte.</summary>
    private const byte PayloadThirdByte = 3;

    /// <summary>The alternate third payload byte.</summary>
    private const byte AlternatePayloadThirdByte = 4;

    /// <summary>The invalid high-surrogate character used for strict UTF-8 checks.</summary>
    private const char MalformedHighSurrogate = '\ud800';

    /// <summary>The invalid low-surrogate character used for strict UTF-8 checks.</summary>
    private const char MalformedLowSurrogate = '\udc00';

    /// <summary>The number of characters in an invalid UTF-16 sample.</summary>
    private const int InvalidTextLength = 1;

    /// <summary>The large text length that forces several encoder chunks.</summary>
    private const int LongTextLength = 300;

    /// <summary>The ASCII prefix length that leaves one byte before a four-byte scalar.</summary>
    private const int EmojiBoundaryAsciiLength = 255;

    /// <summary>The length of the large payload budget test.</summary>
    private const int LargePayloadLength = 512;

    /// <summary>The byte modulus used by the large payload fixture.</summary>
    private const int ByteModulo = 251;

    /// <summary>The valid supplementary code point used at an encoder buffer boundary.</summary>
    private const int ValidSupplementaryCodePoint = 128_512;

    /// <summary>The encoded length of a 64-bit scalar.</summary>
    private const int Int64Length = 8;

    /// <summary>The encoded length of a 32-bit scalar.</summary>
    private const int Int32Length = 4;

    /// <summary>The independent baseline vector computed from the canonical byte layout.</summary>
    private const string ExpectedVector = "7B7350B8CDEA134F8050935D43EE8DABB3DDC753E4EFEFD74AF52BAD8CD4C853";

    /// <summary>The strict UTF-8 encoder used by the independent test encoder.</summary>
    private static readonly Encoding StrictEncoding = new UTF8Encoding(false, true);

    /// <summary>The baseline payload bytes.</summary>
    private static readonly byte[] PayloadBytes = [PayloadFirstByte, PayloadSecondByte, PayloadThirdByte];

    /// <summary>Verifies ordinal metadata order does not affect the fingerprint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenMetadataInsertionOrderChanges_ThenFingerprintIsStable()
    {
        var first = CreateOperation(new Dictionary<string, string> { ["alpha"] = "one", ["beta"] = "two" });
        var second = CreateOperation(new Dictionary<string, string> { ["beta"] = "two", ["alpha"] = "one" });

        await Assert.That(Hex(Compute(first))).IsEqualTo(Hex(Compute(second)));
        await Assert.That(Hex(Compute(first))).IsEqualTo(Hex(IndependentFingerprint(first)));
    }

    /// <summary>Verifies the diagnostic timestamp is excluded from the fingerprint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenTimestampChanges_ThenFingerprintIsStable()
    {
        var operation = CreateOperation();
        var changed = operation with { TimestampUtc = operation.TimestampUtc.AddTicks(1) };

        await Assert.That(Hex(Compute(operation))).IsEqualTo(Hex(Compute(changed)));
    }

    /// <summary>Verifies each persisted operation intent field changes the fingerprint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenPersistedIntentChanges_ThenFingerprintChanges()
    {
        var operation = CreateOperation();
        var variants = new[]
        {
            operation with { OperationId = new(Guid.Parse(AlternateOperationGuid)) },
            operation with { StreamId = new("stream-b") },
            operation with { ClientSequence = operation.ClientSequence + ClientSequence },
            operation with { BaseVersion = null },
            operation with { Type = SyncOperationType.Update },
            operation with { Payload = CreatePayload(contractId: "contract-b") },
            operation with { Payload = CreatePayload(schemaVersion: AlternateSchemaVersion) },
            operation with { Payload = CreatePayload(contentType: "application/other") },
            operation with { Payload = CreatePayload(payloadHash: "other-hash") },
            operation with { Payload = CreatePayload(payload: [PayloadFirstByte, PayloadSecondByte, AlternatePayloadThirdByte]) },
            operation with { Policy = operation.Policy with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce } },
            operation with { Policy = operation.Policy with { Durability = OperationDurability.Volatile } },
            operation with { Policy = operation.Policy with { Priority = AlternatePriority } },
            operation with { Policy = operation.Policy with { ConflictPolicy = ConflictPolicy.LastWriterWins } },
            operation with { Metadata = new Dictionary<string, string> { [MetadataKey] = "other" } },
        };
        var original = Hex(Compute(operation));

        foreach (var variant in variants)
        {
            await Assert.That(Hex(Compute(variant))).IsNotEqualTo(original);
        }
    }

    /// <summary>Verifies actual payload bytes are part of the fingerprint even when the claimed hash is unchanged.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenActualPayloadBytesChangeButClaimedHashDoesNot_ThenFingerprintChanges()
    {
        var operation = CreateOperation();
        var changed = operation with
        {
            Payload = CreatePayload(
                payload: [PayloadFirstByte, PayloadSecondByte, AlternatePayloadThirdByte]),
        };

        await Assert.That(Hex(Compute(changed))).IsNotEqualTo(Hex(Compute(operation)));
    }

    /// <summary>Verifies authenticated tenant and client values scope the fingerprint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenAuthenticatedScopeChanges_ThenFingerprintChanges()
    {
        var operation = CreateOperation();
        var fingerprint = Hex(Compute(operation));

        await Assert.That(Hex(CanonicalOperationFingerprint.Compute("tenant-b", Client, operation, MaximumBytes))).IsNotEqualTo(fingerprint);
        await Assert.That(Hex(CanonicalOperationFingerprint.Compute(Tenant, "client-b", operation, MaximumBytes))).IsNotEqualTo(fingerprint);
    }

    /// <summary>Verifies length prefixes prevent authenticated scope concatenation collisions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenAuthenticatedScopeConcatenatesToSameText_ThenFingerprintChanges()
    {
        var operation = CreateOperation();
        var first = CanonicalOperationFingerprint.Compute("ab", "c", operation, MaximumBytes);
        var second = CanonicalOperationFingerprint.Compute("a", "bc", operation, MaximumBytes);

        await Assert.That(Hex(first)).IsNotEqualTo(Hex(second));
    }

    /// <summary>Verifies null and empty base versions have separate encodings.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenBaseVersionIsNullOrEmpty_ThenFingerprintsDoNotCollide()
    {
        var operation = CreateOperation();

        await Assert.That(Hex(Compute(operation with { BaseVersion = null }))).IsNotEqualTo(Hex(Compute(operation with { BaseVersion = string.Empty })));
    }

    /// <summary>Verifies the exact byte bound succeeds and the preceding bound fails.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenEncodingBudgetIsExact_ThenItIsAcceptedAndOneByteLessIsRejected()
    {
        var operation = CreateOperation();

        await Assert.That(IndependentBytes(Tenant, Client, operation).Length).IsEqualTo(ExactBytes);
        await Assert.That(Hex(Compute(operation, ExactBytes))).IsEqualTo(ExpectedVector);
        await Assert.That(() => Compute(operation, ExactBytes - 1)).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => Compute(CreateOperation(), 0)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies a large payload is bounded per call using the exact encoded byte count.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenLargePayloadFitsExactBudget_ThenItIsAcceptedAndOneByteLessIsRejected()
    {
        var operation = CreateOperation(payload: CreateLargePayload());
        var exactBytes = IndependentBytes(Tenant, Client, operation).Length;

        await Assert.That(Hex(Compute(operation, exactBytes))).IsEqualTo(Hex(IndependentFingerprint(operation)));
        await Assert.That(() => Compute(operation, exactBytes - 1)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies strict UTF-8 validation rejects malformed identity and metadata text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenEncodingContainsMalformedUtf16_ThenItIsRejected()
    {
        var high = new string(MalformedHighSurrogate, InvalidTextLength);
        var low = new string(MalformedLowSurrogate, InvalidTextLength);
        var metadataOperation = CreateOperation(new Dictionary<string, string> { [MetadataKey] = high });

        await Assert.That(() => Compute(metadataOperation)).ThrowsExactly<EncoderFallbackException>();
        await Assert.That(() => CanonicalOperationFingerprint.Compute(high, Client, CreateOperation(), MaximumBytes))
            .ThrowsExactly<EncoderFallbackException>();
        await Assert.That(() => CanonicalOperationFingerprint.Compute(Tenant, low, CreateOperation(), MaximumBytes))
            .ThrowsExactly<EncoderFallbackException>();
    }

    /// <summary>Verifies blank authenticated identity values are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenAuthenticatedScopeIsBlank_ThenItIsRejected()
    {
        await Assert.That(static () => CanonicalOperationFingerprint.Compute(" ", Client, CreateOperation(), MaximumBytes))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(static () => CanonicalOperationFingerprint.Compute(Tenant, " ", CreateOperation(), MaximumBytes))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies streaming text chunks, empty text, and scalar boundaries match the independent encoder.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenTextRequiresChunking_ThenFingerprintMatchesIndependentEncoding()
    {
        var emojiBoundary = new string('a', EmojiBoundaryAsciiLength) + char.ConvertFromUtf32(ValidSupplementaryCodePoint);
        var operation = CreateOperation(new Dictionary<string, string> { ["empty"] = string.Empty, ["long"] = new('x', LongTextLength), ["emoji"] = emojiBoundary }) with
        {
            BaseVersion = string.Empty,
            Payload = CreatePayload(contentType: emojiBoundary),
        };

        var actual = CanonicalOperationFingerprint.Compute(new('t', LongTextLength), Client, operation, MaximumBytes);
        var expected = IndependentFingerprint(new('t', LongTextLength), Client, operation);

        await Assert.That(Hex(actual)).IsEqualTo(Hex(expected));
    }

    /// <summary>Verifies a fixed independent encoding vector remains stable.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenOperationMatchesPublishedEncodingVector_ThenFingerprintMatches()
    {
        var operation = CreateOperation();

        await Assert.That(Hex(IndependentFingerprint(operation))).IsEqualTo(ExpectedVector);
        await Assert.That(Hex(Compute(operation))).IsEqualTo(ExpectedVector);
        await Assert.That(Compute(operation).Length).IsEqualTo(FingerprintLength);
    }

    /// <summary>Verifies nonzero high scalar bytes survive canonical serialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenSequenceExceeds32Bits_ThenAllScalarBytesMatchIndependentEncoding()
    {
        const long sequence = 0x0123_4567_89AB_CDEF;
        const int schema = 0x0123_4567;
        var operation = CreateOperation() with
        {
            ClientSequence = sequence,
            Payload = CreatePayload(schemaVersion: schema),
            Policy = OperationPolicy.Default with { Priority = OperationPolicy.MinimumPriority },
        };

        await Assert.That(Hex(Compute(operation))).IsEqualTo(Hex(IndependentFingerprint(operation)));
        await Assert.That(Hex(Compute(operation with { ClientSequence = sequence & uint.MaxValue }))).IsNotEqualTo(Hex(Compute(operation)));
    }

    /// <summary>Computes the test operation fingerprint.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="maximumEncodedBytes">The maximum canonical byte count.</param>
    /// <returns>The fingerprint.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Compute(SyncOperation operation, int maximumEncodedBytes = MaximumBytes) =>
        CanonicalOperationFingerprint.Compute(Tenant, Client, operation, maximumEncodedBytes);

    /// <summary>Creates a deterministic operation with optional metadata and payload.</summary>
    /// <param name="metadata">The optional metadata.</param>
    /// <param name="payload">The optional payload.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        IReadOnlyDictionary<string, string>? metadata = null,
        byte[]? payload = null) => new()
        {
            OperationId = new(Guid.Parse(OperationGuid)),
            StreamId = new(Stream),
            ClientSequence = ClientSequence,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            BaseVersion = BaseVersion,
            Type = SyncOperationType.Append,
            Payload = CreatePayload(payload: payload),
            Policy = OperationPolicy.Default,
            Metadata = metadata ?? new Dictionary<string, string> { [MetadataKey] = MetadataValue },
        };

    /// <summary>Creates a payload envelope with selective persisted-intent changes.</summary>
    /// <param name="contractId">The contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="contentType">The content type.</param>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="payloadHash">The claimed payload hash.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(
        string contractId = Contract,
        int schemaVersion = SchemaVersion,
        string contentType = ContentType,
        byte[]? payload = null,
        string payloadHash = PayloadHash) =>
        new(contractId, schemaVersion, contentType, payload ?? PayloadBytes, payloadHash);

    /// <summary>Creates a deterministic large payload.</summary>
    /// <returns>The payload bytes.</returns>
    private static byte[] CreateLargePayload()
    {
        var payload = new byte[LargePayloadLength];
        for (var index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(index % ByteModulo);
        }

        return payload;
    }

    /// <summary>Computes an independent fingerprint for the default authenticated scope.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The independent fingerprint.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] IndependentFingerprint(SyncOperation operation) => IndependentFingerprint(Tenant, Client, operation);

    /// <summary>Computes an independent fingerprint for the supplied authenticated scope.</summary>
    /// <param name="tenant">The authenticated tenant.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The independent fingerprint.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] IndependentFingerprint(string tenant, string client, SyncOperation operation) =>
        SHA256.HashData(IndependentBytes(tenant, client, operation));

    /// <summary>Encodes the canonical byte stream independently of the production implementation.</summary>
    /// <param name="tenant">The authenticated tenant.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The canonical bytes.</returns>
    private static byte[] IndependentBytes(string tenant, string client, SyncOperation operation)
    {
        using var stream = new MemoryStream();
        AppendBytesWithLength(stream, StrictEncoding.GetBytes(DomainVersion));
        AppendText(stream, tenant);
        AppendText(stream, client);
        AppendBytes(stream, operation.OperationId.Value.ToByteArray());
        AppendText(stream, operation.StreamId.Value);
        AppendInt64(stream, operation.ClientSequence);
        AppendOptionalText(stream, operation.BaseVersion);
        AppendInt32(stream, (int)operation.Type);
        AppendPayload(stream, operation.Payload);
        AppendInt32(stream, (int)operation.Policy.DeliveryGuarantee);
        AppendInt32(stream, (int)operation.Policy.Durability);
        AppendInt32(stream, operation.Policy.Priority);
        AppendInt32(stream, (int)operation.Policy.ConflictPolicy);
        AppendMetadata(stream, operation.Metadata);
        return stream.ToArray();
    }

    /// <summary>Appends a payload envelope to the independent stream.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="payload">The payload envelope.</param>
    private static void AppendPayload(MemoryStream stream, PayloadEnvelope payload)
    {
        AppendText(stream, payload.ContractId);
        AppendInt32(stream, payload.SchemaVersion);
        AppendText(stream, payload.ContentType);
        AppendText(stream, payload.PayloadHash);
        AppendBytesWithLength(stream, payload.Payload.ToArray());
    }

    /// <summary>Appends metadata using exact ordinal key order.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="metadata">The metadata entries.</param>
    private static void AppendMetadata(MemoryStream stream, IReadOnlyDictionary<string, string> metadata)
    {
        var entries = metadata.ToArray();
        Array.Sort(entries, static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        AppendInt32(stream, entries.Length);
        foreach (var entry in entries)
        {
            AppendText(stream, entry.Key);
            AppendText(stream, entry.Value);
        }
    }

    /// <summary>Appends optional text with an explicit null marker.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The optional text.</param>
    private static void AppendOptionalText(MemoryStream stream, string? value)
    {
        stream.WriteByte(value is null ? (byte)0 : (byte)1);
        if (value is null)
        {
            return;
        }

        AppendText(stream, value);
    }

    /// <summary>Appends length-prefixed strict UTF-8 text.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The text value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendText(MemoryStream stream, string value) => AppendBytesWithLength(stream, StrictEncoding.GetBytes(value));

    /// <summary>Appends length-prefixed bytes.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The bytes.</param>
    private static void AppendBytesWithLength(MemoryStream stream, byte[] value)
    {
        AppendInt32(stream, value.Length);
        AppendBytes(stream, value);
    }

    /// <summary>Appends bytes without a length prefix.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendBytes(MemoryStream stream, byte[] value) => stream.Write(value);

    /// <summary>Appends a 32-bit scalar in little-endian order.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The scalar value.</param>
    private static void AppendInt32(MemoryStream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[Int32Length];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        stream.Write(bytes);
    }

    /// <summary>Appends a 64-bit scalar in little-endian order.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The scalar value.</param>
    private static void AppendInt64(MemoryStream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[Int64Length];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        stream.Write(bytes);
    }

    /// <summary>Formats a fingerprint for assertions.</summary>
    /// <param name="fingerprint">The fingerprint.</param>
    /// <returns>The uppercase hexadecimal string.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string Hex(byte[] fingerprint) => Convert.ToHexString(fingerprint);
}
