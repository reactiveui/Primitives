// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests <see cref="JsonPayloadSerializer"/> upcast chains against retained protocol-v1 payload fixtures.</summary>
public sealed partial class JsonPayloadSerializerTests
{
    /// <summary>The retained version-one reading payload.</summary>
    private const string GoldenReadingV1File = "reading-v1.json";

    /// <summary>The retained version-two reading payload.</summary>
    private const string GoldenReadingV2File = "reading-v2.json";

    /// <summary>The retained version-three reading payload.</summary>
    private const string GoldenReadingV3File = "reading-v3.json";

    /// <summary>The frozen payload hash of the retained version-one reading.</summary>
    private const string GoldenReadingV1Hash = "sha256-9lRhz7UlmCkdkuUsP1wv/Tg4ERgKO2KaUK3OiYMZSzk=";

    /// <summary>The frozen payload hash of the retained version-two reading.</summary>
    private const string GoldenReadingV2Hash = "sha256-0bi5I5WsFcFG/RhwpC7ekm8BS8VN9ac1usn/r4wruRc=";

    /// <summary>The offset from the end of the v1 payload of the digit the corruption test changes.</summary>
    private const int GoldenCorruptedByteFromEnd = 2;

    /// <summary>The number of upcasters in the full golden chain.</summary>
    private const int GoldenChainLength = 2;

    /// <summary>Verifies the retained payload hashes are still computed the same way.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenPayloadHashesMatchCurrentHashFormat()
    {
        await Assert.That(JsonPayloadSerializer.ComputePayloadHash(ReadGoldenPayload(GoldenReadingV1File))).IsEqualTo(GoldenReadingV1Hash);
        await Assert.That(JsonPayloadSerializer.ComputePayloadHash(ReadGoldenPayload(GoldenReadingV2File))).IsEqualTo(GoldenReadingV2Hash);
    }

    /// <summary>Verifies the registry resolves the contiguous v1 to v3 chain in order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenUpcastChainIsContiguous()
    {
        var chain = CreateGoldenRegistry().GetUpcastChain(ReadingContract, ReadingV1Version, ReadingV3Version);

        await Assert.That(chain.Count).IsEqualTo(GoldenChainLength);
        await Assert.That(chain[0].FromVersion).IsEqualTo(ReadingV1Version);
        await Assert.That(chain[0].ToVersion).IsEqualTo(ReadingV2Version);
        await Assert.That(chain[1].FromVersion).IsEqualTo(ReadingV2Version);
        await Assert.That(chain[1].ToVersion).IsEqualTo(ReadingV3Version);
    }

    /// <summary>Verifies a retained v1 payload upcasts through v2 to the current v3 type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenV1PayloadUpcastsToCurrentType()
    {
        JsonPayloadSerializer serializer = new(CreateGoldenRegistry());
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV1Version, JsonContentType, ReadGoldenPayload(GoldenReadingV1File), GoldenReadingV1Hash);

        var result = await serializer.DeserializeAsync(envelope, typeof(ReadingV3));

        await Assert.That(result).IsEqualTo(new ReadingV3(ReadingId, ReadingValue, nameof(ReadingKind.Temperature)));
    }

    /// <summary>Verifies a retained v2 payload upcasts through the last step to the current v3 type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenV2PayloadUpcastsToCurrentType()
    {
        JsonPayloadSerializer serializer = new(CreateGoldenRegistry());
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV2Version, JsonContentType, ReadGoldenPayload(GoldenReadingV2File), GoldenReadingV2Hash);

        var result = await serializer.DeserializeAsync(envelope, typeof(ReadingV3));

        await Assert.That(result).IsEqualTo(new ReadingV3(ReadingId, ReadingValue, nameof(ReadingKind.Temperature)));
    }

    /// <summary>Verifies the current serializer writes each retained payload version byte for byte.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenPayloadsMatchCurrentSerializer()
    {
        JsonPayloadSerializer serializer = new(CreateGoldenRegistry());

        var v1 = await serializer.SerializeAsync(ReadingContract, ReadingV1Version, new ReadingV1(ReadingId, ReadingValue));
        var v2 = await serializer.SerializeAsync(ReadingContract, ReadingV2Version, new ReadingV2(ReadingId, ReadingValue, ReadingKind.Temperature));
        var v3 = await serializer.SerializeAsync(ReadingContract, ReadingV3Version, new ReadingV3(ReadingId, ReadingValue, nameof(ReadingKind.Temperature)));

        await Assert.That(v1.Payload.Span.SequenceEqual(ReadGoldenPayload(GoldenReadingV1File))).IsTrue();
        await Assert.That(v1.PayloadHash).IsEqualTo(GoldenReadingV1Hash);
        await Assert.That(v2.Payload.Span.SequenceEqual(ReadGoldenPayload(GoldenReadingV2File))).IsTrue();
        await Assert.That(v2.PayloadHash).IsEqualTo(GoldenReadingV2Hash);
        await Assert.That(v3.Payload.Span.SequenceEqual(ReadGoldenPayload(GoldenReadingV3File))).IsTrue();
    }

    /// <summary>Verifies a corrupted retained v1 payload fails closed before any upcaster runs.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GoldenV1PayloadWithCorruptedBytesIsRejected()
    {
        JsonPayloadSerializer serializer = new(CreateGoldenRegistry());
        var payload = ReadGoldenPayload(GoldenReadingV1File);
        payload[^GoldenCorruptedByteFromEnd] = (byte)'9';
        var envelope = new PayloadEnvelope(ReadingContract, ReadingV1Version, JsonContentType, payload, GoldenReadingV1Hash);

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(ReadingV3)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadHashMismatch);
    }

    /// <summary>Creates the registry holding every retained reading version and the full upcast chain.</summary>
    /// <returns>The registry.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SchemaRegistry CreateGoldenRegistry() =>
        new SchemaRegistry()
            .Register(ReadingContract, ReadingV1Version, PayloadJsonContext.Default.ReadingV1)
            .Register(ReadingContract, ReadingV2Version, PayloadJsonContext.Default.ReadingV2)
            .Register(ReadingContract, ReadingV3Version, PayloadJsonContext.Default.ReadingV3)
            .RegisterUpcaster(new ReadingV1ToV2Upcaster())
            .RegisterUpcaster(new ReadingV2ToV3Upcaster());

    /// <summary>Reads a retained payload with line endings normalized and the final line break removed.</summary>
    /// <param name="name">The fixture name.</param>
    /// <returns>The canonical payload bytes.</returns>
    private static byte[] ReadGoldenPayload(string name)
    {
        var text = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "GoldenFixtures", "protocol-v1", name), Encoding.UTF8);
        return Encoding.UTF8.GetBytes(text.Replace("\r\n", "\n", StringComparison.Ordinal).TrimEnd('\n'));
    }
}
