// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.InteropServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="PayloadEnvelope"/>.</summary>
public sealed class PayloadEnvelopeTests
{
    /// <summary>The contract identifier used by envelope tests.</summary>
    private const string ContractId = "payload.contract";

    /// <summary>The content type used by envelope tests.</summary>
    private const string ContentType = "application/json";

    /// <summary>The payload hash used by envelope tests.</summary>
    private const string PayloadHash = "sha256-test";

    /// <summary>The schema version used by envelope tests.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The payload mutation offset used by copy tests.</summary>
    private const int MutationOffset = 1;

    /// <summary>Verifies envelope construction stores metadata and copies incoming payload bytes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCopiesPayloadBytes()
    {
        var payload = "abc"u8.ToArray();
        PayloadEnvelope envelope = new(ContractId, SchemaVersion, ContentType, payload, PayloadHash);
        payload[MutationOffset] = (byte)'z';

        var json = Encoding.UTF8.GetString(envelope.Payload.Span);

        await Assert.That(envelope.ContractId).IsEqualTo(ContractId);
        await Assert.That(envelope.SchemaVersion).IsEqualTo(SchemaVersion);
        await Assert.That(envelope.ContentType).IsEqualTo(ContentType);
        await Assert.That(envelope.PayloadHash).IsEqualTo(PayloadHash);
        await Assert.That(envelope.PayloadLength).IsEqualTo(payload.Length);
        await Assert.That(json).IsEqualTo("abc");
    }

    /// <summary>Verifies envelope payload reads return copies.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PayloadGetterReturnsCopy()
    {
        PayloadEnvelope envelope = new(ContractId, SchemaVersion, ContentType, "abc"u8.ToArray(), PayloadHash);
        if (MemoryMarshal.TryGetArray(envelope.Payload, out var segment))
        {
            segment.Array![segment.Offset + MutationOffset] = (byte)'z';
        }

        var json = Encoding.UTF8.GetString(envelope.Payload.Span);

        await Assert.That(json).IsEqualTo("abc");
    }

    /// <summary>Verifies replacing a record payload takes ownership without changing the original envelope.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CopyTakesOwnershipOfReplacementPayload()
    {
        var original = new PayloadEnvelope(ContractId, SchemaVersion, ContentType, "abc"u8.ToArray(), PayloadHash);
        var replacement = "new-value"u8.ToArray();
        var copy = original with { Payload = replacement };
        replacement[0] = (byte)'x';
        await Assert.That(copy.PayloadLength).IsEqualTo(replacement.Length);
        await Assert.That(Encoding.UTF8.GetString(copy.Payload.Span)).IsEqualTo("new-value");
        await Assert.That(Encoding.UTF8.GetString(original.Payload.Span)).IsEqualTo("abc");
    }
}
