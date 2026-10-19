// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="MergeableServerPayloads"/>.</summary>
public sealed class MergeableServerPayloadsTests
{
    /// <summary>The expected SHA-256 hash length including prefix.</summary>
    private const int Sha256HashLength = 51;

    /// <summary>The malformed byte value.</summary>
    private const byte MalformedByte = 255;

    /// <summary>The supported schema.</summary>
    private const int SupportedSchema = 1;

    /// <summary>The unsupported schema.</summary>
    private const int UnsupportedSchema = 2;

    /// <summary>The CRDT state payload type.</summary>
    private const byte StatePayloadType = 1;

    /// <summary>The CRDT input payload type.</summary>
    private const byte InputPayloadType = 2;

    /// <summary>The expected overflowing component count.</summary>
    private const int OverflowingComponentCount = 2;

    /// <summary>The bits per encoded byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The tiny encoded byte bound.</summary>
    private const int TinyEncodedBytes = 1;

    /// <summary>The hash prefix.</summary>
    private const string Sha256Prefix = "sha256-";

    /// <summary>The client id.</summary>
    private const string Client = "client";

    /// <summary>The first sorted client id.</summary>
    private const string ClientA = "client-a";

    /// <summary>The second sorted client id.</summary>
    private const string ClientB = "client-b";

    /// <summary>The mismatched contract reason.</summary>
    private const string ContractMismatchReason = "crdt-contract-mismatch";

    /// <summary>The hash mismatch reason.</summary>
    private const string HashMismatchReason = "crdt-payload-hash-mismatch";

    /// <summary>The invalid state reason.</summary>
    private const string InvalidStateReason = "crdt-invalid-state";

    /// <summary>The invalid mutation reason.</summary>
    private const string InvalidMutationReason = "crdt-invalid-mutation";

    /// <summary>The state kind mismatch reason.</summary>
    private const string StateKindMismatchReason = "crdt-state-kind-mismatch";

    /// <summary>Verifies state payloads use the runtime wire content type and hash format.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateStateUsesBinaryContentTypeAndSha256Prefix()
    {
        var envelope = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter));

        await Assert.That(envelope.ContractId).IsEqualTo(MergeableContracts.StateContractId);
        await Assert.That(envelope.ContentType).IsEqualTo(MergeableServerPayloads.ContentType);
        await Assert.That(envelope.PayloadHash).StartsWith(Sha256Prefix);
        await Assert.That(envelope.PayloadHash).Length().IsEqualTo(Sha256HashLength);
    }

    /// <summary>Verifies state decoding rejects contract metadata mismatches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsContractMismatch()
    {
        var envelope = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter)) with { ContractId = MergeableContracts.InputContractId };

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(ContractMismatchReason);
    }

    /// <summary>Verifies input decoding rejects schema mismatches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeInputRejectsSchemaMismatch()
    {
        var envelope = MergeableServerPayloads.CreateInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, SupportedSchema))) with { SchemaVersion = UnsupportedSchema };

        var result = MergeableServerPayloads.TryDecodeInput(envelope, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(ContractMismatchReason);
    }

    /// <summary>Verifies input decoding rejects content-type mismatches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeInputRejectsContentTypeMismatch()
    {
        var envelope = MergeableServerPayloads.CreateInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, SupportedSchema))) with { ContentType = "application/octet-stream" };

        var result = MergeableServerPayloads.TryDecodeInput(envelope, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(ContractMismatchReason);
    }

    /// <summary>Verifies hash length mismatches are rejected before fixed comparison succeeds.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsShortHash()
    {
        var envelope = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter)) with { PayloadHash = Sha256Prefix };

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(HashMismatchReason);
    }

    /// <summary>Verifies hash value mismatches are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeInputRejectsHashValueMismatch()
    {
        var envelope = MergeableServerPayloads.CreateInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, SupportedSchema))) with
        {
            PayloadHash = MergeableServerPayloads.ComputePayloadHash([MalformedByte]),
        };

        var result = MergeableServerPayloads.TryDecodeInput(envelope, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(HashMismatchReason);
    }

    /// <summary>Verifies encoded-size bounds are enforced before decoding.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsEncodedPayloadOverBound()
    {
        var envelope = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter));
        var bounds = MergeableBounds.Default with { MaximumEncodedBytes = TinyEncodedBytes };

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, bounds, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(InvalidStateReason);
    }

    /// <summary>Verifies malformed state bytes return the stable invalid-state reason.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsMalformedBytes()
    {
        var payload = new[] { MalformedByte };
        var envelope = StateEnvelope(payload, MergeableServerPayloads.ComputePayloadHash(payload));

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(InvalidStateReason);
    }

    /// <summary>Verifies state decoding rejects hash-valid binary whose aggregate value overflows.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsAggregateOverflow()
    {
        var payload = OverflowingIncrementOnlyCounterStatePayload(isInput: false);
        var envelope = StateEnvelope(payload, MergeableServerPayloads.ComputePayloadHash(payload));

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(InvalidStateReason);
    }

    /// <summary>Verifies malformed input bytes return the stable invalid-mutation reason.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeInputRejectsMalformedBytes()
    {
        var payload = new[] { MalformedByte };
        var envelope = InputEnvelope(payload, MergeableServerPayloads.ComputePayloadHash(payload));

        var result = MergeableServerPayloads.TryDecodeInput(envelope, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(InvalidMutationReason);
    }

    /// <summary>Verifies input decoding rejects hash-valid authoritative state bytes whose aggregate value overflows.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeInputRejectsAuthoritativeStateAggregateOverflow()
    {
        var payload = OverflowingIncrementOnlyCounterStatePayload(isInput: true);
        var envelope = InputEnvelope(payload, MergeableServerPayloads.ComputePayloadHash(payload));

        var result = MergeableServerPayloads.TryDecodeInput(envelope, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(InvalidMutationReason);
    }

    /// <summary>Verifies decoded state kind mismatches are rejected after payload validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryDecodeStateRejectsDecodedKindMismatch()
    {
        var envelope = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.Counter));

        var result = MergeableServerPayloads.TryDecodeState(envelope, MergeableKind.IncrementOnlyCounter, MergeableBounds.Default, out _, out var reason);

        await Assert.That(result).IsFalse();
        await Assert.That(reason).IsEqualTo(StateKindMismatchReason);
    }

    /// <summary>Creates a state envelope.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="hash">The payload hash.</param>
    /// <returns>The envelope.</returns>
    private static PayloadEnvelope StateEnvelope(byte[] payload, string hash) =>
        new(MergeableContracts.StateContractId, SupportedSchema, MergeableServerPayloads.ContentType, payload, hash);

    /// <summary>Creates an input envelope.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="hash">The payload hash.</param>
    /// <returns>The envelope.</returns>
    private static PayloadEnvelope InputEnvelope(byte[] payload, string hash) =>
        new(MergeableContracts.InputContractId, SupportedSchema, MergeableServerPayloads.ContentType, payload, hash);

    /// <summary>Creates hash-valid binary CRDT bytes whose G-counter aggregate overflows during validation.</summary>
    /// <param name="isInput">Whether to wrap the state in an authoritative input payload.</param>
    /// <returns>The encoded payload bytes.</returns>
    private static byte[] OverflowingIncrementOnlyCounterStatePayload(bool isInput)
    {
        List<byte> payload = [];
        WriteHeader(payload, isInput ? InputPayloadType : StatePayloadType);
        if (isInput)
        {
            payload.Add((byte)MergeableInputKind.AuthoritativeState);
        }

        payload.Add((byte)MergeableKind.IncrementOnlyCounter);
        WriteComponents(payload);
        WriteInt32(payload, 0);
        WriteInt32(payload, 0);
        WriteInt32(payload, 0);
        WriteInt32(payload, 0);
        WriteInt32(payload, 0);
        payload.Add(0);
        return [.. payload];
    }

    /// <summary>Writes the CRDT binary header.</summary>
    /// <param name="payload">The target payload.</param>
    /// <param name="payloadType">The CRDT payload type.</param>
    private static void WriteHeader(List<byte> payload, byte payloadType)
    {
        payload.Add((byte)'R');
        payload.Add((byte)'C');
        payload.Add((byte)'D');
        payload.Add((byte)'T');
        payload.Add(1);
        payload.Add(payloadType);
    }

    /// <summary>Writes canonical overflowing G-counter components.</summary>
    /// <param name="payload">The target payload.</param>
    private static void WriteComponents(List<byte> payload)
    {
        WriteInt32(payload, OverflowingComponentCount);
        WriteString(payload, ClientA);
        WriteInt64(payload, 1);
        WriteString(payload, ClientB);
        WriteInt64(payload, long.MaxValue);
    }

    /// <summary>Writes a CRDT binary string.</summary>
    /// <param name="payload">The target payload.</param>
    /// <param name="value">The value.</param>
    private static void WriteString(List<byte> payload, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInt32(payload, bytes.Length);
        payload.AddRange(bytes);
    }

    /// <summary>Writes a big-endian 32-bit integer.</summary>
    /// <param name="payload">The target payload.</param>
    /// <param name="value">The value.</param>
    private static void WriteInt32(List<byte> payload, int value)
    {
        for (var shift = 24; shift >= 0; shift -= BitsPerByte)
        {
            payload.Add((byte)((value >> shift) & byte.MaxValue));
        }
    }

    /// <summary>Writes a big-endian 64-bit integer.</summary>
    /// <param name="payload">The target payload.</param>
    /// <param name="value">The value.</param>
    private static void WriteInt64(List<byte> payload, long value)
    {
        for (var shift = 56; shift >= 0; shift -= BitsPerByte)
        {
            payload.Add((byte)((value >> shift) & byte.MaxValue));
        }
    }
}
