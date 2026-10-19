// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests stable schema failures at the CRDT serializer boundary.</summary>
public sealed class MergeablePayloadSerializerTests
{
    /// <summary>Verifies invalid numeric state and timestamps become stable schema failures.</summary>
    /// <param name="invalidTimestamp">Whether to corrupt a register timestamp instead of a counter sum.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InvalidNumericStateReportsDeserializationFailure(bool invalidTimestamp)
    {
        const int secondCounterLastByte = 36;
        const int registerTimestampFirstByte = 32;
        var state = invalidTimestamp
            ? new MergeableState
            {
                Kind = MergeableKind.LastWriterWinsValue,
                LatestWriteStamp = new() { CommittedAtUtc = DateTimeOffset.UnixEpoch, ClientId = "client", OperationId = new(Guid.NewGuid()) },
            }
            : new MergeableState { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = new Dictionary<string, long> { ["a"] = long.MaxValue, ["b"] = 0 } };
        var bytes = MergeableCodec.EncodeState(state);
        bytes[invalidTimestamp ? registerTimestampFirstByte : secondCounterLastByte] = invalidTimestamp ? byte.MaxValue : (byte)1;
        var serializer = new MergeablePayloadSerializer();
        var envelope = new PayloadEnvelope(MergeableContracts.StateContractId, MergeableContracts.SchemaVersion, serializer.ContentType, bytes, JsonPayloadSerializer.ComputePayloadHash(bytes));
        var failure = await Assert.That(async () => await serializer.DeserializeAsync(envelope, typeof(MergeableState), CancellationToken.None))
            .ThrowsExactly<PayloadSchemaException>();
        await Assert.That(failure?.Reason).IsEqualTo(PayloadSchemaFailureReason.DeserializationFailed);
    }

    /// <summary>Verifies hash-valid malformed bytes report a quarantine-compatible schema failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MalformedBinaryReportsDeserializationSchemaFailure()
    {
        var serializer = new MergeablePayloadSerializer();
        byte[] bytes = [1];
        var envelope = new PayloadEnvelope(MergeableContracts.StateContractId, MergeableContracts.SchemaVersion, serializer.ContentType, bytes, JsonPayloadSerializer.ComputePayloadHash(bytes));
        var failure = await Assert.That(async () => await serializer.DeserializeAsync(envelope, typeof(MergeableState), CancellationToken.None))
            .ThrowsExactly<PayloadSchemaException>();
        await Assert.That(failure?.Reason).IsEqualTo(PayloadSchemaFailureReason.DeserializationFailed);
    }

    /// <summary>Verifies size rejection precedes hashing and decoding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OversizeEnvelopeReportsPayloadTooLargeBeforeHashValidation()
    {
        var serializer = new MergeablePayloadSerializer(new() { MaximumEncodedBytes = 1 });
        byte[] oversized = [1, 1];
        var envelope = new PayloadEnvelope(MergeableContracts.StateContractId, MergeableContracts.SchemaVersion, serializer.ContentType, oversized, "incorrect-hash");
        var failure = await Assert.That(async () => await serializer.DeserializeAsync(envelope, typeof(MergeableState), CancellationToken.None))
            .ThrowsExactly<PayloadSchemaException>();
        await Assert.That(failure?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }
}
