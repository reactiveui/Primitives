// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalPayloadQuarantineEvidenceFactory"/>.</summary>
public sealed class LocalPayloadQuarantineEvidenceFactoryTests
{
    /// <summary>The schema version used by evidence tests.</summary>
    private const int SchemaVersion = 2;

    /// <summary>The contract identifier used by evidence tests.</summary>
    private const string ContractId = "reading";

    /// <summary>The content type used by evidence tests.</summary>
    private const string ContentType = "application/test";

    /// <summary>The full payload length used by evidence tests.</summary>
    private const int FullPayloadLength = 6;

    /// <summary>The short payload length used by invalid evidence tests.</summary>
    private const int ShortPayloadLength = 2;

    /// <summary>The default maximum evidence byte count.</summary>
    private const int DefaultMaximumEvidenceBytes = 4096;

    /// <summary>Verifies null envelopes produce empty evidence.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEnvelopeReturnsEmptyEvidenceForNullEnvelope()
    {
        var evidence = LocalPayloadQuarantineEvidenceFactory.FromEnvelope(null, maximumEvidenceBytes: 10);

        await Assert.That(LocalPayloadQuarantineEvidenceFactory.DefaultMaximumEvidenceBytes).IsEqualTo(DefaultMaximumEvidenceBytes);
        await Assert.That(evidence.ContractId).IsNull();
        await Assert.That(evidence.SchemaVersion).IsNull();
        await Assert.That(evidence.ContentType).IsNull();
        await Assert.That(evidence.PayloadLength).IsEqualTo(0);
        await Assert.That(evidence.PayloadHash).IsNull();
        await Assert.That(evidence.PayloadPrefix.Length).IsEqualTo(0);
    }

    /// <summary>Verifies evidence keeps the original payload length while bounding retained payload and metadata bytes.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEnvelopeCopiesOnlyBoundedPayloadPrefixAndMetadata()
    {
        var envelope = new PayloadEnvelope(ContractId, SchemaVersion, ContentType, "abcdef"u8.ToArray(), "hash");

        var evidence = LocalPayloadQuarantineEvidenceFactory.FromEnvelope(envelope, maximumEvidenceBytes: 3);

        await Assert.That(evidence.ContractId).IsEqualTo("rea");
        await Assert.That(evidence.SchemaVersion).IsEqualTo(envelope.SchemaVersion);
        await Assert.That(evidence.ContentType).IsEqualTo("app");
        await Assert.That(evidence.PayloadLength).IsEqualTo(FullPayloadLength);
        await Assert.That(evidence.PayloadHash).IsEqualTo("has");
        await Assert.That(evidence.PayloadPrefix.ToArray().SequenceEqual("abc"u8.ToArray())).IsTrue();
    }

    /// <summary>Verifies evidence rejects impossible negative payload lengths.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceRejectsNegativePayloadLength()
    {
        var evidence = new LocalPayloadQuarantineEvidence(ContractId, SchemaVersion, ContentType, -1, "hash", Array.Empty<byte>());

        Action action = () => LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 3);

        await Assert.That(action).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies evidence rejects prefixes longer than the original payload length.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceRejectsPrefixLongerThanPayload()
    {
        var evidence = new LocalPayloadQuarantineEvidence(ContractId, SchemaVersion, ContentType, ShortPayloadLength, "hash", "abc"u8.ToArray());

        Action action = () => LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 3);

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies evidence preserves unreadable metadata while retaining an owned prefix copy.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidencePreservesNullMetadataAndOwnsPrefix()
    {
        var prefix = "abc"u8.ToArray();
        var evidence = new LocalPayloadQuarantineEvidence(null, null, null, FullPayloadLength, null, prefix);

        prefix[0] = (byte)'z';
        var bounded = LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 3);
        prefix[1] = (byte)'z';

        await Assert.That(bounded.ContractId).IsNull();
        await Assert.That(bounded.SchemaVersion).IsNull();
        await Assert.That(bounded.ContentType).IsNull();
        await Assert.That(bounded.PayloadHash).IsNull();
        await Assert.That(bounded.PayloadLength).IsEqualTo(FullPayloadLength);
        await Assert.That(bounded.PayloadPrefix.ToArray().SequenceEqual("abc"u8.ToArray())).IsTrue();
    }

    /// <summary>Verifies evidence rejects malformed Unicode metadata.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceRejectsMalformedMetadata()
    {
        var evidence = new LocalPayloadQuarantineEvidence("bad\uD800", SchemaVersion, ContentType, FullPayloadLength, "hash", Array.Empty<byte>());

        Action action = () => LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 3);

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies evidence keeps valid metadata that is already within the byte limit.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceKeepsUntrimmedMetadataWithinLimit()
    {
        var evidence = new LocalPayloadQuarantineEvidence("id", SchemaVersion, "ct", FullPayloadLength, "h", "abcd"u8.ToArray());

        var bounded = LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 4);

        await Assert.That(bounded.ContractId).IsEqualTo("id");
        await Assert.That(bounded.SchemaVersion).IsEqualTo(SchemaVersion);
        await Assert.That(bounded.ContentType).IsEqualTo("ct");
        await Assert.That(bounded.PayloadHash).IsEqualTo("h");
        await Assert.That(bounded.PayloadLength).IsEqualTo(FullPayloadLength);
        await Assert.That(bounded.PayloadPrefix.ToArray().SequenceEqual("abcd"u8.ToArray())).IsTrue();
    }

    /// <summary>Verifies evidence trims metadata on two-byte UTF-8 scalar boundaries.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceTrimsMetadataAtTwoByteScalarBoundary()
    {
        var evidence = new LocalPayloadQuarantineEvidence("éx", SchemaVersion, ContentType, FullPayloadLength, "hash", Array.Empty<byte>());

        var bounded = LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 2);

        await Assert.That(bounded.ContractId).IsEqualTo("é");
        await Assert.That(bounded.SchemaVersion).IsEqualTo(SchemaVersion);
        await Assert.That(bounded.ContentType).IsEqualTo("ap");
        await Assert.That(bounded.PayloadHash).IsEqualTo("ha");
    }

    /// <summary>Verifies evidence trims metadata on three-byte UTF-8 scalar boundaries.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceTrimsMetadataAtThreeByteScalarBoundary()
    {
        var evidence = new LocalPayloadQuarantineEvidence("€x", SchemaVersion, ContentType, FullPayloadLength, "hash", Array.Empty<byte>());

        var bounded = LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 3);

        await Assert.That(bounded.ContractId).IsEqualTo("€");
        await Assert.That(bounded.SchemaVersion).IsEqualTo(SchemaVersion);
        await Assert.That(bounded.ContentType).IsEqualTo("app");
        await Assert.That(bounded.PayloadHash).IsEqualTo("has");
    }

    /// <summary>Verifies evidence trims metadata on four-byte surrogate-pair UTF-8 scalar boundaries.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task FromEvidenceTrimsMetadataAtSurrogatePairBoundary()
    {
        var evidence = new LocalPayloadQuarantineEvidence("😀x", SchemaVersion, ContentType, FullPayloadLength, "hash", Array.Empty<byte>());

        var bounded = LocalPayloadQuarantineEvidenceFactory.FromEvidence(evidence, maximumEvidenceBytes: 4);

        await Assert.That(bounded.ContractId).IsEqualTo("😀");
        await Assert.That(bounded.SchemaVersion).IsEqualTo(SchemaVersion);
        await Assert.That(bounded.ContentType).IsEqualTo("appl");
        await Assert.That(bounded.PayloadHash).IsEqualTo("hash");
    }

    /// <summary>Verifies quarantine records and results expose persisted marker values.</summary>
    /// <returns>The asynchronous assertions.</returns>
    [Test]
    public async Task QuarantineResultExposesRecordValues()
    {
        var quarantineId = Guid.NewGuid();
        var streamId = new StreamId("sensor/temperature");
        var subscriptionId = new SubscriptionId(Guid.NewGuid());
        var operationId = new OperationId(Guid.NewGuid());
        var eventId = Guid.NewGuid();
        var evidence = new LocalPayloadQuarantineEvidence(ContractId, SchemaVersion, ContentType, FullPayloadLength, "hash", "abc"u8.ToArray());
        var observedAtUtc = DateTimeOffset.UnixEpoch;
        var record = new LocalPayloadQuarantineRecord(
            quarantineId,
            streamId,
            subscriptionId,
            operationId,
            eventId,
            LocalPayloadQuarantineSource.OutboxOperation,
            LocalPayloadQuarantineReason.PayloadHashMismatch,
            "PayloadHashMismatch",
            "cursor-1",
            evidence,
            observedAtUtc);

        var result = new LocalPayloadQuarantineResult(record, Created: true);

        await Assert.That(result.Created).IsTrue();
        await Assert.That(result.Record).IsSameReferenceAs(record);
        await Assert.That(record.QuarantineId).IsEqualTo(quarantineId);
        await Assert.That(record.StreamId).IsEqualTo(streamId);
        await Assert.That(record.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(record.OperationId).IsEqualTo(operationId);
        await Assert.That(record.EventId).IsEqualTo(eventId);
        await Assert.That(record.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
        await Assert.That(record.Reason).IsEqualTo(LocalPayloadQuarantineReason.PayloadHashMismatch);
        await Assert.That(record.ReasonCode).IsEqualTo("PayloadHashMismatch");
        await Assert.That(record.Cursor).IsEqualTo("cursor-1");
        await Assert.That(record.Evidence).IsEqualTo(evidence);
        await Assert.That(record.ObservedAtUtc).IsEqualTo(observedAtUtc);
    }
}
