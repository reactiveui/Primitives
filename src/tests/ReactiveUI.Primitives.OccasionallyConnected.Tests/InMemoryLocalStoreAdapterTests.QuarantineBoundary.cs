// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Quarantine boundary tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The maximum retained quarantine evidence prefix byte count.</summary>
    private const int QuarantineBoundaryEvidenceBytes = 4096;

    /// <summary>A caller supplied value that exceeds the evidence boundary.</summary>
    private const int OversizedQuarantineBoundaryBytes = QuarantineBoundaryEvidenceBytes + 904;

    /// <summary>The UTF-8 byte count for one grinning-face Unicode scalar.</summary>
    private const int GrinningFaceUtf8Bytes = 4;

    /// <summary>The UTF-16 code unit count for one surrogate pair.</summary>
    private const int SurrogatePairCodeUnitCount = 2;

    /// <summary>Verifies caller supplied evidence is normalized before the marker is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task QuarantinePayloadAsyncBoundsSuppliedEvidenceBeforeRetainingMarker()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = CreateOversizedQuarantineBoundaryEvidence(OversizedQuarantineBoundaryBytes),
            Envelope = null,
        };

        _ = await store.QuarantinePayloadAsync(request, CancellationToken.None);
        var marker = await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException("Expected quarantine marker.");

        await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(OversizedQuarantineBoundaryBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(QuarantineBoundaryEvidenceBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.ToArray().SequenceEqual(CreateExpectedBoundaryPrefix())).IsTrue();
        await AssertBoundedUtf8Async(marker.Evidence.ContractId);
        await AssertBoundedUtf8Async(marker.Evidence.ContentType);
        await AssertBoundedUtf8Async(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies impossible caller evidence cannot poison the stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsImpossibleSuppliedEvidencePrefix()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = new("reading", SchemaVersion, "application/json", 1, "hash", "xx"u8.ToArray()),
            Envelope = null,
        };
        Func<Task> quarantine = () => store.QuarantinePayloadAsync(request, CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<ArgumentException>();
        await Assert.That(await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies oversized valid Unicode evidence metadata is bounded without splitting a surrogate pair.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The expected marker or metadata is missing.</exception>
    [Test]
    public async Task QuarantinePayloadAsyncBoundsUnicodeEvidenceMetadataOnSurrogateBoundary()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var unicode = CreateOversizedUnicodeEvidenceText();
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = new(unicode, SchemaVersion, unicode, FirstClientSequence, unicode, "x"u8.ToArray()),
            Envelope = null,
        };

        _ = await store.QuarantinePayloadAsync(request, CancellationToken.None);
        var marker = await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException("Expected quarantine marker.");

        await AssertUnicodeBoundaryAsync(marker.Evidence.ContractId);
        await AssertUnicodeBoundaryAsync(marker.Evidence.ContentType);
        await AssertUnicodeBoundaryAsync(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies overlong and malformed caller request metadata is rejected instead of truncated.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsInvalidCallerRequestMetadata()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> overlongReasonCode = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { ReasonCode = new('r', OversizedQuarantineBoundaryBytes) },
            CancellationToken.None).AsTask();
        Func<Task> malformedCursor = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { Cursor = "\uD800" },
            CancellationToken.None).AsTask();

        await Assert.That(overlongReasonCode).ThrowsExactly<ArgumentException>();
        await Assert.That(malformedCursor).ThrowsExactly<ArgumentException>();
        await Assert.That(await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies supplied subscription identities must match the durable stream binding before poisoning.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsMismatchedSubscriptionBeforePoisoning()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> quarantine = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                SubscriptionId = SubscriptionId.New(),
                Evidence = CreateRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies a supplied operation id must belong to the request stream before poisoning.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsMismatchedOperationStreamBeforePoisoning()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var otherOperation = await CommitOperationAsync(store, OtherStream, FirstClientSequence, "other-operation");
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> quarantine = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                OperationId = otherOperation.OperationId,
                Evidence = CreateRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        await Assert.That(await store.GetPayloadQuarantineAsync(OtherStream, CancellationToken.None)).IsNull();
        var lease = RequireBatch(await LeaseSingleBatchAsync(
            store,
            new(OtherStream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(otherOperation.OperationId);
    }

    /// <summary>Verifies an unregistered operation id cannot poison an in-memory stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsUnregisteredOperationBeforePoisoning()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> quarantine = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                OperationId = OperationId.New(),
                Evidence = CreateRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies idempotent requests still validate caller identity before returning an existing marker.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task QuarantinePayloadAsyncRejectsMismatchedIdempotentRequestBeforeReturningExistingMarker()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var first = await store.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> idempotentMismatch = () => store.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { SubscriptionId = SubscriptionId.New() },
            CancellationToken.None).AsTask();

        await Assert.That(idempotentMismatch).ThrowsExactly<InvalidOperationException>();
        await Assert.That((await store.GetPayloadQuarantineAsync(Stream, CancellationToken.None))?.QuarantineId).IsEqualTo(first.Record.QuarantineId);
    }

    /// <summary>Creates oversized supplied evidence for boundary tests.</summary>
    /// <param name="payloadLength">The advertised payload length.</param>
    /// <returns>The oversized evidence.</returns>
    private static LocalPayloadQuarantineEvidence CreateOversizedQuarantineBoundaryEvidence(int payloadLength)
    {
        var prefix = new byte[OversizedQuarantineBoundaryBytes];
        Array.Fill(prefix, (byte)'x');
        return new(
            new('c', OversizedQuarantineBoundaryBytes),
            SchemaVersion,
            new('t', OversizedQuarantineBoundaryBytes),
            payloadLength,
            new('h', OversizedQuarantineBoundaryBytes),
            prefix);
    }

    /// <summary>Creates the expected retained evidence prefix.</summary>
    /// <returns>The expected prefix.</returns>
    private static byte[] CreateExpectedBoundaryPrefix()
    {
        var prefix = new byte[QuarantineBoundaryEvidenceBytes];
        Array.Fill(prefix, (byte)'x');
        return prefix;
    }

    /// <summary>Asserts the supplied text fits the quarantine metadata evidence boundary.</summary>
    /// <param name="value">The value to inspect.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    /// <exception cref="InvalidOperationException">The expected metadata is missing.</exception>
    private static async Task AssertBoundedUtf8Async(string? value)
    {
        if (value is null)
        {
            throw new InvalidOperationException("Expected retained evidence metadata.");
        }

        await Assert.That(System.Text.Encoding.UTF8.GetByteCount(value)).IsLessThanOrEqualTo(QuarantineBoundaryEvidenceBytes);
    }

    /// <summary>Creates valid oversized Unicode evidence metadata.</summary>
    /// <returns>The oversized Unicode string.</returns>
    private static string CreateOversizedUnicodeEvidenceText()
    {
        var builder = new System.Text.StringBuilder();
        for (var i = 0; i < (OversizedQuarantineBoundaryBytes / GrinningFaceUtf8Bytes) + 1; i++)
        {
            _ = builder.Append("\U0001F600");
        }

        return builder.ToString();
    }

    /// <summary>Asserts that retained Unicode metadata is bounded and ends on a surrogate-pair boundary.</summary>
    /// <param name="value">The retained value.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    /// <exception cref="InvalidOperationException">The expected metadata is missing.</exception>
    private static async Task AssertUnicodeBoundaryAsync(string? value)
    {
        if (value is null)
        {
            throw new InvalidOperationException("Expected retained evidence metadata.");
        }

        await Assert.That(System.Text.Encoding.UTF8.GetByteCount(value)).IsEqualTo(QuarantineBoundaryEvidenceBytes);
        await Assert.That(char.IsSurrogatePair(value, value.Length - SurrogatePairCodeUnitCount)).IsTrue();
    }
}
