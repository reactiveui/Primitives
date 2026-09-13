// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
/// <content>Validation tests for <see cref="LoopbackTransportAdapter"/>.</content>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Verifies malformed options are rejected before connection.</summary>
    /// <param name="scenario">The malformed option scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("request-limit")]
    [Arguments("batch-bytes")]
    [Arguments("retention")]
    [Arguments("protocol")]
    [Arguments("features")]
    [Arguments("client")]
    [Arguments("tenant-unicode")]
    [Arguments("client-size")]
    public async Task OptionsRejectMalformedBoundsCapabilitiesAndIdentity(string scenario)
    {
        var hub = new RecordingHub();
        var options = scenario switch
        {
            "request-limit" => CreateOptions(hub) with { MaximumConcurrentRequests = 0 },
            "batch-bytes" => CreateOptions(hub) with { PeerCapabilities = CreateCapabilities(maximumBytes: 0) },
            "retention" => CreateOptions(hub) with { PeerCapabilities = new(new(1, 0), AllFeatures, PeerMaximumOperations, DefaultBatchBytes, TimeSpan.Zero, TimeSpan.FromDays(ClientRetentionDays)) },
            "protocol" => CreateOptions(hub) with
            {
                PeerCapabilities = new(
                    new(2, 0),
                    AllFeatures,
                    PeerMaximumOperations,
                    DefaultBatchBytes,
                    TimeSpan.FromDays(ServerRetentionDays),
                    TimeSpan.FromDays(ClientRetentionDays)),
            },
            "features" => CreateOptions(hub) with { PeerCapabilities = CreateCapabilities((RemoteTransportCapabilities)int.MinValue) },
            "client" => CreateOptions(hub, new(TrustedTenant, " ")),
            "tenant-unicode" => CreateOptions(hub, new(new string('\ud800', 1), TrustedClientId)),
            _ => CreateOptions(hub, new(TrustedTenant, new string('c', OversizedStringLength))) with { MaximumStringBytes = BoundedStringBytes },
        };

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            async () =>
            {
                await using var rejected = new LoopbackTransportAdapter(options);
            });
        await Assert.That(exception).IsNotNull();
    }

    /// <summary>Verifies public connection validation covers accepted and rejected guarantee shapes.</summary>
    /// <param name="scenario">The connection validation scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("at-most-once")]
    [Arguments("exactly-once")]
    [Arguments("range")]
    [Arguments("unknown")]
    [Arguments("missing-at-least-once")]
    [Arguments("missing-retention")]
    public async Task ConnectValidatesGuaranteesAndProtocolRange(string scenario)
    {
        var hub = new RecordingHub();
        var options = scenario switch
        {
            "missing-at-least-once" => CreateOptions(hub) with { PeerCapabilities = CreateCapabilities(AllFeatures & ~RemoteTransportCapabilities.ServerIdempotency) },
            "missing-retention" => CreateOptions(hub) with { PeerCapabilities = new(new(1, 0), AllFeatures, PeerMaximumOperations, DefaultBatchBytes, null, TimeSpan.FromDays(ClientRetentionDays)) },
            _ => CreateOptions(hub),
        };
        await using var adapter = new LoopbackTransportAdapter(options);
        DeliveryGuarantee[] exactGuarantees = [DeliveryGuarantee.ExactlyOnce];
        var request = scenario switch
        {
            "at-most-once" => CreateConnectRequest(guarantees: [DeliveryGuarantee.AtMostOnce]),
            "exactly-once" or "missing-retention" => CreateConnectRequest(guarantees: exactGuarantees),
            "range" => new TransportConnectRequest(new(new(2, 0), new(1, 0)), new(TrustedClientId), [DeliveryGuarantee.AtMostOnce]),
            "unknown" => CreateConnectRequest(guarantees: [(DeliveryGuarantee)int.MinValue]),
            _ => CreateConnectRequest(),
        };

        if (scenario is "at-most-once" or "exactly-once")
        {
            await using var session = await adapter.ConnectAsync(request, CancellationToken.None);
            await Assert.That(session.NegotiatedCapabilities).IsEqualTo(options.PeerCapabilities);
            return;
        }

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => adapter.ConnectAsync(request, CancellationToken.None).AsTask());
    }

    /// <summary>Verifies subscribe and acknowledgement validation rejects malformed public input.</summary>
    /// <param name="scenario">The input validation scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("subscribe-stream")]
    [Arguments("subscribe-id")]
    [Arguments("subscribe-position")]
    [Arguments("subscribe-cursor")]
    [Arguments("ack-id")]
    [Arguments("ack-stream")]
    [Arguments("ack-cursor")]
    public async Task SessionRejectsMalformedSubscribeAndAcknowledgementInput(string scenario)
    {
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumStringBytes = BoundedStringBytes });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var subscribeRequest = scenario switch
        {
            "subscribe-stream" => new RemoteSubscribeRequest(default, SubscriptionId.New(), null, StartPosition.Latest),
            "subscribe-id" => new RemoteSubscribeRequest(Stream, default, null, StartPosition.Latest),
            "subscribe-position" => CreateSubscribeRequestWithNullPosition(),
            "subscribe-cursor" => new RemoteSubscribeRequest(Stream, SubscriptionId.New(), new('c', OversizedStringLength), StartPosition.Latest),
            _ => CreateSubscribeRequest(),
        };
        var acknowledgement = scenario switch
        {
            "ack-id" => new ReceiveAcknowledgement(default, Stream, NextCursor),
            "ack-stream" => new ReceiveAcknowledgement(SubscriptionId.New(), default, NextCursor),
            "ack-cursor" => new ReceiveAcknowledgement(SubscriptionId.New(), Stream, new('c', OversizedStringLength)),
            _ => new ReceiveAcknowledgement(SubscriptionId.New(), Stream, NextCursor),
        };

        if (scenario.StartsWith("subscribe", StringComparison.Ordinal))
        {
            _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CollectAsync(session.SubscribeAsync(subscribeRequest, CancellationToken.None)).AsTask());
            return;
        }

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.AcknowledgeAsync(acknowledgement, CancellationToken.None).AsTask());
    }

    /// <summary>Verifies all public start position factories are accepted by loopback subscribe validation.</summary>
    /// <param name="scenario">The start position scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("timestamp")]
    [Arguments("sequence")]
    [Arguments("cursor")]
    public async Task SubscribeAcceptsPublicStartPositions(string scenario)
    {
        var position = scenario switch
        {
            "timestamp" => StartPosition.FromTimestamp(CommittedUtc),
            "sequence" => StartPosition.FromSequence(0),
            _ => StartPosition.FromCursor(NextCursor),
        };
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var request = new RemoteSubscribeRequest(Stream, SubscriptionId.New(), null, position);

        var batches = await CollectAsync(session.SubscribeAsync(request, CancellationToken.None));

        await Assert.That(batches).Count().IsEqualTo(0);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies outgoing batch validation rejects malformed operations and membership.</summary>
    /// <param name="scenario">The outgoing validation scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("empty-batch")]
    [Arguments("operation-id")]
    [Arguments("stream")]
    [Arguments("sequence")]
    [Arguments("type")]
    [Arguments("mixed-stream")]
    [Arguments("duplicate-id")]
    [Arguments("duplicate-sequence")]
    [Arguments("unordered")]
    [Arguments("base-version")]
    [Arguments("metadata-count")]
    [Arguments("payload")]
    [Arguments("payload-version")]
    public async Task PushRejectsMalformedOperationsBeforeHubUse(string scenario)
    {
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumMetadataEntries = 1, MaximumStringBytes = BoundedStringBytes });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var operationId = OperationId.New();
        var batch = scenario switch
        {
            "empty-batch" => new SyncBatch(Guid.Empty, []),
            "operation-id" => new SyncBatch(Guid.NewGuid(), [CreateOperation(operationId: new OperationId(Guid.Empty))]),
            "stream" => new SyncBatch(Guid.NewGuid(), [CreateOperation(streamId: default(StreamId))]),
            "sequence" => new SyncBatch(Guid.NewGuid(), [CreateOperation(sequence: 0)]),
            "type" => new SyncBatch(Guid.NewGuid(), [CreateOperation(type: (SyncOperationType)byte.MaxValue)]),
            "mixed-stream" => new SyncBatch(Guid.NewGuid(), [CreateOperation(), CreateOperation(streamId: new("sensor/humidity"), sequence: 2)]),
            "duplicate-id" => new SyncBatch(Guid.NewGuid(), [CreateOperation(operationId: operationId), CreateOperation(operationId: operationId, sequence: 2)]),
            "duplicate-sequence" => new SyncBatch(Guid.NewGuid(), [CreateOperation(), CreateOperation()]),
            "unordered" => new SyncBatch(Guid.NewGuid(), [CreateOperation(sequence: 2), CreateOperation()]),
            "base-version" => new SyncBatch(Guid.NewGuid(), [CreateOperation(baseVersion: new('v', OversizedStringLength))]),
            "metadata-count" => new SyncBatch(Guid.NewGuid(), [CreateOperation(metadata: new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" })]),
            "payload" => new SyncBatch(Guid.NewGuid(), [CreateOperationWithNullPayload()]),
            _ => new SyncBatch(Guid.NewGuid(), [CreateOperation(payload: new(ContractId, 0, PayloadContentType, OperationPayload, "hash"))]),
        };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.PushAsync(batch, CancellationToken.None).AsTask());
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies receive validation rejects malformed events and completion accounting.</summary>
    /// <param name="scenario">The receive validation scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("event-payload")]
    [Arguments("event-metadata")]
    [Arguments("completion-origin")]
    [Arguments("previous-cursor")]
    [Arguments("event-without-origin")]
    public async Task SubscribeRejectsMalformedReceiveAccounting(string scenario)
    {
        var eventWithoutOrigin = new RemoteEvent(
            Guid.NewGuid(),
            Stream,
            NextCursor,
            CommittedUtc,
            null,
            CreatePayload(),
            new Dictionary<string, string>(StringComparer.Ordinal));
        var malformedEvent = scenario switch
        {
            "event-payload" => CreateRemoteEventWithNullPayload(),
            "event-metadata" => new RemoteEvent(
                Guid.NewGuid(),
                Stream,
                NextCursor,
                CommittedUtc,
                null,
                CreatePayload(),
                new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" }),
            _ => eventWithoutOrigin,
        };
        var batch = scenario switch
        {
            "completion-origin" => new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextCursor, []) { CompletedOperations = [new(new(new('c', OversizedStringLength), OperationId.New()), [])] },
            "previous-cursor" => new RemoteEventBatch(Guid.NewGuid(), Stream, new('p', OversizedStringLength), NextCursor, [eventWithoutOrigin]),
            _ => new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextCursor, [malformedEvent]),
        };
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => YieldBatches(batch) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumMetadataEntries = 1, MaximumStringBytes = BoundedStringBytes });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        if (scenario == "event-without-origin")
        {
            var batches = await CollectAsync(session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None));
            await Assert.That(batches).Count().IsEqualTo(1);
            return;
        }

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CollectAsync(session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None)).AsTask());
    }
}
