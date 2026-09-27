// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Receive, cursor and acknowledgement conformance cases for <see cref="IRemoteTransportAdapter"/>.</summary>
public sealed partial class IRemoteTransportAdapterTests
{
    /// <summary>A cursor the hub never issued, so it lies outside retained history.</summary>
    private const string UnknownCursor = "missing-cursor";

    /// <summary>The number of events the domain emits per operation in the receive-bound case.</summary>
    private const int TwoEventsPerOperation = 2;

    /// <summary>The snapshot response byte bound requested by the client.</summary>
    private const int SnapshotResponseBytes = 16 * 1024;

    /// <summary>Verifies reconnecting with the acknowledged cursor resumes at exactly the next durable event.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncResumesAtNextDurableEventAfterReconnect(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        var subscriptionId = SubscriptionId.New();
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        string acknowledged;
        await using (var session = await harness.ConnectAsync())
        {
            harness.RequireCapability(session, RemoteTransportCapabilities.CursorResume);
            _ = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
            await using var receive = Subscribe(session, subscriptionId, null);
            var page = await ReadNextAsync(receive);
            await AssertCausedByAsync(page, first);
            acknowledged = page.NextCursor;
            await session.AcknowledgeAsync(new(subscriptionId, Stream, acknowledged), CancellationToken.None);
        }

        await using var resumed = await harness.ConnectAsync();
        _ = await resumed.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None);
        await using var resumedReceive = Subscribe(resumed, subscriptionId, acknowledged);
        var next = await ReadNextAsync(resumedReceive);

        await Assert.That(next.PreviousCursor).IsEqualTo(acknowledged);
        await AssertCausedByAsync(next, second);
    }

    /// <summary>
    /// Verifies a cursor outside retained history fails the subscription without inventing progress. Spec 17.1 layer 7
    /// expects a typed <see cref="RemoteSubscriptionRetentionGapException"/> so the engine can run snapshot recovery;
    /// this case pins what each adapter surfaces today over a real <see cref="ServerStreamHub"/>.
    /// </summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncFailsClosedOnServerRetentionGap(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
        await using var receive = Subscribe(session, SubscriptionId.New(), UnknownCursor);

        var failure = await Assert.ThrowsAsync<Exception>(() => receive.MoveNextAsync().AsTask().WaitAsync(AwaitTimeout));

        await Assert.That(failure).IsNotTypeOf<TimeoutException>();
        await Assert.That(harness.Peer.SubscribeCalls).IsEqualTo(1);
        if (transport == LoopbackTransport)
        {
            await Assert.That(failure).IsTypeOf<ServerReceiveRetentionGapException>();
            return;
        }

        await Assert.That(failure).IsTypeOf<HttpRemoteTransportException>();
        await Assert.That(((HttpRemoteTransportException)failure!).IsTransient).IsTrue();
    }

    /// <summary>Verifies an exact duplicate batch cannot advance the cursor and delivery continues from the same cursor.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncDeliversDuplicateBatchWithoutAdvancingCursor(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.CursorResume);
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        _ = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
        _ = await session.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None);
        await using var receive = Subscribe(session, SubscriptionId.New(), null);

        var original = await ReadNextAsync(receive);
        harness.Peer.DuplicateNextBatch();
        var duplicate = await ReadNextAsync(receive);
        var next = await ReadNextAsync(receive);

        await Assert.That(duplicate.BatchId).IsEqualTo(original.BatchId);
        await Assert.That(duplicate.NextCursor).IsEqualTo(original.NextCursor);
        await Assert.That(SameEventIds(duplicate, original)).IsTrue();
        await Assert.That(next.PreviousCursor).IsEqualTo(original.NextCursor);
        await AssertCausedByAsync(next, second);
    }

    /// <summary>Verifies a reordered batch is rejected and a resubscribe redelivers both batches in order without loss.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncRejectsReorderedBatchesAndRedeliversInOrder(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.CursorResume);
        var subscriptionId = SubscriptionId.New();
        var second = CreateOperation(SecondSequence);
        var third = CreateOperation(ThirdSequence);
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
        var anchor = await ReadFirstPageAsync(session, subscriptionId, null);
        _ = await session.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None);
        _ = await session.PushAsync(new(Guid.NewGuid(), [third]), CancellationToken.None);
        harness.Peer.ReorderNextBatches();
        var subscribeCalls = harness.Peer.SubscribeCalls;

        await using (var reordered = Subscribe(session, subscriptionId, anchor.NextCursor))
        {
            _ = await Assert.ThrowsAsync<Exception>(() => reordered.MoveNextAsync().AsTask().WaitAsync(AwaitTimeout));
        }

        await Assert.That(harness.Peer.SubscribeCalls).IsEqualTo(subscribeCalls + 1);
        await using var ordered = Subscribe(session, subscriptionId, anchor.NextCursor);
        var secondPage = await ReadNextAsync(ordered);
        var thirdPage = await ReadNextAsync(ordered);
        await Assert.That(secondPage.PreviousCursor).IsEqualTo(anchor.NextCursor);
        await AssertCausedByAsync(secondPage, second);
        await Assert.That(thirdPage.PreviousCursor).IsEqualTo(secondPage.NextCursor);
        await AssertCausedByAsync(thirdPage, third);
    }

    /// <summary>Verifies a lost acknowledgement is not retried silently and a resubscribe redelivers without loss.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task AcknowledgeAsyncLossRedeliversFromLastAcknowledgedCursor(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.ReceiveAcknowledgements);
        var subscriptionId = SubscriptionId.New();
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(SecondSequence)]), CancellationToken.None);
        RemoteEventBatch acknowledged;
        RemoteEventBatch lost;
        await using (var receive = Subscribe(session, subscriptionId, null))
        {
            acknowledged = await ReadNextAsync(receive);
            await session.AcknowledgeAsync(new(subscriptionId, Stream, acknowledged.NextCursor), CancellationToken.None);
            lost = await ReadNextAsync(receive);
        }

        harness.Peer.DropNextAcknowledgement();
        var acknowledgeCalls = harness.Peer.AcknowledgeCalls;
        _ = await Assert.ThrowsAsync<Exception>(() => session.AcknowledgeAsync(new(subscriptionId, Stream, lost.NextCursor), CancellationToken.None).AsTask());
        await Assert.That(harness.Peer.AcknowledgeCalls).IsEqualTo(acknowledgeCalls + 1);

        var redelivered = await ReadFirstPageAsync(session, subscriptionId, acknowledged.NextCursor);
        await session.AcknowledgeAsync(new(subscriptionId, Stream, redelivered.NextCursor), CancellationToken.None);
        await session.AcknowledgeAsync(new(subscriptionId, Stream, redelivered.NextCursor), CancellationToken.None);

        await Assert.That(redelivered.PreviousCursor).IsEqualTo(acknowledged.NextCursor);
        await Assert.That(redelivered.NextCursor).IsEqualTo(lost.NextCursor);
        await Assert.That(SameEventIds(redelivered, lost)).IsTrue();
    }

    /// <summary>Verifies a received batch above the adapter event bound is rejected rather than buffered.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncRejectsBatchesAboveAdapterEventBound(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(
            transport,
            hubKind,
            new() { EventsPerOperation = TwoEventsPerOperation, AdapterMaximumReceiveEvents = 1 });
        await using var session = await harness.ConnectAsync();
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
        await using var receive = Subscribe(session, SubscriptionId.New(), null);

        var failure = await Assert.ThrowsAsync<Exception>(() => receive.MoveNextAsync().AsTask().WaitAsync(AwaitTimeout));

        await Assert.That(failure).IsNotTypeOf<TimeoutException>();
        await Assert.That(harness.Peer.SubscribeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies snapshot recovery returns a frontier from which the subscription resumes at the next event.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task GetSnapshotAsyncRecoversFrontierAfterCursorGap(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.SnapshotRecovery);
        var recovery = session as IRemoteSnapshotRecoverySession;
        await Assert.That(recovery).IsNotNull();
        var subscriptionId = SubscriptionId.New();
        var first = CreateOperation(FirstSequence);
        var third = CreateOperation(ThirdSequence);
        _ = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
        var expired = await ReadFirstPageAsync(session, subscriptionId, null);
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(SecondSequence)]), CancellationToken.None);

        var result = await recovery!.GetSnapshotAsync(
            new()
            {
                StreamId = Stream,
                SubscriptionId = subscriptionId,
                ExpiredCursor = expired.NextCursor,
                ClientStateContractId = Contract,
                ClientStateSchemaVersion = 1,
                SnapshotFormatVersion = 1,
                PendingOperations = [first],
                MaximumResponseBytes = SnapshotResponseBytes,
            },
            CancellationToken.None);
        var frontier = result.Checkpoint?.FrontierCursor;
        await Assert.That(frontier).IsNotNull();
        _ = await session.PushAsync(new(Guid.NewGuid(), [third]), CancellationToken.None);
        var resumed = await ReadFirstPageAsync(session, subscriptionId, frontier);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(frontier).IsNotEqualTo(expired.NextCursor);
        await Assert.That(result.Checkpoint?.ClientState.PayloadHash).IsEqualTo(FixedSnapshotMaterializer.ClientState.PayloadHash);
        await Assert.That(result.OperationDispositions[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(result.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(resumed.PreviousCursor).IsEqualTo(frontier);
        await AssertCausedByAsync(resumed, third);
    }

    /// <summary>Reads the first page of a fresh subscription enumerator and closes it.</summary>
    /// <param name="session">The connected session.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="cursor">The resume cursor.</param>
    /// <returns>The first page.</returns>
    private static async Task<RemoteEventBatch> ReadFirstPageAsync(IRemoteTransportSession session, SubscriptionId subscriptionId, string? cursor)
    {
        await using var receive = Subscribe(session, subscriptionId, cursor);
        return await ReadNextAsync(receive);
    }
}
