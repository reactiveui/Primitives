// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>
/// Shared conformance suite for every <see cref="IRemoteTransportAdapter"/>. Each case runs through the public adapter
/// against a real <see cref="ServerStreamHub"/> and runs only when the adapter advertises the capability it proves.
/// </summary>
public sealed partial class IRemoteTransportAdapterTests
{
    /// <summary>The first client sequence.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second client sequence.</summary>
    private const int SecondSequence = 2;

    /// <summary>The third client sequence.</summary>
    private const int ThirdSequence = 3;

    /// <summary>The fourth client sequence.</summary>
    private const int FourthSequence = 4;

    /// <summary>The number of operations in a two-operation batch.</summary>
    private const int PairCount = 2;

    /// <summary>The number of domain effects after two corrupted two-operation pushes.</summary>
    private const int TwoPairEffects = 4;

    /// <summary>Maps each transport capability to the conformance tests that prove it.</summary>
    private static readonly (RemoteTransportCapabilities Capability, string Test)[] CapabilitySuites =
    [
        (RemoteTransportCapabilities.BatchPush, nameof(PushAsyncHonorsNegotiatedOperationBoundWithCompleteResults)),
        (RemoteTransportCapabilities.BatchPush, nameof(PushAsyncRejectsPartialAndForeignOperationResults)),
        (RemoteTransportCapabilities.CursorResume, nameof(SubscribeAsyncResumesAtNextDurableEventAfterReconnect)),
        (RemoteTransportCapabilities.CursorResume, nameof(SubscribeAsyncDeliversDuplicateBatchWithoutAdvancingCursor)),
        (RemoteTransportCapabilities.CursorResume, nameof(SubscribeAsyncRejectsReorderedBatchesAndRedeliversInOrder)),
        (RemoteTransportCapabilities.ReceiveAcknowledgements, nameof(AcknowledgeAsyncLossRedeliversFromLastAcknowledgedCursor)),
        (RemoteTransportCapabilities.ServerIdempotency, nameof(PushAsyncReturnsOriginalTerminalResultsForDuplicateOperations)),
        (RemoteTransportCapabilities.AtomicApplyAndAcknowledge, nameof(PushAsyncCommitsEffectEventAndLedgerTogether)),
        (RemoteTransportCapabilities.StreamingReceive, nameof(SubscribeAsyncStreamsCommittedEventsOnOneEnumeration)),
        (RemoteTransportCapabilities.SnapshotRecovery, nameof(GetSnapshotAsyncRecoversFrontierAfterCursorGap)),
    ];

    /// <summary>Verifies a duplicate operation returns the original terminal result without a second effect.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task PushAsyncReturnsOriginalTerminalResultsForDuplicateOperations(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.ServerIdempotency);
        SyncOperation[] operations = [CreateOperation(FirstSequence), CreateOperation(SecondSequence)];

        var original = await session.PushAsync(new(Guid.NewGuid(), operations), CancellationToken.None);
        var duplicate = await session.PushAsync(new(Guid.NewGuid(), operations), CancellationToken.None);

        await AssertAcceptedAsync(original, operations);
        await Assert.That(duplicate.Operations.SequenceEqual(original.Operations)).IsTrue();
        await Assert.That(harness.Domain.CallCount).IsEqualTo(PairCount);
        await Assert.That(harness.Peer.ApplyCalls).IsEqualTo(PairCount);
    }

    /// <summary>Verifies an oversized batch is rejected before any server effect and a full batch returns every result.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task PushAsyncHonorsNegotiatedOperationBoundWithCompleteResults(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.BatchPush);
        var bound = session.NegotiatedCapabilities.MaximumBatchOperations;
        var oversized = Enumerable.Range(FirstSequence, bound + 1).Select(static sequence => CreateOperation(sequence)).ToArray();
        var full = oversized.Take(bound).ToArray();

        _ = await Assert.ThrowsAsync<Exception>(() => session.PushAsync(new(Guid.NewGuid(), oversized), CancellationToken.None).AsTask());
        await Assert.That(harness.Peer.ApplyCalls).IsEqualTo(0);
        await Assert.That(harness.Domain.CallCount).IsEqualTo(0);

        var result = await session.PushAsync(new(Guid.NewGuid(), full), CancellationToken.None);
        await AssertAcceptedAsync(result, full);
        await Assert.That(harness.Domain.CallCount).IsEqualTo(bound);
    }

    /// <summary>Verifies a push result that omits or substitutes an operation result invalidates the whole batch.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task PushAsyncRejectsPartialAndForeignOperationResults(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        harness.RequireCapability(session, RemoteTransportCapabilities.BatchPush);
        harness.RequireCapability(session, RemoteTransportCapabilities.ServerIdempotency);

        await AssertCorruptedPushRejectedThenRetriedAsync(harness, session, PushResultFault.Truncate, FirstSequence);
        await AssertCorruptedPushRejectedThenRetriedAsync(harness, session, PushResultFault.ForeignOperation, ThirdSequence);
        await Assert.That(harness.Domain.CallCount).IsEqualTo(TwoPairEffects);
    }

    /// <summary>Verifies the server effect, its canonical event and its ledger entry commit together.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task PushAsyncCommitsEffectEventAndLedgerTogether(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync(DeliveryGuarantee.ExactlyOnce);
        harness.RequireCapability(session, RemoteTransportCapabilities.AtomicApplyAndAcknowledge);
        var first = CreateOperation(FirstSequence);
        var second = CreateOperation(SecondSequence);
        var subscriptionId = SubscriptionId.New();

        var original = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
        var replayed = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
        _ = await session.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None);
        await using var receive = Subscribe(session, subscriptionId, null);
        var firstPage = await ReadNextAsync(receive);
        var secondPage = await ReadNextAsync(receive);
        await session.AcknowledgeAsync(new(subscriptionId, Stream, secondPage.NextCursor), CancellationToken.None);

        await Assert.That(replayed.Operations[0]).IsEqualTo(original.Operations[0]);
        await Assert.That(harness.Domain.CallCount).IsEqualTo(PairCount);
        await AssertCausedByAsync(firstPage, first);
        await Assert.That(firstPage.Events[0].Origin?.ClientId).IsEqualTo(Client);
        await AssertCausedByAsync(secondPage, second);
        await Assert.That(secondPage.PreviousCursor).IsEqualTo(firstPage.NextCursor);
    }

    /// <summary>Verifies connect fails when a required guarantee depends on a capability the peer omits.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task ConnectAsyncRejectsGuaranteesWhosePeerCapabilityIsMissing(int transport)
    {
        var defaults = transport == LoopbackTransport ? LoopbackFeatures : HttpFeatures;
        (RemoteTransportCapabilities Missing, DeliveryGuarantee Guarantee)[] cases =
        [
            (RemoteTransportCapabilities.ServerIdempotency, DeliveryGuarantee.AtLeastOnce),
            (RemoteTransportCapabilities.AtomicApplyAndAcknowledge, DeliveryGuarantee.ExactlyOnce),
            (RemoteTransportCapabilities.ReceiveAcknowledgements, DeliveryGuarantee.ExactlyOnce),
        ];

        foreach (var (missing, guarantee) in cases)
        {
            await using var harness = TransportHarness.Create(transport, InMemoryHub, new() { PeerFeatures = defaults & ~missing });
            _ = await Assert.ThrowsAsync<Exception>(() => harness.ConnectAsync(guarantee).AsTask());
            await Assert.That(harness.Peer.ApplyCalls).IsEqualTo(0);
        }

        await using var complete = TransportHarness.Create(transport, InMemoryHub);
        await using var session = await complete.ConnectAsync(DeliveryGuarantee.ExactlyOnce);
        await Assert.That(session.NegotiatedCapabilities.Features).IsEqualTo(defaults);
    }

    /// <summary>Verifies every advertised transport capability maps to at least one behavioral conformance test.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task EveryAdvertisedCapabilityHasBehavioralConformanceTest(int transport)
    {
        await using var harness = TransportHarness.Create(transport, InMemoryHub);
        await using var session = await harness.ConnectAsync();
        var advertised = harness.Adapter.Capabilities & session.NegotiatedCapabilities.Features;
        var covered = RemoteTransportCapabilities.None;
        foreach (var (capability, test) in CapabilitySuites)
        {
            await Assert.That(typeof(IRemoteTransportAdapterTests).GetMethod(test)).IsNotNull();
            covered |= capability;
        }

        await Assert.That(advertised & ~covered).IsEqualTo(RemoteTransportCapabilities.None);
        await Assert.That(covered).IsEqualTo(LoopbackFeatures | HttpFeatures);
    }

    /// <summary>Pushes a batch whose result the peer corrupts, then proves an idempotent retry completes it.</summary>
    /// <param name="harness">The transport stack.</param>
    /// <param name="session">The connected session.</param>
    /// <param name="fault">The injected fault.</param>
    /// <param name="firstSequence">The first client sequence of the two-operation batch.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertCorruptedPushRejectedThenRetriedAsync(
        TransportHarness harness,
        IRemoteTransportSession session,
        PushResultFault fault,
        int firstSequence)
    {
        SyncOperation[] operations = [CreateOperation(firstSequence), CreateOperation(firstSequence + 1)];
        var callsBefore = harness.Peer.ApplyCalls;
        harness.Peer.CorruptNextPushResult(fault);

        _ = await Assert.ThrowsAsync<Exception>(() => session.PushAsync(new(Guid.NewGuid(), operations), CancellationToken.None).AsTask());
        await Assert.That(harness.Peer.ApplyCalls).IsEqualTo(callsBefore + 1);

        var retried = await session.PushAsync(new(Guid.NewGuid(), operations), CancellationToken.None);
        await AssertAcceptedAsync(retried, operations);
    }

    /// <summary>Asserts a push result carries exactly one accepted result per operation, in order.</summary>
    /// <param name="result">The push result.</param>
    /// <param name="operations">The pushed operations.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertAcceptedAsync(RemoteSyncResult result, SyncOperation[] operations)
    {
        await Assert.That(result.Operations.Count).IsEqualTo(operations.Length);
        for (var index = 0; index < operations.Length; index++)
        {
            await Assert.That(result.Operations[index].OperationId).IsEqualTo(operations[index].OperationId);
            await Assert.That(result.Operations[index].Kind).IsEqualTo(OperationResultKind.Accepted);
        }
    }

    /// <summary>Opens a subscription enumerator for the shared stream.</summary>
    /// <param name="session">The connected session.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="cursor">The resume cursor.</param>
    /// <param name="cancellationToken">The optional cancellation token.</param>
    /// <returns>The enumerator.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IAsyncEnumerator<RemoteEventBatch> Subscribe(
        IRemoteTransportSession session,
        SubscriptionId subscriptionId,
        string? cursor,
        CancellationToken cancellationToken = default) =>
        session.SubscribeAsync(new(Stream, subscriptionId, cursor, StartPosition.FromSequence(0)), cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

    /// <summary>Reads the next batch within the bounded wait.</summary>
    /// <param name="enumerator">The subscription enumerator.</param>
    /// <returns>The batch.</returns>
    /// <exception cref="InvalidOperationException">The subscription completed without a batch.</exception>
    private static async Task<RemoteEventBatch> ReadNextAsync(IAsyncEnumerator<RemoteEventBatch> enumerator)
    {
        var moved = await enumerator.MoveNextAsync().AsTask().WaitAsync(AwaitTimeout);
        return moved ? enumerator.Current : throw new InvalidOperationException("The subscription completed before delivering a batch.");
    }

    /// <summary>Asserts a batch carries exactly the single event caused by an operation.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="operation">The causing operation.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertCausedByAsync(RemoteEventBatch batch, SyncOperation operation)
    {
        await Assert.That(batch.Events.Count).IsEqualTo(1);
        await Assert.That(batch.Events[0].CausedByOperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Returns whether two batches carry the same event identifiers in the same order.</summary>
    /// <param name="actual">The actual batch.</param>
    /// <param name="expected">The expected batch.</param>
    /// <returns><see langword="true"/> when the event identifiers match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool SameEventIds(RemoteEventBatch actual, RemoteEventBatch expected) =>
        actual.Events.Select(static remoteEvent => remoteEvent.EventId).SequenceEqual(expected.Events.Select(static remoteEvent => remoteEvent.EventId));
}
