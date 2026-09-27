// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Public <c>PublishAsync</c> admission tests for contexts composed by <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies dropping durable admission and unsupported custom admission fail before any sequence is consumed.</summary>
    /// <param name="strategy">The unsupported admission strategy.</param>
    /// <param name="durable">Whether the publication is durable.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(BufferStrategy.DropOldest, true)]
    [Arguments(BufferStrategy.DropNewest, true)]
    [Arguments(BufferStrategy.Custom, true)]
    [Arguments(BufferStrategy.Custom, false)]
    public async Task PublicPublishAsyncRejectsUnsupportedAdmissionBeforeCommit(BufferStrategy strategy, bool durable)
    {
        await using var store = CreatePublicAdmissionStore("oc-public-invalid-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateByteBoundOutbox()).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(async () => await stream.PublishAsync(new(1), CreatePublicPublishOptions(strategy, durable), CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        var receipt = await stream.PublishAsync(new(1), null, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(receipt.ClientSequence).IsEqualTo(1L);
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1);
    }

    /// <summary>Verifies the outbox byte limit rejects Reject and DropNewest publications without evicting committed work.</summary>
    /// <param name="strategy">The non-evicting, non-blocking admission strategy.</param>
    /// <param name="durable">Whether the publications are durable.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(BufferStrategy.Reject, true)]
    [Arguments(BufferStrategy.Reject, false)]
    [Arguments(BufferStrategy.DropNewest, false)]
    public async Task PublicPublishAsyncByteLimitRejectsWithoutEvictingCommittedWork(BufferStrategy strategy, bool durable)
    {
        await using var store = CreatePublicAdmissionStore("oc-public-bytes-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateByteBoundOutbox()).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var options = CreatePublicPublishOptions(strategy, durable);

        var first = await stream.PublishAsync(new(HalfOutboxPayloadDelta), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var failure = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => stream.PublishAsync(new(HalfOutboxPayloadDelta), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout));
        var small = await stream.PublishAsync(new(1), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(failure?.CanFitWhenEmpty).IsTrue();
        await Assert.That(first.ClientSequence).IsEqualTo(1L);
        await Assert.That(small.ClientSequence).IsEqualTo(SecondClientSequence);
        await AssertPendingValuesAsync(store, stream.SubscriptionId, HalfOutboxPayloadDelta, 1);
    }

    /// <summary>Verifies the outbox byte limit makes DropOldest evict committed non-durable work to admit the new publication.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicPublishAsyncByteLimitDropOldestEvictsCommittedNonDurableWork()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-bytes-oldest-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateByteBoundOutbox()).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var options = CreatePublicPublishOptions(BufferStrategy.DropOldest, durable: false);

        var first = await stream.PublishAsync(new(HalfOutboxPayloadDelta), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var second = await stream.PublishAsync(new(HalfOutboxPayloadDelta), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(second.ClientSequence).IsEqualTo(SecondClientSequence);
        await AssertPendingValuesAsync(store, stream.SubscriptionId, HalfOutboxPayloadDelta);
        await AssertDeadLetteredAsync(store, first.OperationId, DroppedOldestReasonCode);
    }

    /// <summary>Verifies Block fails promptly when an operation cannot fit even an empty outbox.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicPublishAsyncBlockRejectsOperationLargerThanEmptyOutbox()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-oversized-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateByteBoundOutbox()).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var options = CreatePublicPublishOptions(BufferStrategy.Block, durable: true);

        var failure = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => stream.PublishAsync(new(OversizedPayloadDelta), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout));
        var receipt = await stream.PublishAsync(new(1), options, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(failure?.CanFitWhenEmpty).IsFalse();
        await Assert.That(receipt.ClientSequence).IsEqualTo(1L);
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1);
    }

    /// <summary>
    /// Verifies cancellation before commit removes a blocked publisher without leaking admission, and cancellation after
    /// commit cancels only the caller's wait while the durable operation still synchronizes.
    /// </summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicPublishAsyncCancellationBeforeCommitReleasesAdmissionAndAfterCommitCancelsOnlyWait()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-cancel-");
        await using var transport = new RecoveredUploadTransportAdapter();
        await using var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateSingleOperationOutbox(1)).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var block = CreatePublicPublishOptions(BufferStrategy.Block, durable: true);
        var committed = await stream.PublishAsync(new(1), block, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        for (var round = 0; round < CancelledPublisherRounds; round++)
        {
            using CancellationTokenSource cancellation = new();
            var blocked = stream.PublishAsync(new(RejectedInputDelta), block, cancellation.Token).AsTask();
            await Assert.That(blocked.IsCompleted).IsFalse();
            await cancellation.CancelAsync();
            await Assert.That(async () => await blocked.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();
        }

        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1);
        using (CancellationTokenSource waitCancellation = new())
        {
            await waitCancellation.CancelAsync();
            await Assert.That(async () => await context.SyncEngine.AwaitSynchronizedAsync(
                    committed.OperationId,
                    GuardTimeout,
                    TimeProvider.System,
                    waitCancellation.Token))
                .Throws<OperationCanceledException>();
        }

        var saved = await store.GetOperationStatusAsync(committed.OperationId, CancellationToken.None);
        await Assert.That(saved?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await context.SyncEngine.AwaitSynchronizedAsync(committed.OperationId, GuardTimeout, TimeProvider.System, CancellationToken.None);
        var next = await stream.PublishAsync(new(RejectedInputDelta), block, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(next.ClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies context disposal releases a publisher blocked on outbox capacity and keeps committed work.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicContextDisposeReleasesBlockedPublisherAndKeepsCommittedWork()
    {
        await using var store = CreatePublicAdmissionStore("oc-public-dispose-");
        await using var transport = new RecordingTransportAdapter();
        var context = CreatePublicAdmissionBuilder(store, transport, new PaddedCounterPayloadSerializer(), CreateSingleOperationOutbox(1)).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var block = CreatePublicPublishOptions(BufferStrategy.Block, durable: true);
        _ = await stream.PublishAsync(new(1), block, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var subscriptionId = stream.SubscriptionId;
        var blocked = stream.PublishAsync(new(RejectedInputDelta), block, CancellationToken.None).AsTask();

        await Assert.That(blocked.IsCompleted).IsFalse();
        await context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);

        await Assert.That(async () => await blocked.WaitAsync(GuardTimeout)).Throws<InvalidOperationException>();
        await AssertPendingValuesAsync(store, subscriptionId, 1);
    }
}
