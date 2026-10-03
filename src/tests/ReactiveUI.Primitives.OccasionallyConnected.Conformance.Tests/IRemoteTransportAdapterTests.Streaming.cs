// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Streaming and polling receive conformance cases for <see cref="IRemoteTransportAdapter"/>.</summary>
public sealed partial class IRemoteTransportAdapterTests
{
    /// <summary>
    /// Verifies a streaming adapter delivers events committed after subscribe on the same enumeration, keeps frame order
    /// for a slow receiver within the page bound, ends promptly on cancellation and resumes after reconnect.
    /// </summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncStreamsCommittedEventsOnOneEnumeration(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        var subscriptionId = SubscriptionId.New();
        string lastCursor;
        await using (var session = await harness.ConnectAsync())
        {
            harness.RequireCapability(session, RemoteTransportCapabilities.StreamingReceive);
            using var cancellation = new CancellationTokenSource();
            await using var receive = Subscribe(session, subscriptionId, null, cancellation.Token);
            var pending = receive.MoveNextAsync().AsTask();
            _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
            await Assert.That(await pending.WaitAsync(AwaitTimeout)).IsTrue();
            var first = receive.Current;

            _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(SecondSequence)]), CancellationToken.None);
            _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(ThirdSequence)]), CancellationToken.None);
            var second = await ReadNextAsync(receive);
            var third = await ReadNextAsync(receive);

            await AssertCausedByAsync(first, CreateOperation(FirstSequence));
            await AssertFrameAsync(second, first, SecondSequence);
            await AssertFrameAsync(third, second, ThirdSequence);
            lastCursor = third.NextCursor;

            var idle = receive.MoveNextAsync().AsTask();
            await cancellation.CancelAsync();
            await Assert.That(await EndsAfterCancellationAsync(idle)).IsTrue();
        }

        await using var reconnected = await harness.ConnectAsync();
        _ = await reconnected.PushAsync(new(Guid.NewGuid(), [CreateOperation(FourthSequence)]), CancellationToken.None);
        var resumed = await ReadFirstPageAsync(reconnected, subscriptionId, lastCursor);
        await Assert.That(resumed.PreviousCursor).IsEqualTo(lastCursor);
        await AssertCausedByAsync(resumed, CreateOperation(FourthSequence));
    }

    /// <summary>
    /// Verifies an adapter without <see cref="RemoteTransportCapabilities.StreamingReceive"/> does not claim it, still
    /// delivers an event committed while a receive is pending, and ends promptly on cancellation.
    /// </summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="hubKind">The hub journal selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport, InMemoryHub)]
    [Arguments(LoopbackTransport, SqliteHub)]
    [Arguments(HttpTransport, InMemoryHub)]
    [Arguments(HttpTransport, SqliteHub)]
    public async Task SubscribeAsyncPollsWithoutClaimingStreamingReceive(int transport, int hubKind)
    {
        await using var harness = TransportHarness.Create(transport, hubKind);
        await using var session = await harness.ConnectAsync();
        Skip.When(
            harness.Advertises(session, RemoteTransportCapabilities.StreamingReceive),
            $"{TransportName(transport)} advertises StreamingReceive; its streaming suite replaces the polling suite.");
        using var cancellation = new CancellationTokenSource();
        await using var receive = Subscribe(session, SubscriptionId.New(), null, cancellation.Token);

        var pending = receive.MoveNextAsync().AsTask();
        _ = await session.PushAsync(new(Guid.NewGuid(), [CreateOperation(FirstSequence)]), CancellationToken.None);
        await Assert.That(await pending.WaitAsync(AwaitTimeout)).IsTrue();
        var delivered = receive.Current;
        var idle = receive.MoveNextAsync().AsTask();
        await cancellation.CancelAsync();

        await AssertCausedByAsync(delivered, CreateOperation(FirstSequence));
        await Assert.That(session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.StreamingReceive)
            .IsEqualTo(RemoteTransportCapabilities.None);
        await Assert.That(await EndsAfterCancellationAsync(idle)).IsTrue();
    }

    /// <summary>Asserts a batch directly follows its predecessor, carries one operation and stays within the page bound.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="previous">The preceding batch.</param>
    /// <param name="sequence">The expected client sequence.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertFrameAsync(RemoteEventBatch batch, RemoteEventBatch previous, int sequence)
    {
        await Assert.That(batch.PreviousCursor).IsEqualTo(previous.NextCursor);
        await Assert.That(batch.Events.Count).IsLessThanOrEqualTo(DefaultAdapterReceiveEvents);
        await AssertCausedByAsync(batch, CreateOperation(sequence));
    }

    /// <summary>Returns whether a pending receive ends, by completion or cancellation, within the bounded wait.</summary>
    /// <param name="pending">The pending receive.</param>
    /// <returns><see langword="true"/> when the receive ended without delivering a batch.</returns>
    private static async Task<bool> EndsAfterCancellationAsync(Task<bool> pending)
    {
        try
        {
            return !await pending.WaitAsync(AwaitTimeout);
        }
        catch (OperationCanceledException)
        {
            return true;
        }
    }
}
