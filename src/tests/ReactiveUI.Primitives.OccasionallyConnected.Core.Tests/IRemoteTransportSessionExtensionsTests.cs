// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IRemoteTransportSessionExtensions"/>.</summary>
public sealed class IRemoteTransportSessionExtensionsTests
{
    /// <summary>The expected token count.</summary>
    private const int TokenCount = 3;

    /// <summary>Verifies the session overloads forward their arguments and cancellation token once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConvenienceOverloadsForwardArgumentsOnceAndReturnResults()
    {
        var session = new RecordingSession();
        var operation = CreateOperation();
        var batch = new SyncBatch(Guid.NewGuid(), [operation]);
        var request = new RemoteSubscribeRequest(operation.StreamId, SubscriptionId.New(), "cursor", StartPosition.Latest);
        var acknowledgement = new ReceiveAcknowledgement(request.SubscriptionId, operation.StreamId, "next");
        var pushed = await session.PushAsync(batch);
        var received = new List<RemoteEventBatch>();
        await foreach (var item in session.SubscribeAsync(request))
        {
            received.Add(item);
        }

        await session.AcknowledgeAsync(acknowledgement);
        await Assert.That(pushed.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(received).Count().IsEqualTo(1);
        await Assert.That(session.PushCalls).IsEqualTo(1);
        await Assert.That(session.PushedBatch).IsEqualTo(batch);
        await Assert.That(session.SubscribeCalls).IsEqualTo(1);
        await Assert.That(session.SubscribeRequest).IsEqualTo(request);
        await Assert.That(session.AcknowledgeCalls).IsEqualTo(1);
        await Assert.That(session.Acknowledgement).IsEqualTo(acknowledgement);
        foreach (var token in session.Tokens)
        {
            await Assert.That(token).IsEqualTo(CancellationToken.None);
        }

        await Assert.That(session.Tokens).Count().IsEqualTo(TokenCount);
    }

    /// <summary>Verifies the push overload propagates session failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PushAsyncPropagatesSessionFailure()
    {
        var error = new InvalidOperationException("failure");
        var session = new RecordingSession { Error = error };
        Func<Task> action = async () => await session.PushAsync(new(Guid.NewGuid(), []));
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(session.PushCalls).IsEqualTo(1);
    }

    /// <summary>Creates a synchronization operation for forwarding assertions.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = new("stream"),
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = new("contract", 1, "json", ReadOnlyMemory<byte>.Empty, "hash"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Records remote transport session calls.</summary>
    private sealed class RecordingSession : IRemoteTransportSession
    {
        /// <summary>Gets the recorded cancellation tokens.</summary>
        public List<CancellationToken> Tokens { get; } = [];

        /// <summary>Gets the push call count.</summary>
        public int PushCalls { get; private set; }

        /// <summary>Gets the subscribe call count.</summary>
        public int SubscribeCalls { get; private set; }

        /// <summary>Gets the acknowledge call count.</summary>
        public int AcknowledgeCalls { get; private set; }

        /// <summary>Gets the pushed batch.</summary>
        public SyncBatch? PushedBatch { get; private set; }

        /// <summary>Gets the subscribe request.</summary>
        public RemoteSubscribeRequest? SubscribeRequest { get; private set; }

        /// <summary>Gets the acknowledgement.</summary>
        public ReceiveAcknowledgement? Acknowledgement { get; private set; }

        /// <summary>Gets the exception to throw from push.</summary>
        public Exception? Error { get; init; }

        /// <inheritdoc />
        public NegotiatedCapabilities NegotiatedCapabilities =>
            new(new Version(1, 0), RemoteTransportCapabilities.None, 1, 1, TimeSpan.Zero, TimeSpan.Zero);

        /// <inheritdoc />
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            PushCalls++;
            PushedBatch = batch;
            Tokens.Add(cancellationToken);
            return Error is null
                ? new(new RemoteSyncResult(batch.BatchId, [], null, null))
                : ValueTask.FromException<RemoteSyncResult>(Error);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            SubscribeCalls++;
            SubscribeRequest = request;
            Tokens.Add(cancellationToken);
            return Batches(request.StreamId);
        }

        /// <inheritdoc />
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken)
        {
            AcknowledgeCalls++;
            Acknowledgement = acknowledgement;
            Tokens.Add(cancellationToken);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Creates remote event batches for a stream.</summary>
        /// <param name="stream">The stream identifier.</param>
        /// <returns>The remote event batches.</returns>
        private static async IAsyncEnumerable<RemoteEventBatch> Batches(StreamId stream)
        {
            await Task.CompletedTask;
            yield return new(Guid.NewGuid(), stream, null, "next", []);
        }
    }
}
