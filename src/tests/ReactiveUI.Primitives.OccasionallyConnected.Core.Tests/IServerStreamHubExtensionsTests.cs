// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IServerStreamHubExtensions"/>.</summary>
public sealed partial class IServerStreamHubExtensionsTests
{
    /// <summary>The tenant identifier used by tests.</summary>
    private const string TenantId = "tenant";

    /// <summary>The client identifier used by tests.</summary>
    private const string ClientId = "client";

    /// <summary>The cursor value used by tests.</summary>
    private const string Cursor = "cursor";

    /// <summary>The expected number of returned batches.</summary>
    private const int ExpectedBatchCount = 2;

    /// <summary>The stream name used by tests.</summary>
    private const string StreamName = "stream";

    /// <summary>Verifies convenience overloads forward their exact arguments and returned values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConvenienceOverloadsForwardExactArgumentsAndResults()
    {
        var hub = new RecordingHub();
        var batch = new SyncBatch(Guid.NewGuid(), []);
        var request = new RemoteSubscribeRequest(new(StreamName), SubscriptionId.New(), Cursor, StartPosition.Latest);
        var acknowledgement = new ReceiveAcknowledgement(request.SubscriptionId, request.StreamId, "next");
        var client = new ServerAuthenticatedClient(TenantId, ClientId);

        var result = await hub.ApplyOperationsAsync(batch, client);
        var enumerable = hub.SubscribeStreamAsync(request, client);
        var received = new List<RemoteEventBatch>();
        await foreach (var item in enumerable)
        {
            received.Add(item);
        }

        await hub.AcknowledgeAsync(acknowledgement, client);
        await Assert.That(result).IsSameReferenceAs(hub.ApplyResult);
        await Assert.That(enumerable).IsSameReferenceAs(hub.SubscribeResult);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
        await Assert.That(hub.ApplyBatch).IsSameReferenceAs(batch);
        await Assert.That(hub.ApplyClient).IsSameReferenceAs(client);
        await Assert.That(hub.SubscribeRequest).IsSameReferenceAs(request);
        await Assert.That(hub.SubscribeClient).IsSameReferenceAs(client);
        await Assert.That(hub.Acknowledgement).IsSameReferenceAs(acknowledgement);
        await Assert.That(hub.AcknowledgeClient).IsSameReferenceAs(client);
        await Assert.That(hub.ApplyToken).IsEqualTo(CancellationToken.None);
        await Assert.That(hub.SubscribeToken).IsEqualTo(CancellationToken.None);
        await Assert.That(hub.AcknowledgeToken).IsEqualTo(CancellationToken.None);
        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(1);
        await Assert.That(received).Count().IsEqualTo(ExpectedBatchCount);
        await Assert.That(received[0]).IsSameReferenceAs(hub.FirstBatch);
        await Assert.That(received[1]).IsSameReferenceAs(hub.SecondBatch);
    }

    /// <summary>Verifies the apply overload propagates hub failures without wrapping them.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyOperationsAsyncPropagatesHubFailure()
    {
        var error = new InvalidOperationException("apply failure");
        var hub = new RecordingHub { ApplyError = error };
        Func<Task> action = async () => await hub.ApplyOperationsAsync(new(Guid.NewGuid(), []), new(TenantId, ClientId));

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
    }

    /// <summary>Verifies acknowledgement completion waits for durable persistence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AcknowledgeAsyncWaitsForHubCompletion()
    {
        var hub = new RecordingHub { AcknowledgeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        var acknowledgement = new ReceiveAcknowledgement(SubscriptionId.New(), new(StreamName), Cursor);
        var client = new ServerAuthenticatedClient(TenantId, ClientId);
        var completion = hub.AcknowledgeAsync(acknowledgement, client).AsTask();

        await Assert.That(completion.IsCompleted).IsFalse();
        await Assert.That(hub.Acknowledgement).IsSameReferenceAs(acknowledgement);
        await Assert.That(hub.AcknowledgeClient).IsSameReferenceAs(client);
        hub.AcknowledgeCompletion.SetResult();
        await completion;
    }

    /// <summary>Verifies acknowledgement failures are propagated without wrapping them.</summary>
    /// <param name="deferred">Whether the hub fails after returning its pending operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task AcknowledgeAsyncPropagatesHubFailure(bool deferred)
    {
        var error = new InvalidOperationException("acknowledgement failure");
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub { AcknowledgeError = deferred ? null : error, AcknowledgeCompletion = deferred ? source : null };
        var completion = hub.AcknowledgeAsync(
            new(SubscriptionId.New(), new(StreamName), Cursor),
            new(TenantId, ClientId)).AsTask();
        if (deferred)
        {
            await Assert.That(completion.IsCompleted).IsFalse();
            source.SetException(error);
        }

        var thrown = await Assert.That(completion).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies a deferred canceled hub operation remains canceled through the convenience overload.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AcknowledgeAsyncPreservesDeferredCancellation()
    {
        using var cancellationSource = new CancellationTokenSource();
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub { AcknowledgeCompletion = source };
        var acknowledgement = new ReceiveAcknowledgement(SubscriptionId.New(), new(StreamName), Cursor);
        var client = new ServerAuthenticatedClient(TenantId, ClientId);
        var completion = hub.AcknowledgeAsync(acknowledgement, client).AsTask();
        await Assert.That(completion.IsCompleted).IsFalse();
        await cancellationSource.CancelAsync();
        source.SetCanceled(cancellationSource.Token);
        var thrown = await Assert.That(completion).ThrowsExactly<TaskCanceledException>();

        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(1);
        await Assert.That(hub.Acknowledgement).IsSameReferenceAs(acknowledgement);
        await Assert.That(hub.AcknowledgeClient).IsSameReferenceAs(client);
        await Assert.That(hub.AcknowledgeToken).IsEqualTo(CancellationToken.None);
        await Assert.That(thrown?.CancellationToken).IsEqualTo(cancellationSource.Token);
    }

    /// <summary>Verifies the subscribe overload propagates hub failures without wrapping them.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeStreamAsyncPropagatesHubFailure()
    {
        var error = new InvalidOperationException("subscribe failure");
        var hub = new RecordingHub { SubscribeError = error };
        Action action = () => hub.SubscribeStreamAsync(CreateRequest(), new(TenantId, ClientId));

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies failures during stream enumeration are propagated without wrapping them.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeStreamAsyncPropagatesEnumerationFailure()
    {
        var error = new InvalidOperationException("enumeration failure");
        var hub = new RecordingHub { EnumerationError = error };
        var enumerable = hub.SubscribeStreamAsync(CreateRequest(), new(TenantId, ClientId));
        Func<Task> action = async () =>
        {
            await foreach (var _ in enumerable)
            {
                await Task.CompletedTask;
            }
        };

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
    }

    /// <summary>Creates a remote subscription request.</summary>
    /// <returns>A remote subscription request.</returns>
    private static RemoteSubscribeRequest CreateRequest() => new(new(StreamName), SubscriptionId.New(), null, StartPosition.Latest);

    /// <summary>Records server stream hub calls.</summary>
    private sealed class RecordingHub : IServerStreamHub
    {
        /// <summary>Initializes a new instance of the <see cref="RecordingHub"/> class.</summary>
        public RecordingHub() => SubscribeResult = Batches();

        /// <summary>Gets the first result batch.</summary>
        public RemoteEventBatch FirstBatch { get; } = new(Guid.NewGuid(), new(StreamName), null, "one", []);

        /// <summary>Gets the second result batch.</summary>
        public RemoteEventBatch SecondBatch { get; } = new(Guid.NewGuid(), new(StreamName), "one", "two", []);

        /// <summary>Gets the result returned by apply.</summary>
        public ServerSyncResult ApplyResult { get; } = new(new(Guid.NewGuid(), [], null, null), []);

        /// <summary>Gets the stream returned by subscribe.</summary>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeResult { get; }

        /// <summary>Gets the number of apply calls.</summary>
        public int ApplyCalls { get; private set; }

        /// <summary>Gets the number of subscribe calls.</summary>
        public int SubscribeCalls { get; private set; }

        /// <summary>Gets the number of acknowledgement calls.</summary>
        public int AcknowledgeCalls { get; private set; }

        /// <summary>Gets the applied batch.</summary>
        public SyncBatch? ApplyBatch { get; private set; }

        /// <summary>Gets the client supplied to apply.</summary>
        public ServerAuthenticatedClient? ApplyClient { get; private set; }

        /// <summary>Gets the subscription request.</summary>
        public RemoteSubscribeRequest? SubscribeRequest { get; private set; }

        /// <summary>Gets the client supplied to subscribe.</summary>
        public ServerAuthenticatedClient? SubscribeClient { get; private set; }

        /// <summary>Gets the acknowledgement supplied to the hub.</summary>
        public ReceiveAcknowledgement? Acknowledgement { get; private set; }

        /// <summary>Gets the client supplied to acknowledge.</summary>
        public ServerAuthenticatedClient? AcknowledgeClient { get; private set; }

        /// <summary>Gets the apply cancellation token.</summary>
        public CancellationToken ApplyToken { get; private set; }

        /// <summary>Gets the subscribe cancellation token.</summary>
        public CancellationToken SubscribeToken { get; private set; }

        /// <summary>Gets the acknowledgement cancellation token.</summary>
        public CancellationToken AcknowledgeToken { get; private set; }

        /// <summary>Gets the apply exception.</summary>
        public Exception? ApplyError { get; init; }

        /// <summary>Gets the exception to throw from acknowledge.</summary>
        public Exception? AcknowledgeError { get; init; }

        /// <summary>Gets the completion source used to delay acknowledgement persistence.</summary>
        public TaskCompletionSource? AcknowledgeCompletion { get; init; }

        /// <summary>Gets the subscribe exception.</summary>
        public Exception? SubscribeError { get; init; }

        /// <summary>Gets the enumeration exception.</summary>
        public Exception? EnumerationError { get; init; }

        /// <inheritdoc />
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            ApplyCalls++;
            ApplyBatch = batch;
            ApplyClient = client;
            ApplyToken = cancellationToken;
            return ApplyError is null ? new(ApplyResult) : ValueTask.FromException<ServerSyncResult>(ApplyError);
        }

        /// <inheritdoc />
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            SubscribeCalls++;
            SubscribeRequest = request;
            SubscribeClient = client;
            SubscribeToken = cancellationToken;
            if (SubscribeError is not null)
            {
                throw SubscribeError;
            }

            return SubscribeResult;
        }

        /// <inheritdoc />
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            AcknowledgeCalls++;
            Acknowledgement = acknowledgement;
            AcknowledgeClient = client;
            AcknowledgeToken = cancellationToken;
            if (AcknowledgeError is not null)
            {
                return ValueTask.FromException(AcknowledgeError);
            }

            return AcknowledgeCompletion is null ? ValueTask.CompletedTask : new(AcknowledgeCompletion.Task);
        }

        /// <summary>Returns the recorded remote batches.</summary>
        /// <returns>The recorded remote batches.</returns>
        private async IAsyncEnumerable<RemoteEventBatch> Batches()
        {
            yield return FirstBatch;
            await Task.CompletedTask;

            if (EnumerationError is not null)
            {
                throw EnumerationError;
            }

            yield return SecondBatch;
        }
    }
}
