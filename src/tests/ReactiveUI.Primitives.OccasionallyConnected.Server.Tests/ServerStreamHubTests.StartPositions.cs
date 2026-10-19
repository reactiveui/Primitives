// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Initial position tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The client that belongs to <see cref="OtherTenant"/>.</summary>
    private const string OtherTenantClient = "client-b";

    /// <summary>The number of operations published by start-position tests.</summary>
    private const int StartPositionOperationCount = 3;

    /// <summary>The server event sequence of the second published event.</summary>
    private const long SecondEventSequence = 2;

    /// <summary>The operation index of the third published event.</summary>
    private const int ThirdEventIndex = 2;

    /// <summary>Verifies a latest subscription skips existing history and delivers only later events.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncLatestDeliversOnlyEventsPublishedAfterRegistration()
    {
        var timeProvider = new SignalingFixedTimeProvider(Start);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                TimeProvider = timeProvider,
            });
        var operations = UnconditionalOperations(StartPositionOperationCount, PayloadA);
        await PublishEachAsync(hub, operations[..^1]);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.Latest),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pending = NextPageAsync(enumerator);
        await timeProvider.WaitUntilTimerCreatedAsync().WaitAsync(MoveTimeout);

        await PublishEachAsync(hub, operations[^1..]);
        var page = await pending;

        await Assert.That(page.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(page.Events[0].CausedByOperationId).IsEqualTo(operations[^1].OperationId);
    }

    /// <summary>Verifies a sequence start position begins at the requested server event.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncFromSequenceStartsAtRequestedEvent()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var operations = UnconditionalOperations(StartPositionOperationCount, PayloadA);
        await PublishEachAsync(hub, operations);

        var page = await ReadFirstPageAsync(hub, StartPosition.FromSequence(SecondEventSequence));

        await AssertEventOperationsAsync(page, operations[1..]);
    }

    /// <summary>Verifies a timestamp start position begins at the first event committed at or after the timestamp.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncFromTimestampStartsAtFirstInclusiveCommit()
    {
        var clock = new MutableSnapshotTimeProvider(Start);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with { TimeProvider = clock });
        var operations = UnconditionalOperations(StartPositionOperationCount, PayloadA);
        for (var index = 0; index < operations.Length; index++)
        {
            clock.SetUtcNow(Start.AddMinutes(index));
            await PublishEachAsync(hub, operations[index..(index + 1)]);
        }

        var page = await ReadFirstPageAsync(hub, StartPosition.FromTimestamp(Start.AddMinutes(1)));

        await AssertEventOperationsAsync(page, operations[1..]);
        await Assert.That(page.Events[0].CommittedAtUtc).IsEqualTo(Start.AddMinutes(1));
    }

    /// <summary>Verifies a cursor start position continues after the group named by a canonical cursor.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncFromCursorContinuesAfterCanonicalCursor()
    {
        await using var hub = ServerStreamHub.CreateInMemory(SingleEventPageOptions());
        var operations = UnconditionalOperations(StartPositionOperationCount, PayloadA);
        await PublishEachAsync(hub, operations);
        var firstPage = await ReadFirstPageAsync(hub, StartPosition.FromSequence(0));

        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromCursor(firstPage.NextCursor)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var second = await NextPageAsync(enumerator);
        var third = await NextPageAsync(enumerator);

        await AssertEventOperationsAsync(firstPage, operations[..1]);
        await AssertEventOperationsAsync(second, operations[1..ThirdEventIndex]);
        await AssertEventOperationsAsync(third, operations[ThirdEventIndex..]);
        await Assert.That(third.PreviousCursor).IsEqualTo(second.NextCursor);
    }

    /// <summary>Verifies a cursor issued to another tenant is rejected without delivering its events.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsOtherTenantCursorWithoutLeakingEvents()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new ClientTenantPolicy(), new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadB) with { BaseVersion = null }), new(OtherTenant, OtherTenantClient), CancellationToken.None);
        var foreign = await ReadFirstBatchAsync(hub, new(OtherTenant, OtherTenantClient), SubscriptionId.New());

        await AssertForeignCursorRejectedAsync(hub, foreign.NextCursor);
    }

    /// <summary>Verifies a cursor issued for another stream is rejected without delivering its events.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsOtherStreamCursorWithoutLeakingEvents()
    {
        await using var hub = ServerStreamHub.CreateInMemory(TwoStreamOptions());
        var otherOperation = Operation(1, PayloadB) with { StreamId = OtherStream, BaseVersion = null };
        _ = await hub.ApplyOperationsAsync(Batch(otherOperation), new(Tenant, Client), CancellationToken.None);
        var otherEnumerable = hub.SubscribeStreamAsync(
            new(OtherStream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var otherEnumerator = otherEnumerable.GetAsyncEnumerator(CancellationToken.None);
        var foreign = await NextPageAsync(otherEnumerator);

        await AssertForeignCursorRejectedAsync(hub, foreign.NextCursor);
    }

    /// <summary>Verifies a canonical cursor from another store that is ahead of this stream is rejected without inventing progress.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsCursorAheadOfRetainedStream()
    {
        string staleCursor;
        await using (var source = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler())))
        {
            await PublishEachAsync(source, UnconditionalOperations(StartPositionOperationCount, PayloadB));
            staleCursor = (await ReadFirstBatchAsync(source, new(Tenant, Client), SubscriptionId.New())).NextCursor;
        }

        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA) with { BaseVersion = null }), new(Tenant, Client), CancellationToken.None);

        RemoteSubscribeRequest resume = new(Stream, SubscriptionId.New(), staleCursor, StartPosition.FromSequence(0));
        RemoteSubscribeRequest start = new(Stream, SubscriptionId.New(), null, StartPosition.FromCursor(staleCursor));
        var resumeException = await CaptureSubscribeFailureAsync(hub, resume);
        var startException = await CaptureSubscribeFailureAsync(hub, start);

        await AssertSanitizedRetentionGapAsync(resumeException, resume);
        await AssertSanitizedRetentionGapAsync(startException, start);
    }

    /// <summary>
    /// Asserts a cursor rejection is the Core retention gap that echoes only the caller's own request and the fixed
    /// reason code, so expired, ahead and foreign cursors stay indistinguishable.
    /// </summary>
    /// <param name="exception">The captured exception.</param>
    /// <param name="request">The rejected request.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertSanitizedRetentionGapAsync(Exception exception, RemoteSubscribeRequest request)
    {
        await Assert.That(exception).IsTypeOf<RemoteSubscriptionRetentionGapException>();
        var gap = (RemoteSubscriptionRetentionGapException)exception;
        await Assert.That(gap.ReasonCode).IsEqualTo(ServerReceiveRetentionGapException.ReceiveRetentionGapReasonCode);
        await Assert.That(gap.StreamId).IsEqualTo(request.StreamId);
        await Assert.That(gap.SubscriptionId).IsEqualTo(request.SubscriptionId);
        await Assert.That(gap.ExpiredCursor).IsEqualTo(request.Cursor);
        await Assert.That(exception.Message).IsEqualTo(RetentionGapText);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Asserts a foreign cursor is rejected both as a resume cursor and as an initial position.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="foreignCursor">The cursor issued outside the caller's stream scope.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertForeignCursorRejectedAsync(ServerStreamHub hub, string foreignCursor)
    {
        RemoteSubscribeRequest resume = new(Stream, SubscriptionId.New(), foreignCursor, StartPosition.FromSequence(0));
        RemoteSubscribeRequest start = new(Stream, SubscriptionId.New(), null, StartPosition.FromCursor(foreignCursor));
        var resumeException = await CaptureSubscribeFailureAsync(hub, resume);
        var startException = await CaptureSubscribeFailureAsync(hub, start);

        await AssertSanitizedRetentionGapAsync(resumeException, resume);
        await AssertSanitizedRetentionGapAsync(startException, start);
    }

    /// <summary>Captures the failure raised by the first move of a subscription.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="request">The subscribe request.</param>
    /// <returns>The raised exception.</returns>
    /// <exception cref="InvalidOperationException">The subscription yielded a page instead of failing.</exception>
    private static async Task<Exception> CaptureSubscribeFailureAsync(ServerStreamHub hub, RemoteSubscribeRequest request)
    {
        var enumerable = hub.SubscribeStreamAsync(request, new(Tenant, Client), CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var exception = await Assert.ThrowsAsync<Exception>(() => NextPageAsync(enumerator));
        await Assert.That(exception).IsNotTypeOf<TimeoutException>();
        return exception ?? throw new InvalidOperationException("The subscription delivered a page for a rejected cursor.");
    }

    /// <summary>Reads the first page for a new subscription with the requested initial position.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="position">The initial position.</param>
    /// <returns>The first page.</returns>
    private static async Task<RemoteEventBatch> ReadFirstPageAsync(ServerStreamHub hub, StartPosition position)
    {
        var enumerable = hub.SubscribeStreamAsync(new(Stream, SubscriptionId.New(), null, position), new(Tenant, Client), CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        return await NextPageAsync(enumerator);
    }

    /// <summary>Asserts a page carries exactly the events caused by the expected operations.</summary>
    /// <param name="page">The page.</param>
    /// <param name="expected">The expected causing operations in order.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertEventOperationsAsync(RemoteEventBatch page, SyncOperation[] expected)
    {
        await Assert.That(page.Events).Count().IsEqualTo(expected.Length);
        for (var index = 0; index < expected.Length; index++)
        {
            await Assert.That(page.Events[index].CausedByOperationId).IsEqualTo(expected[index].OperationId);
        }
    }

    /// <summary>Creates hub options with conflict registrations for two streams.</summary>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions TwoStreamOptions()
    {
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler());
        var registration = options.ConflictHandler.Streams[0];
        return options with
        {
            ConflictHandler = options.ConflictHandler with { Streams = [registration, registration with { StreamId = OtherStream }] },
        };
    }

    /// <summary>Authorizes each client for the tenant named by its trusted tenant hint.</summary>
    private sealed class ClientTenantPolicy : IServerStreamAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }
}
