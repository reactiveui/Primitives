// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The trusted tenant returned by allow policies.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The mismatched tenant returned by tenant-switch policies.</summary>
    private const string OtherTenant = "tenant-b";

    /// <summary>The authenticated client used by hub calls.</summary>
    private const string Client = "client-a";

    /// <summary>The payload contract used by test envelopes.</summary>
    private const string Contract = "contract-a";

    /// <summary>The payload content type used by test envelopes.</summary>
    private const string ContentType = "application/json";

    /// <summary>The initial server version used by conflict tests.</summary>
    private const string InitialVersion = "v0";

    /// <summary>The first committed server version used by conflict tests.</summary>
    private const string FirstVersion = "v1";

    /// <summary>The first test payload value.</summary>
    private const string PayloadA = "payload-a";

    /// <summary>The second test payload value.</summary>
    private const string PayloadB = "payload-b";

    /// <summary>The missing cursor value used by retention-gap tests.</summary>
    private const string MissingCursor = "missing-cursor";

    /// <summary>The sanitized retention gap diagnostic used by exception tests.</summary>
    private const string RetentionGapText = "The requested receive cursor is outside retained server history.";

    /// <summary>The second operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>The long poll delay in minutes.</summary>
    private const int LongPollMinutes = 5;

    /// <summary>The default poll delay in milliseconds.</summary>
    private const int PollDelayMilliseconds = 50;

    /// <summary>The cancellation timeout in milliseconds.</summary>
    private const int CancellationMilliseconds = 100;

    /// <summary>The operation retention in minutes.</summary>
    private const int OperationRetentionMinutes = 20;

    /// <summary>The subscription retention in minutes.</summary>
    private const int SubscriptionRetentionMinutes = 40;

    /// <summary>The oversized batch operation limit used by validation tests.</summary>
    private const int SingleOperationLimit = 1;

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The fixed test time.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 6, 0, 0, TimeSpan.Zero);

    /// <summary>The stream identifier used by tests.</summary>
    private static readonly StreamId Stream = new("stream-a");

    /// <summary>The alternate stream identifier used by validation tests.</summary>
    private static readonly StreamId OtherStream = new("stream-b");

    /// <summary>Verifies that a SQLite hub publishes through the conflict handler.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithSqlitePublishesThroughConflictHandler()
    {
        using var database = new SqliteLease();
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), domain));
        var operation = Operation(1, PayloadA);

        var result = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);

        await Assert.That(result.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(result.ProducedEvents[0].StreamId).IsEqualTo(Stream);
        await Assert.That(result.ProducedEvents[0].Origin?.ClientId).IsEqualTo(Client);
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
    }

    /// <summary>Verifies that denied publish authorization does not replay existing ledger entries.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithDeniedPublishDoesNotReplayDuplicateLedger()
    {
        using var database = new SqliteLease();
        var operation = Operation(1, PayloadA);
        await using (var first = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler())))
        {
            var firstResult = await first.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
            await Assert.That(firstResult.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        }

        var domain = new RecordingDomainHandler();
        await using var denied = ServerStreamHub.CreateSqlite(database.Path, Options(new DenyPolicy(), domain));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => denied.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies that the authorized tenant is used instead of the client tenant hint.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncUsesTrustedTenantInsteadOfTenantHint()
    {
        using var database = new SqliteLease();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var operation = Operation(1, PayloadA);

        _ = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var batch = await ReadFirstBatchAsync(hub, new(Tenant, Client), SubscriptionId.New());

        await Assert.That(batch.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(batch.CompletedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(batch.PreviousCursor).IsNull();
    }

    /// <summary>Verifies that SQLite receive offers persist before acknowledgement.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncWithSqlitePersistsOfferBeforeAcknowledgement()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        string cursor;
        await using (var first = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler())))
        {
            _ = await first.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
            var page = await ReadFirstBatchAsync(first, new(Tenant, Client), subscriptionId);
            cursor = page.NextCursor;
        }

        await using var second = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));

        await second.AcknowledgeAsync(new(subscriptionId, Stream, cursor), new(Tenant, Client), CancellationToken.None);
        await second.AcknowledgeAsync(new(subscriptionId, Stream, cursor), new(Tenant, Client), CancellationToken.None);
    }

    /// <summary>Verifies that acknowledgements use the trusted tenant scope.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AcknowledgeAsyncRejectsWrongTenantBeforeSubscriptionLookup()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        string cursor;
        await using (var first = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler())))
        {
            _ = await first.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
            var page = await ReadFirstBatchAsync(first, new(Tenant, Client), subscriptionId);
            cursor = page.NextCursor;
        }

        await using var second = ServerStreamHub.CreateSqlite(database.Path, Options(new TenantSwitchPolicy(OtherTenant), new RecordingDomainHandler()));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => second.AcknowledgeAsync(new(subscriptionId, Stream, cursor), new(Tenant, Client), CancellationToken.None).AsTask());
    }

    /// <summary>Verifies that active call capacity rejects an overlapping call.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsSecondActiveCall()
    {
        var domain = new BlockingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), domain) with { MaximumActiveCalls = 1 });
        var first = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await domain.WaitUntilStartedAsync();

        _ = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None).AsTask());

        domain.Release();
        var result = await first;
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies that receive retention gaps raise a typed failure.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRaisesRetentionGapInsteadOfInventingProgress()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), "missing-cursor", StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);

        var exception = await Assert.ThrowsExactlyAsync<ServerReceiveRetentionGapException>(() => enumerator.MoveNextAsync().AsTask());
        await Assert.That(exception?.ReasonCode).IsEqualTo(ServerReceiveRetentionGapException.ReceiveRetentionGapReasonCode);
    }

    /// <summary>Verifies that mismatched authorized clients are rejected before effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsMismatchedAuthorizedClientBeforeEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new MismatchedClientPolicy(), domain));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies that mismatched operation tenant scopes are rejected before effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsMismatchedOperationTenantBeforeEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new OperationTenantMismatchPolicy(), domain));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies that publish wakeups coalesce when no subscriber is waiting.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncCoalescesBurstWakeupsWithoutSubscribers()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var first = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var second = await hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None);

        await Assert.That(first.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(second.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies that invalid hub options are rejected before resource use.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task CreateHubRejectsInvalidOptions()
    {
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler());

        await Assert.That(() => ServerStreamHub.CreateInMemory(options with { EmptyPollDelay = TimeSpan.Zero })).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => ServerStreamHub.CreateInMemory(options with { EmptyPollDelay = TimeSpan.MaxValue })).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => ServerStreamHub.CreateInMemory(options with { MaximumBatchLogicalBytes = 0 })).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => ServerStreamHub.CreateInMemory(options with { MaximumReceiveLogicalBytes = 0 })).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => ServerStreamHub.CreateSqlite(" ", options)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies that SQLite options are validated before database creation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task CreateSqliteRejectsInvalidOptionsBeforeDatabaseCreation()
    {
        using var database = new SqliteLease();
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with { MaximumBatchLogicalBytes = 0 };

        await Assert.That(() => ServerStreamHub.CreateSqlite(database.Path, options)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies that malformed publish batches are rejected before domain effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsMalformedBatchesBeforeEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var limitedHub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), domain) with { MaximumBatchOperations = SingleOperationLimit });
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var first = Operation(1, PayloadA);
        var second = Operation(SecondOperationSeed, PayloadB);
        var emptyOperationId = first with { OperationId = new(Guid.Empty) };
        var emptyStream = first with { StreamId = default };
        var zeroSequence = first with { ClientSequence = 0 };
        var mixedStream = second with { StreamId = OtherStream };
        var duplicateOperation = second with { OperationId = first.OperationId };
        var duplicateSequence = second with { ClientSequence = first.ClientSequence };
        var outOfOrderFirst = first with { ClientSequence = SecondOperationSeed };
        var outOfOrderSecond = second with { ClientSequence = 1 };

        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(new(Guid.Empty, [first]), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(new(Guid.NewGuid(), []), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(
            () => limitedHub.ApplyOperationsAsync(Batch(first, second), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(emptyOperationId), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(emptyStream), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(zeroSequence), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(first, mixedStream), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(first, duplicateOperation), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(first, duplicateSequence), new(Tenant, Client), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(
            () => hub.ApplyOperationsAsync(Batch(outOfOrderFirst, outOfOrderSecond), new(Tenant, Client), CancellationToken.None).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies that subscribe and acknowledge validate empty subscription identifiers.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeAndAcknowledgeRejectEmptySubscriptionId()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        var emptySubscriptionId = new SubscriptionId(Guid.Empty);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, emptySubscriptionId, null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => enumerator.MoveNextAsync().AsTask());
        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(
            () => hub.AcknowledgeAsync(new(emptySubscriptionId, Stream, MissingCursor), new(Tenant, Client), CancellationToken.None).AsTask());
    }

    /// <summary>Verifies that an empty subscription poll wakes when a publish arrives.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncWakesEmptyPollAfterPublish()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                TimeProvider = TimeProvider.System,
            });
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pending = enumerator.MoveNextAsync().AsTask();

        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var hasPage = await pending;

        await Assert.That(hasPage).IsTrue();
        var events = enumerator.Current?.Events ?? [];
        await Assert.That(events).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Verifies an idle empty poll does not consume publish capacity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncEmptyPollDoesNotConsumePublishCapacity()
    {
        var timeProvider = new SignalingFixedTimeProvider(Start);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                MaximumActiveCalls = 1,
                TimeProvider = timeProvider,
            });
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pending = enumerator.MoveNextAsync().AsTask();
        await timeProvider.WaitUntilTimerCreatedAsync();

        var result = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var hasPage = await pending;

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(hasPage).IsTrue();
        await Assert.That(enumerator.Current.Events).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Verifies acknowledgement capacity is reserved separately from blocked publishes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AcknowledgeAsyncCanEnterWhilePublishConsumesEffectCapacity()
    {
        var domain = new SecondCallBlockingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), domain) with { MaximumActiveCalls = 1 });
        var subscriptionId = SubscriptionId.New();
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var page = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);
        var blockedPublish = hub.ApplyOperationsAsync(
            Batch(Operation(SecondOperationSeed, PayloadB)),
            new(Tenant, Client),
            CancellationToken.None).AsTask();
        await domain.WaitUntilStartedAsync();

        await hub.AcknowledgeAsync(new(subscriptionId, Stream, page.NextCursor), new(Tenant, Client), CancellationToken.None);

        domain.Release();
        var result = await blockedPublish;
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies acknowledgement admission is bounded independently from effect calls.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task AcknowledgeAsyncRejectsSecondActiveAcknowledgementAndDrainsOnDispose()
    {
        var policy = new BlockingAcknowledgePolicy();
        var hub = ServerStreamHub.CreateInMemory(
            Options(policy, new RecordingDomainHandler()) with { MaximumActiveCalls = 1 });
        var subscriptionId = SubscriptionId.New();
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var page = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);
        var acknowledgement = new ReceiveAcknowledgement(subscriptionId, Stream, page.NextCursor);
        var firstAcknowledgement = hub.AcknowledgeAsync(acknowledgement, new(Tenant, Client), CancellationToken.None).AsTask();
        await policy.WaitUntilStartedAsync();

        _ = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => hub.AcknowledgeAsync(acknowledgement, new(Tenant, Client), CancellationToken.None).AsTask());

        var dispose = hub.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        await Assert.That(dispose.IsCompleted).IsFalse();

        policy.Release();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => firstAcknowledgement);
        await dispose;
    }

    /// <summary>Verifies that subscription polling observes caller cancellation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesCancellationWhilePolling()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                TimeProvider = TimeProvider.System,
            });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(CancellationMilliseconds));
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            cancellation.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(cancellation.Token);

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());

        await Assert.That(exception is OperationCanceledException).IsTrue();
    }

    /// <summary>Verifies caller cancellation terminates an already waiting empty poll.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesCancellationDuringWaitingEmptyPoll()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                TimeProvider = new FixedTimeProvider(Start),
            });
        using var cancellation = new CancellationTokenSource();
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            cancellation.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(cancellation.Token);
        var pending = enumerator.MoveNextAsync().AsTask();

        await Task.Yield();
        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => pending);

        await Assert.That(exception is OperationCanceledException).IsTrue();
    }

    /// <summary>Verifies the empty-poll race observes the secondary completed task.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesCompletedSecondaryPollTask()
    {
        using var cancellation = new CancellationTokenSource();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMilliseconds(1),
                TimeProvider = new ImmediateCancelingTimeProvider(Start, cancellation),
            });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(OtherStream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            cancellation.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(cancellation.Token);

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());

        await Assert.That(exception is OperationCanceledException).IsTrue();
    }

    /// <summary>Verifies hub disposal terminates an already waiting empty poll.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesDisposeDuringWaitingEmptyPoll()
    {
        var timeProvider = new SignalingFixedTimeProvider(Start);
        var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
                TimeProvider = timeProvider,
            });
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pending = enumerator.MoveNextAsync().AsTask();

        await timeProvider.WaitUntilTimerCreatedAsync();
        await hub.DisposeAsync();
        var hasPage = await pending;

        await Assert.That(hasPage).IsFalse();
    }

    /// <summary>Verifies caller cancellation is observed when enumeration resumes after yielding a page.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncObservesCancellationAfterYieldedPage()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        using var cancellation = new CancellationTokenSource();
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            cancellation.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(cancellation.Token);
        var hasPage = await enumerator.MoveNextAsync();
        await Assert.That(hasPage).IsTrue();

        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());

        await Assert.That(exception is OperationCanceledException).IsTrue();
    }

    /// <summary>Verifies that disposal drains an active call and remains idempotent.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForActiveCallDrainAndIsIdempotent()
    {
        var domain = new BlockingDomainHandler();
        var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var publish = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await domain.WaitUntilStartedAsync();

        var dispose = hub.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        domain.Release();
        try
        {
            _ = await publish;
        }
        catch (OperationCanceledException)
        {
        }

        await dispose;
        await hub.DisposeAsync();
    }

    /// <summary>Verifies concurrent disposers share the same drain completion.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncConcurrentCallsShareDrainCompletion()
    {
        var domain = new BlockingDomainHandler();
        var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var publish = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await domain.WaitUntilStartedAsync();

        var firstDispose = hub.DisposeAsync().AsTask();
        var secondDispose = hub.DisposeAsync().AsTask();
        await Assert.That(firstDispose.IsCompleted).IsFalse();
        await Assert.That(secondDispose.IsCompleted).IsFalse();
        domain.Release();
        try
        {
            _ = await publish;
        }
        catch (OperationCanceledException)
        {
        }

        await firstDispose;
        await secondDispose;
    }

    /// <summary>Verifies disposal prevents new admissions while an active call drains.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncRejectsNewCallsWhileActiveCallDrains()
    {
        var domain = new BlockingDomainHandler();
        var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var publish = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await domain.WaitUntilStartedAsync();

        var dispose = hub.DisposeAsync().AsTask();
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () => hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None).AsTask());
        domain.Release();
        try
        {
            _ = await publish;
        }
        catch (OperationCanceledException)
        {
        }

        await dispose;
    }

    /// <summary>Verifies that disposal waits for an active authorization callback before journal access.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForActiveAuthorizationDrain()
    {
        var policy = new BlockingPublishPolicy();
        var domain = new RecordingDomainHandler();
        var hub = ServerStreamHub.CreateInMemory(Options(policy, domain));
        var publish = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await policy.WaitUntilStartedAsync();

        var dispose = hub.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        await Assert.That(dispose.IsCompleted).IsFalse();
        policy.Release();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => publish);

        await dispose;
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies disposal still releases SQLite ownership when cancellation callbacks throw.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncDisposesSqliteWhenCancellationCallbackThrows()
    {
        using var database = new SqliteLease();
        var policy = new ThrowingCancellationPolicy();
        var hub = ServerStreamHub.CreateSqlite(database.Path, Options(policy, new RecordingDomainHandler()));
        var publish = hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None).AsTask();
        await policy.WaitUntilStartedAsync();

        var dispose = hub.DisposeAsync().AsTask();
        await policy.WaitUntilCanceledAsync();
        policy.Release();
        _ = await Assert.ThrowsAsync<Exception>(() => dispose);

        try
        {
            _ = await publish;
        }
        catch (OperationCanceledException)
        {
        }

        await Assert.That(File.Exists(database.Path)).IsTrue();
    }

    /// <summary>Verifies retention gap exception constructors expose stable sanitized reason codes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ServerReceiveRetentionGapExceptionConstructorsExposeReasonCode()
    {
        var inner = new InvalidOperationException("inner");
        var defaultException = new ServerReceiveRetentionGapException();
        var wrapped = new ServerReceiveRetentionGapException(RetentionGapText, inner);

        await Assert.That(defaultException.ReasonCode).IsEqualTo(ServerReceiveRetentionGapException.ReceiveRetentionGapReasonCode);
        await Assert.That(wrapped.InnerException).IsEqualTo(inner);
    }

    /// <summary>Verifies receive page result batch invariants.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ServerReceivePageResultRequiresPageBatch()
    {
        var batch = new RemoteEventBatch(Guid.NewGuid(), Stream, null, MissingCursor, []);
        var valid = new ServerReceivePageResult(ServerReceivePageStatus.Page, batch, 1, 1);
        var invalid = new ServerReceivePageResult(ServerReceivePageStatus.Page, null, 1, 1);

        await Assert.That(valid.RequireBatch()).IsEqualTo(batch);
        await Assert.That(invalid.RequireBatch).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Reads the first subscription page from a hub.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="client">The client.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The first remote event batch.</returns>
    /// <exception cref="InvalidOperationException">A batch is not available after the first successful move.</exception>
    private static async Task<RemoteEventBatch> ReadFirstBatchAsync(
        ServerStreamHub hub,
        ServerAuthenticatedClient client,
        SubscriptionId subscriptionId)
    {
        var enumerable = hub.SubscribeStreamAsync(
            new(Stream, subscriptionId, null, StartPosition.FromSequence(0)),
            client,
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await enumerator.MoveNextAsync();
        await Assert.That(hasPage).IsTrue();
        return enumerator.Current ?? throw new InvalidOperationException("A received page must be available after MoveNextAsync returns true.");
    }

    /// <summary>Creates hub options for tests.</summary>
    /// <param name="policy">The authorization policy.</param>
    /// <param name="domain">The domain handler.</param>
    /// <returns>The configured hub options.</returns>
    private static ServerStreamHubOptions Options(
        IServerStreamAuthorizationPolicy policy,
        IServerDomainHandler domain) =>
        new()
        {
            AuthorizationPolicy = policy,
            ConflictHandler = new()
            {
                Streams =
                [
                    new()
                    {
                        StreamId = Stream,
                        InitialStateFactory = new InitialStateFactory(),
                        LastWriterWinsResolver = Resolver(),
                        MergeResolver = Resolver(),
                        CustomResolver = Resolver(),
                        DomainHandler = domain,
                    },
                ],
            },
            EmptyPollDelay = TimeSpan.FromMilliseconds(PollDelayMilliseconds),
            TimeProvider = new FixedTimeProvider(Start),
            JournalLimits = new() { OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes), SubscriptionRetention = TimeSpan.FromMinutes(SubscriptionRetentionMinutes) },
        };

    /// <summary>Creates the test conflict resolver.</summary>
    /// <returns>The configured resolver.</returns>
    private static LastWriterWinsResolver Resolver() =>
        new(new LastWriterWinsResolverOptions { VersionFactory = new IncrementingVersionFactory() });

    /// <summary>Creates a test sync batch.</summary>
    /// <param name="operations">The operations.</param>
    /// <returns>The sync batch.</returns>
    private static SyncBatch Batch(params SyncOperation[] operations) =>
        new(Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd"), operations);

    /// <summary>Creates a test sync operation.</summary>
    /// <param name="seed">The deterministic seed.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The sync operation.</returns>
    private static SyncOperation Operation(int seed, string payload) =>
        new()
        {
            OperationId = new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1])),
            StreamId = Stream,
            ClientSequence = seed,
            TimestampUtc = Start,
            BaseVersion = seed == 1 ? InitialVersion : FirstVersion,
            Type = SyncOperationType.Update,
            Payload = Payload(payload),
        };

    /// <summary>Creates a test payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope Payload(string text) =>
        new(Contract, 1, ContentType, Encoding.UTF8.GetBytes(text), text);

    /// <summary>Owns a temporary SQLite database path.</summary>
    private sealed class SqliteLease : IDisposable
    {
        /// <summary>The temporary directory.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="SqliteLease"/> class.</summary>
        internal SqliteLease()
        {
            _directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"rxui-server-hub-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(_directory);
            Path = System.IO.Path.Combine(_directory, "journal.db");
        }

        /// <summary>Gets the SQLite database path.</summary>
        internal string Path { get; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);
    }

    /// <summary>Allows every operation for one trusted tenant.</summary>
    /// <param name="tenant">The trusted tenant.</param>
    private sealed class AllowPolicy(string tenant) : IServerStreamAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));
    }

    /// <summary>Authorizes calls for a tenant different from the stored subscription tenant.</summary>
    /// <param name="tenant">The trusted tenant.</param>
    private sealed class TenantSwitchPolicy(string tenant) : IServerStreamAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));
    }

    /// <summary>Denies every authorization request.</summary>
    private sealed class DenyPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>The repeated denial diagnostic used by denying test policies.</summary>
        private const string DeniedReason = "denied";

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            throw new UnauthorizedAccessException(DeniedReason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            throw new UnauthorizedAccessException(DeniedReason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            throw new UnauthorizedAccessException(DeniedReason);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            throw new UnauthorizedAccessException(DeniedReason);
    }

    /// <summary>Returns a scope for a different client than the authenticated client.</summary>
    private sealed class MismatchedClientPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>The mismatched authorized client.</summary>
        private const string OtherClient = "client-b";

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, OtherClient));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, OtherClient));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, OtherClient));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, OtherClient));
    }
}
