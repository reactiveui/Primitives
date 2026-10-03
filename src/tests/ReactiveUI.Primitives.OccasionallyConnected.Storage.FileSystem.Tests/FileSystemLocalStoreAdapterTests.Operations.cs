// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Additional state-transition and validation tests for the filesystem adapter.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>The local client identifier used by the tests.</summary>
    private const string ClientIdentity = "client";

    /// <summary>A remote client identifier that does not match the local client.</summary>
    private const string RemoteClientIdentity = "another-client";

    /// <summary>The first cursor used by remote batch tests.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The second cursor used by remote batch tests.</summary>
    private const string SecondCursor = "cursor-2";

    /// <summary>The initial operation sequence.</summary>
    private const int FirstSequence = 0;

    /// <summary>The second operation sequence.</summary>
    private const int SecondSequence = 1;

    /// <summary>The third operation sequence.</summary>
    private const int ThirdSequence = 2;

    /// <summary>The fourth operation sequence.</summary>
    private const int FourthSequence = 3;

    /// <summary>The initial snapshot revision.</summary>
    private const int InitialRevision = 0;

    /// <summary>The first committed snapshot revision.</summary>
    private const int FirstRevision = 1;

    /// <summary>The second committed snapshot revision.</summary>
    private const int SecondRevision = 2;

    /// <summary>The third committed snapshot revision.</summary>
    private const int ThirdRevision = 3;

    /// <summary>The fourth committed snapshot revision.</summary>
    private const int FourthRevision = 4;

    /// <summary>The fifth committed snapshot revision.</summary>
    private const int FifthRevision = 5;

    /// <summary>The number of outbox operations leased by the sync-result test.</summary>
    private const int LeaseLimit = 4;

    /// <summary>The payload byte limit for test leases.</summary>
    private const int PayloadLimit = 1024;

    /// <summary>The serialized snapshot format version used by the tests.</summary>
    private const int SnapshotFormatVersion = 1;

    /// <summary>The snapshot payload used in precondition assertions.</summary>
    private const string SnapshotPayloadName = "snapshot";

    /// <summary>Verifies identity and local commit preconditions reject invalid state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task IdentityAndCommitPreconditionsRejectInvalidState()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("preconditions");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await Assert.That(() => adapter.GetOrCreateSubscriptionIdAsync(
                stream,
                CreateSubscriptionId(Guid.Empty),
                CancellationToken.None).AsTask()).Throws<ArgumentException>();
            await adapter.InitializeAsync(new(ClientIdentity, 1, false), CancellationToken.None);

            await Assert.That(() => IgnoreResultAsync(adapter.RecoverStreamAsync(
                stream,
                CreateSubscriptionId(Guid.NewGuid()),
                CancellationToken.None))).Throws<InvalidOperationException>();

            var operation = CreateOperation(stream, InitialRevision);
            await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
                operation,
                CreateSnapshotMutation(stream, SnapshotPayloadName, InitialRevision),
                CancellationToken.None))).Throws<InvalidOperationException>();

            var subscription = await adapter.GetOrCreateSubscriptionIdAsync(
                stream,
                null,
                CancellationToken.None);
            await Assert.That(() => IgnoreResultAsync(adapter.GetOrCreateSubscriptionIdAsync(
                stream,
                CreateSubscriptionId(Guid.NewGuid()),
                CancellationToken.None))).Throws<InvalidOperationException>();
            await Assert.That(() => IgnoreResultAsync(adapter.RecoverStreamAsync(
                stream,
                CreateSubscriptionId(Guid.NewGuid()),
                CancellationToken.None))).Throws<InvalidOperationException>();

            _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, subscription, CancellationToken.None);
            await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
                operation with { ClientSequence = 1 },
                CreateSnapshotMutation(stream, SnapshotPayloadName, InitialRevision),
                CancellationToken.None))).Throws<InvalidOperationException>();
            await adapter.CommitLocalOperationAsync(
                operation,
                CreateSnapshotMutation(stream, SnapshotPayloadName, InitialRevision),
                CancellationToken.None);
            await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
                CreateOperation(stream, 1),
                CreateSnapshotMutation(stream, SnapshotPayloadName, InitialRevision),
                CancellationToken.None))).Throws<InvalidOperationException>();
            await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
                CreateOperation(CreateStreamId("different"), InitialRevision),
                CreateSnapshotMutation(stream, SnapshotPayloadName, InitialRevision),
                CancellationToken.None))).Throws<ArgumentException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies leases map accepted, conflicted, rejected, and retryable results correctly.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SyncResultsUpdateOperationAndSnapshotStates()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("sync-results");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(ClientIdentity, 1, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);

            var accepted = CreateOperation(stream, FirstSequence);
            var conflicted = CreateOperation(stream, SecondSequence);
            var rejected = CreateOperation(stream, ThirdSequence);
            var retryable = CreateOperation(stream, FourthSequence);
            _ = await adapter.CommitLocalOperationAsync(accepted, CreateSnapshotMutation(stream, "s0", InitialRevision), CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(conflicted, CreateSnapshotMutation(stream, "s1", FirstRevision), CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(rejected, CreateSnapshotMutation(stream, "s2", SecondRevision), CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(retryable, CreateSnapshotMutation(stream, "s3", ThirdRevision), CancellationToken.None);

            await using var leases = adapter.LeasePendingOperationsAsync(
                new(stream, LeaseLimit, PayloadLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await leases.MoveNextAsync()).IsTrue();
            var lease = leases.Current;
            var changed = await adapter.ApplySyncResultAsync(
                lease.LeaseId,
                new(
                    Guid.NewGuid(),
                    [
                        new OperationSyncResult(accepted.OperationId, OperationResultKind.Accepted, null, null),
                        new OperationSyncResult(conflicted.OperationId, OperationResultKind.Conflict, "conflict", null),
                        new OperationSyncResult(rejected.OperationId, OperationResultKind.Rejected, "invalid", null),
                        new OperationSyncResult(retryable.OperationId, OperationResultKind.Retryable, "retry", null),
                    ],
                    null,
                    null),
                [CreateSnapshotMutation(stream, "reconciled", FourthRevision)],
                CancellationToken.None);

            await Assert.That(changed).HasSingleItem();
            await Assert.That(changed[0].Revision).IsEqualTo(FifthRevision);
            await AssertOperationStateAsync(adapter, accepted.OperationId, SyncOperationState.Synchronized);
            await AssertOperationStateAsync(adapter, conflicted.OperationId, SyncOperationState.Conflict);
            await AssertOperationStateAsync(adapter, rejected.OperationId, SyncOperationState.Rejected);
            await AssertOperationStateAsync(adapter, retryable.OperationId, SyncOperationState.QueuedForUpload);
            await Assert.That(await adapter.GetOperationStatusAsync(OperationId.New(), CancellationToken.None)).IsNull();
            await Assert.That(await adapter.GetRetryStateAsync(OperationId.New(), CancellationToken.None)).IsNull();

            await Assert.That(() => IgnoreResultAsync(adapter.ApplySyncResultAsync(
                lease.LeaseId,
                new(Guid.NewGuid(), [], null, null),
                CancellationToken.None))).Throws<InvalidOperationException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies dead-letter transitions and compaction remove only terminal records.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task DeadLetterOperationsCanBeCompacted()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("dead-letter-compaction");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(ClientIdentity, 1, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);

            var deadLetter = CreateOperation(stream, FirstSequence);
            _ = await adapter.CommitLocalOperationAsync(
                deadLetter,
                CreateSnapshotMutation(stream, "local", InitialRevision),
                CancellationToken.None);
            await using var deadLetterLeases = adapter.LeasePendingOperationsAsync(
                new(stream, 1, PayloadLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await deadLetterLeases.MoveNextAsync()).IsTrue();
            _ = await adapter.DeadLetterOperationAsync(
                deadLetterLeases.Current.LeaseId,
                deadLetter.OperationId,
                "permanent",
                CreateSnapshotMutation(stream, "replacement", FirstRevision),
                CancellationToken.None);
            await AssertOperationStateAsync(adapter, deadLetter.OperationId, SyncOperationState.DeadLettered);

            var compacted = await adapter.CompactAsync(new(stream, DateTimeOffset.MaxValue, 0), CancellationToken.None);
            await Assert.That(compacted.RecordsRemoved).IsEqualTo(1);
            await Assert.That(await adapter.GetOperationStatusAsync(deadLetter.OperationId, CancellationToken.None)).IsNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies remote inbox deduplication and locally originated completions.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task RemoteBatchDeduplicatesEventsAndCompletesOperations()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("remote-transitions");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(
                new(ClientIdentity, 1, false) { ClientId = ClientIdentity },
                CancellationToken.None);
            var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var initialOperation = CreateOperation(stream, FirstSequence);
            _ = await adapter.CommitLocalOperationAsync(
                initialOperation,
                CreateSnapshotMutation(stream, "initial", InitialRevision),
                CancellationToken.None);
            await ApplyRemoteBatchWithDuplicateEventAsync(adapter, stream);

            var accepted = CreateOperation(stream, SecondSequence);
            _ = await adapter.CommitLocalOperationAsync(
                accepted,
                CreateSnapshotMutation(stream, "local-2", SecondRevision),
                CancellationToken.None);
            var completion = new RemoteEventBatch(Guid.NewGuid(), stream, FirstCursor, SecondCursor, [])
            {
                CompletedOperations = [new(new RemoteEventOrigin(ClientIdentity, accepted.OperationId), [])],
            };
            _ = await adapter.ApplyRemoteBatchAsync(
                completion,
                CreateSnapshotMutation(stream, "remote-2", ThirdRevision),
                CancellationToken.None);
            await AssertOperationStateAsync(adapter, accepted.OperationId, SyncOperationState.Synchronized);
            await Assert.That((await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None)).ServerCursor)
                .IsEqualTo(SecondCursor);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies a remote completion cannot acknowledge an unknown local operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task UnknownRemoteCompletionDoesNotAdvanceDurableCursor()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("unknown-completion");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(ClientIdentity, 1, false), CancellationToken.None);
            var subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var batch = new RemoteEventBatch(Guid.NewGuid(), stream, null, FirstCursor, [])
            { CompletedOperations = [new(new RemoteEventOrigin(ClientIdentity, OperationId.New()), [])], };

            await Assert.That(() => IgnoreResultAsync(adapter.ApplyRemoteBatchAsync(
                batch,
                CreateSnapshotMutation(stream, "remote", InitialRevision),
                CancellationToken.None))).Throws<InvalidOperationException>();
            await Assert.That((await adapter.RecoverStreamAsync(stream, subscription, CancellationToken.None)).ServerCursor).IsNull();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies at-most-once attempt barriers stop ambiguous retries.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task RemoteAttemptBarrierStopsAmbiguousAtMostOnceRetry()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-filesystem-{Guid.NewGuid():N}");
        try
        {
            var stream = new StreamId("attempt-barrier");
            await using var adapter = new FileSystemLocalStoreAdapter(directory);
            await adapter.InitializeAsync(new(ClientIdentity, 1, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var operation = CreateOperation(stream, FirstSequence) with
            {
                Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
            };
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(stream, "state", InitialRevision), CancellationToken.None);
            await using var leases = adapter.LeasePendingOperationsAsync(
                new(stream, 1, PayloadLimit, TimeSpan.FromMinutes(1)),
                CancellationToken.None).GetAsyncEnumerator();
            await Assert.That(await leases.MoveNextAsync()).IsTrue();
            var lease = leases.Current;

            await Assert.That(() => IgnoreResultAsync(adapter.TryBeginRemoteAttemptAsync(
                Guid.NewGuid(),
                operation.OperationId,
                1,
                CancellationToken.None))).Throws<InvalidOperationException>();

            var firstAttempt = await adapter.TryBeginRemoteAttemptAsync(
                lease.LeaseId,
                operation.OperationId,
                1,
                CancellationToken.None);
            await Assert.That(firstAttempt.MaySend).IsTrue();
            await Assert.That((await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None))?.State)
                .IsEqualTo(SyncOperationState.Ambiguous);

            const int secondAttempt = 2;
            var blockedRetry = await adapter.TryBeginRemoteAttemptAsync(
                lease.LeaseId,
                operation.OperationId,
                secondAttempt,
                CancellationToken.None);
            await Assert.That(blockedRetry.MaySend).IsFalse();
            await Assert.That(blockedRetry.ReasonCode).IsEqualTo("OC.AmbiguousAtMostOnce");

            var invalidLease = new OutboxLeaseRequest(stream, InitialRevision, PayloadLimit, TimeSpan.FromMinutes(1));
            await Assert.That(() => adapter.LeasePendingOperationsAsync(invalidLease, CancellationToken.None))
                .Throws<ArgumentOutOfRangeException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Applies a batch containing a duplicate event and verifies inbox deduplication.</summary>
    /// <param name="adapter">The initialized FileSystem store.</param>
    /// <param name="stream">The event stream.</param>
    /// <returns>The asynchronous assertion task.</returns>
    private static async Task ApplyRemoteBatchWithDuplicateEventAsync(
        FileSystemLocalStoreAdapter adapter,
        StreamId stream)
    {
        var firstEventId = Guid.NewGuid();
        var remoteEvent = new RemoteEvent(
            firstEventId,
            stream,
            FirstCursor,
            TestTimeProvider.GetUtcNow(),
            null,
            CreatePayload("event"),
            new Dictionary<string, string>());
        var remoteBatch = new RemoteEventBatch(Guid.NewGuid(), stream, null, FirstCursor, [remoteEvent, remoteEvent])
        {
            CompletedOperations = [new(new RemoteEventOrigin(RemoteClientIdentity, OperationId.New()), [])],
        };
        var firstApply = await adapter.ApplyRemoteBatchAsync(
            remoteBatch,
            CreateSnapshotMutation(stream, "remote-1", FirstRevision),
            CancellationToken.None);
        await Assert.That(firstApply.AppliedCount).IsEqualTo(1);
        await Assert.That(firstApply.DuplicateCount).IsEqualTo(1);
        await Assert.That(await adapter.GetUnappliedEventIdsAsync(stream, [firstEventId], CancellationToken.None)).IsEmpty();
    }

    /// <summary>Awaits an asynchronous operation and discards its result for exception assertions.</summary>
    /// <typeparam name="T">The operation result type.</typeparam>
    /// <param name="operation">The operation to await.</param>
    /// <returns>A task that completes when the operation completes.</returns>
    private static async Task IgnoreResultAsync<T>(ValueTask<T> operation) => _ = await operation;

    /// <summary>Awaits an asynchronous operation for exception assertions.</summary>
    /// <param name="operation">The operation to await.</param>
    /// <returns>A task that completes when the operation completes.</returns>
    private static async Task IgnoreResultAsync(ValueTask operation) => await operation;

    /// <summary>Verifies the durable state recorded for an operation.</summary>
    /// <param name="adapter">The initialized store.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="expectedState">The expected durable state.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertOperationStateAsync(
        FileSystemLocalStoreAdapter adapter,
        OperationId operationId,
        SyncOperationState expectedState)
    {
        var status = await adapter.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(expectedState);
    }

    /// <summary>Creates a mutation with the filesystem snapshot format version.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="value">The payload value.</param>
    /// <param name="expectedRevision">The current snapshot revision.</param>
    /// <returns>The requested snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(StreamId stream, string value, long expectedRevision) =>
        new(stream, CreatePayload(value), SnapshotFormatVersion, expectedRevision);

    /// <summary>Creates a subscription identifier for a test value.</summary>
    /// <param name="value">The identifier value.</param>
    /// <returns>The requested identifier.</returns>
    private static SubscriptionId CreateSubscriptionId(Guid value) => new(value);

    /// <summary>Creates a stream identifier for a test value.</summary>
    /// <param name="value">The identifier value.</param>
    /// <returns>The requested identifier.</returns>
    private static StreamId CreateStreamId(string value) => new(value);
}
