// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery tests for replay-only operation proof reconciliation.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The pending operation payload used for unknown proof checks.</summary>
    private const string PendingUnknownPayload = "pending-unknown";

    /// <summary>The fingerprint input bound for the retained replay proof.</summary>
    private const int ReplayProofFingerprintMaximumBytes = 4096;

    /// <summary>Verifies recovery includes accepted replay-only proof while preserving pending unknown proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncIncludesReplayOnlyAcceptedProofWithPendingUnknown()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var replayOperation = Operation(1, PayloadA);
        var pendingOperation = Operation(SnapshotThirdOperationSeed, PendingUnknownPayload);
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, replayOperation);
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, [pendingOperation], [replayOperation]);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
        var pendingDisposition = result.OperationDispositions.SingleOrDefault(disposition => disposition.OperationId == pendingOperation.OperationId);
        var replayDisposition = result.OperationDispositions.SingleOrDefault(disposition => disposition.OperationId == replayOperation.OperationId);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(result.OperationDispositions).Count().IsEqualTo(SnapshotDoubleCount);
        await Assert.That(pendingDisposition).IsNotNull();
        await Assert.That(pendingDisposition?.Kind).IsEqualTo(SnapshotOperationDispositionKind.Unknown);
        await Assert.That(replayDisposition).IsNotNull();
        await Assert.That(replayDisposition?.Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(replayDisposition?.Result?.Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(materializer.CallCount).IsEqualTo(SingleCount);
    }

    /// <summary>Verifies replay-only missing proof fails closed before materialization or durable offer mutation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsAmbiguousPendingOperationForReplayOnlyMissingProofBeforeMaterialization()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var replayOnlyWithoutProof = Operation(SnapshotThirdOperationSeed, "replay-missing-proof");
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, [], [replayOnlyWithoutProof]);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
        var retry = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(result.OperationDispositions).IsEmpty();
        await Assert.That(retry.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies SQLite recovery replays a lost snapshot response after disposal and reopen.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncWithSqliteReplaysLostSnapshotResponseAfterReopen()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        var materializedClientState = Payload(SnapshotClientPayload);
        var operation = Operation(1, PayloadA);
        string? expiredCursor;
        RemoteSnapshotRecoveryResult first;
        await using (var hub = ServerStreamHub.CreateSqlite(
            database.Path,
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(materializedClientState),
            }))
        {
            var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, operation);
            expiredCursor = seed.ExpiredCursor;
            first = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
                SnapshotRecoveryRequest(subscriptionId, expiredCursor, operation),
                new(Tenant, Client),
                CancellationToken.None);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(
            database.Path,
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(materializedClientState),
            });
        var replayed = await ((IServerSnapshotRecoveryHub)reopened).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, expiredCursor, operation),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(first.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(replayed.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(replayed.Checkpoint?.FrontierCursor).IsEqualTo(first.Checkpoint?.FrontierCursor);
        await AssertSnapshotPayloadAsync(replayed.Checkpoint?.ClientState, materializedClientState);

        _ = await ReadFirstBatchAsync(reopened, new(Tenant, Client), subscriptionId);
        var afterNormalOffer = await ((IServerSnapshotRecoveryHub)reopened).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, expiredCursor, operation),
            new(Tenant, Client),
            CancellationToken.None);
        await Assert.That(afterNormalOffer.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);
    }

    /// <summary>Verifies SQLite reopens and replays a durable snapshot offer bound to accepted replay-only proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncWithSqliteReplaysAcceptedReplayOnlyProofAfterReopen()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        var materializedClientState = Payload(SnapshotClientPayload);
        var replayOperation = Operation(1, PayloadA);
        string? expiredCursor;
        RemoteSnapshotRecoveryResult first;
        await using (var hub = ServerStreamHub.CreateSqlite(
            database.Path,
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(materializedClientState),
            }))
        {
            var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, replayOperation);
            expiredCursor = seed.ExpiredCursor;
            first = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
                SnapshotRecoveryRequest(subscriptionId, expiredCursor, [], [replayOperation]),
                new(Tenant, Client),
                CancellationToken.None);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(
            database.Path,
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(materializedClientState),
            });
        var replayed = await ((IServerSnapshotRecoveryHub)reopened).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, expiredCursor, [], [replayOperation]),
            new(Tenant, Client),
            CancellationToken.None);

        var firstDisposition = first.OperationDispositions.SingleOrDefault(disposition => disposition.OperationId == replayOperation.OperationId);
        var replayedDisposition = replayed.OperationDispositions.SingleOrDefault(disposition => disposition.OperationId == replayOperation.OperationId);

        await Assert.That(first.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(firstDisposition?.Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(firstDisposition?.Result?.Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(replayed.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(replayed.Checkpoint?.FrontierCursor).IsEqualTo(first.Checkpoint?.FrontierCursor);
        await Assert.That(replayedDisposition?.Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(replayedDisposition?.Result?.Kind).IsEqualTo(OperationResultKind.Accepted);
        await AssertSnapshotPayloadAsync(replayed.Checkpoint?.ClientState, materializedClientState);
    }

    /// <summary>Verifies a replay-only rejected proof fails closed before materialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsAmbiguousPendingOperationForReplayOnlyRejectedProofBeforeMaterialization()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        var replayOperation = Operation(SnapshotThirdOperationSeed, "replay-contradictory-proof");
        var pendingOperation = Operation(SnapshotThirdOperationSeed + 1, PendingUnknownPayload);
        await using var hub = ServerStreamHub.CreateInMemory(
            ReplayProofOptions(
                new RecordingDomainHandler(),
                new TargetReplayProofResolver(replayOperation.OperationId),
                materializer));
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var upload = await hub.ApplyOperationsAsync(Batch(replayOperation), new(Tenant, Client), CancellationToken.None);
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, [pendingOperation], [replayOperation]);

        await Assert.That(upload.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(upload.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(result.OperationDispositions).IsEmpty();
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies a retained conflict receipt blocks replay-only snapshot recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsAmbiguousPendingOperationForReplayOnlyConflictProofBeforeMaterialization()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        var replayOperation = Operation(SnapshotThirdOperationSeed, "replay-conflict-proof");
        var pendingOperation = Operation(SnapshotThirdOperationSeed + 1, PendingUnknownPayload);
        var options = ReplayProofOptions(new RecordingDomainHandler(), Resolver(), materializer);
        string? expiredCursor;
        await using (var seedingHub = ServerStreamHub.CreateSqlite(database.Path, options))
        {
            var seed = await SeedSnapshotRecoveryFrontierAsync(seedingHub, subscriptionId);
            expiredCursor = seed.ExpiredCursor;
        }

        var streamKey = new ServerStreamKey(Tenant, Stream);
        var operationKey = new ServerOperationKey(Client, replayOperation.OperationId);
        using (var journal = new SqliteServerCommitJournal(database.Path, new() { TimeProvider = options.TimeProvider }))
        {
            var before = journal.Read(streamKey, [operationKey]);
            var fingerprint = new ServerCommitFingerprint(CanonicalOperationFingerprint.Compute(
                Tenant,
                Client,
                replayOperation,
                ReplayProofFingerprintMaximumBytes));
            var result = new OperationSyncResult(replayOperation.OperationId, OperationResultKind.Conflict, "snapshot-replay-conflict", before.State?.Version);
            var entry = new ServerLedgerEntry(operationKey, fingerprint, result, [], []);
            var commit = journal.TryCommit(new(streamKey, before.Revision, null, null, [entry]));
            await Assert.That(commit.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        await using var hub = ServerStreamHub.CreateSqlite(database.Path, options);
        var upload = await hub.ApplyOperationsAsync(Batch(replayOperation), new(Tenant, Client), CancellationToken.None);
        var request = SnapshotRecoveryRequest(subscriptionId, expiredCursor, [pendingOperation], [replayOperation]);
        var recovery = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(upload.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(recovery.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(recovery.Checkpoint).IsNull();
        await Assert.That(recovery.OperationDispositions).IsEmpty();
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies same-id replay fingerprint mismatch fails closed while pending unknown remains recoverable by itself.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsAmbiguousPendingOperationForReplayOnlyMismatchedSameIdProofWithPendingUnknown()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var retainedReplayOperation = Operation(1, PayloadA);
        var mismatchedReplayOperation = retainedReplayOperation with { Payload = Payload("replay-mismatched-proof") };
        var pendingOperation = Operation(SnapshotThirdOperationSeed, PendingUnknownPayload);
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, retainedReplayOperation);
        var request = SnapshotRecoveryRequest(
            subscriptionId,
            seed.ExpiredCursor,
            [pendingOperation],
            [mismatchedReplayOperation]);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(result.OperationDispositions).IsEmpty();
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Creates snapshot recovery hub options with a custom resolver for one replay proof.</summary>
    /// <param name="domain">The domain handler.</param>
    /// <param name="resolver">The conflict resolver.</param>
    /// <param name="materializer">The snapshot materializer.</param>
    /// <returns>The configured hub options.</returns>
    private static ServerStreamHubOptions ReplayProofOptions(
        IServerDomainHandler domain,
        IConflictResolver resolver,
        IServerSnapshotMaterializer materializer) =>
        Options(new AllowPolicy(Tenant), domain) with
        {
            SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
            SnapshotRecoveryMaterializer = materializer,
            ConflictHandler = new()
            {
                Streams =
                [
                    new()
                    {
                        StreamId = Stream,
                        InitialStateFactory = new InitialStateFactory(),
                        LastWriterWinsResolver = resolver,
                        MergeResolver = resolver,
                        CustomResolver = resolver,
                        DomainHandler = domain,
                    },
                ],
            },
        };

    /// <summary>Rejects one target operation and delegates all other operations.</summary>
    /// <param name="operationId">The operation that should receive contradictory proof.</param>
    private sealed class TargetReplayProofResolver(OperationId operationId) : IConflictResolver
    {
        /// <inheritdoc/>
        public ValueTask<ConflictResolutionResult> ResolveAsync(
            ConflictContext context,
            CancellationToken cancellationToken)
        {
            var operation = context.Incoming[0];
            if (operation.OperationId != operationId)
            {
                return Resolver().ResolveAsync(context, cancellationToken);
            }

            var result = new ConflictResolutionResult(
                [],
                [new(operation.OperationId, "snapshot-replay-rejected", false)],
                [],
                [],
                context.Current.Version);
            return ValueTask.FromResult(result);
        }
    }
}
