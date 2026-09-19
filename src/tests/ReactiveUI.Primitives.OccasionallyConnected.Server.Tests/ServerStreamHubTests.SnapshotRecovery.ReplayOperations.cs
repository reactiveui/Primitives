// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery tests for replay-only operation proof reconciliation.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The pending operation payload used for unknown proof checks.</summary>
    private const string PendingUnknownPayload = "pending-unknown";

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

    /// <summary>Verifies replay-only rejected and conflict proofs fail closed before materialization.</summary>
    /// <param name="operationResultKind">The retained replay-only proof kind.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(OperationResultKind.Rejected)]
    [Arguments(OperationResultKind.Conflict)]
    public async Task GetSnapshotAsyncReturnsAmbiguousPendingOperationForReplayOnlyContradictoryProofBeforeMaterialization(
        OperationResultKind operationResultKind)
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        var replayOperation = Operation(SnapshotThirdOperationSeed, "replay-contradictory-proof");
        var pendingOperation = Operation(SnapshotThirdOperationSeed + 1, PendingUnknownPayload);
        await using var hub = ServerStreamHub.CreateInMemory(
            ReplayProofOptions(
                new RecordingDomainHandler(),
                new TargetReplayProofResolver(replayOperation.OperationId, operationResultKind),
                materializer));
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var upload = await hub.ApplyOperationsAsync(Batch(replayOperation), new(Tenant, Client), CancellationToken.None);
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, [pendingOperation], [replayOperation]);

        await Assert.That(upload.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(upload.Result.Operations[0].Kind).IsEqualTo(operationResultKind);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(result.OperationDispositions).IsEmpty();
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

    /// <summary>Returns rejected or conflict proof for one target operation and delegates all other operations.</summary>
    /// <param name="operationId">The operation that should receive contradictory proof.</param>
    /// <param name="operationResultKind">The contradictory proof kind.</param>
    private sealed class TargetReplayProofResolver(OperationId operationId, OperationResultKind operationResultKind) : IConflictResolver
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

            var result = operationResultKind == OperationResultKind.Conflict
                ? new ConflictResolutionResult(
                    [operation.OperationId],
                    [],
                    [new(operation.OperationId, "snapshot-replay-conflict", null)],
                    [],
                    context.Current.Version)
                : new ConflictResolutionResult(
                    [],
                    [new(operation.OperationId, "snapshot-replay-rejected", false)],
                    [],
                    [],
                    context.Current.Version);
            return ValueTask.FromResult(result);
        }
    }
}
