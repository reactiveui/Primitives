// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The snapshot recovery contract used by hub tests.</summary>
    private const string SnapshotClientContract = Contract;

    /// <summary>The materialized client-state payload used by snapshot recovery tests.</summary>
    private const string SnapshotClientPayload = "snapshot-client";

    /// <summary>The second committed server version used by snapshot recovery tests.</summary>
    private const string SnapshotSecondVersion = "v2";

    /// <summary>The third operation seed used by snapshot recovery mutation tests.</summary>
    private const int SnapshotThirdOperationSeed = 3;

    /// <summary>The default maximum snapshot response byte count used by tests.</summary>
    private const int SnapshotMaximumResponseBytes = 16_384;

    /// <summary>The small configured payload byte limit used by capacity tests.</summary>
    private const int SnapshotSmallPayloadBytes = 8;

    /// <summary>The large payload byte count used to exceed aggregate logical byte limits.</summary>
    private const int SnapshotLargePayloadBytes = 3_000;

    /// <summary>The configured payload byte limit used by logical-byte capacity tests.</summary>
    private const int SnapshotLogicalCapacityPayloadBytes = 4_096;

    /// <summary>The aggregate logical byte limit used by capacity tests.</summary>
    private const int SnapshotSmallLogicalBytes = 2_000;

    /// <summary>The logical byte limit that permits the large test payload but rejects the full response.</summary>
    private const int SnapshotMediumLogicalBytes = 8_000;

    /// <summary>The reason code length that exceeds the default reason-code byte limit.</summary>
    private const int SnapshotOversizedReasonCodeLength = 129;

    /// <summary>An undefined materializer status used to verify fail-closed status mapping.</summary>
    private const int SnapshotInvalidMaterializationStatus = 99;

    /// <summary>The expected double item count.</summary>
    private const int SnapshotDoubleCount = 2;

    /// <summary>The guard timeout used by snapshot recovery barrier tests.</summary>
    private static readonly TimeSpan SnapshotGuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Gets a malformed UTF-16 protocol string used by validation tests.</summary>
    private static string SnapshotInvalidUtf16 => new('\uD800', 1);

    /// <summary>Verifies the concrete hub exposes the public snapshot recovery hub contract.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ServerStreamHubImplementsSnapshotRecoveryHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        object candidate = hub;

        await Assert.That(candidate is IServerSnapshotRecoveryHub).IsTrue();
    }

    /// <summary>Verifies snapshot recovery uses trusted authorization and materializes a captured server state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncMaterializesCapturedServerStateForAuthorizedSubscription()
    {
        var subscriptionId = SubscriptionId.New();
        var operation = Operation(1, PayloadA);
        var materializedClientState = Payload(SnapshotClientPayload);
        var materializer = new RecordingSnapshotMaterializer(materializedClientState);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, operation);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, operation),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(result.Checkpoint).IsNotNull();
        await Assert.That(result.Checkpoint?.StreamId).IsEqualTo(Stream);
        await Assert.That(result.Checkpoint?.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(result.Checkpoint?.FrontierCursor).IsEqualTo(materializer.Contexts[0].FrontierCursor);
        await Assert.That(result.Checkpoint?.FrontierCursor).IsNotEqualTo(seed.ExpiredCursor);
        await Assert.That(result.Checkpoint?.ServerVersion).IsEqualTo(SnapshotSecondVersion);
        await AssertSnapshotPayloadAsync(result.Checkpoint?.ClientState, materializedClientState);
        await Assert.That(result.OperationDispositions).Count().IsEqualTo(SingleCount);
        await Assert.That(result.OperationDispositions[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(result.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(materializer.CallCount).IsEqualTo(SingleCount);
        await Assert.That(materializer.Contexts[0].TenantId).IsEqualTo(Tenant);
        await Assert.That(materializer.Contexts[0].ClientId).IsEqualTo(Client);
        await Assert.That(materializer.Contexts[0].CapturedServerVersion).IsEqualTo(SnapshotSecondVersion);
        await Assert.That(materializer.Contexts[0].CapturedServerState.Version).IsEqualTo(SnapshotSecondVersion);
        await Assert.That(materializer.Contexts[0].CapturedServerState.State.PayloadHash).IsEqualTo(PayloadB);
        await Assert.That(materializer.Contexts[0].ClientStateContractId).IsEqualTo(SnapshotClientContract);
    }

    /// <summary>Verifies a repeated recovery replays a durable snapshot offer after a lost response.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReplaysLostSnapshotResponseForRepeatedPublicRecovery()
    {
        var subscriptionId = SubscriptionId.New();
        var materializedClientState = Payload(SnapshotClientPayload);
        var materializer = new SequencedSnapshotMaterializer(materializedClientState, materializedClientState, Payload("changed-snapshot-client"), materializedClientState);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var operation = Operation(1, PayloadA);
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, operation);
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, operation);

        var first = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
        var replayed = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);

        await Assert.That(first.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(replayed.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(replayed.Checkpoint?.FrontierCursor).IsEqualTo(first.Checkpoint?.FrontierCursor);
        await AssertSnapshotPayloadAsync(replayed.Checkpoint?.ClientState, materializedClientState);
        await Assert.That(replayed.OperationDispositions[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(replayed.OperationDispositions[0].Kind).IsEqualTo(first.OperationDispositions[0].Kind);
        var changedPayload = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
        await Assert.That(changedPayload.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);

        _ = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);
        var afterNormalOffer = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
        await Assert.That(afterNormalOffer.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);
    }

    /// <summary>Verifies recovery uses a group frontier cursor when accepted commits produce no receive events.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncMaterializesGroupCursorFrontierWhenAcceptedCommitsHaveNoEvents()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new NoEventDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var expired = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);
        _ = await hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None);

        var recovered = await GetSnapshotAsync(hub, subscriptionId, expired.NextCursor);

        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.FrontierCursor).IsEqualTo(materializer.Contexts[0].FrontierCursor);
        await Assert.That(recovered.Checkpoint?.FrontierCursor).IsNotEqualTo(expired.NextCursor);
        var expectedGroupCursor = ServerReceiveGroupCursor.Create(new(Tenant, Stream), SnapshotDoubleCount);

        await Assert.That(recovered.Checkpoint?.FrontierCursor).IsEqualTo(expectedGroupCursor);
    }

    /// <summary>Verifies authorization scope mismatch rejects recovery before materialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsTenantMismatchBeforeMaterialization()
    {
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new TenantMismatchSnapshotRecoveryPolicy(),
                SnapshotRecoveryMaterializer = materializer,
            });

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => ((IServerSnapshotRecoveryHub)hub)
                .GetSnapshotAsync(SnapshotRecoveryRequest(SubscriptionId.New(), MissingCursor), new(Tenant, Client), CancellationToken.None)
                .AsTask());
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies unsupported recovery configuration fails closed without exposing state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncWithoutMaterializerReturnsUnsupportedProjection()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(SubscriptionId.New(), MissingCursor),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.UnsupportedProjection);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(result.OperationDispositions).IsEmpty();
    }

    /// <summary>Verifies recovery returns retention-expired before materialization when no retained state is present.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsRetentionExpiredWithoutRetainedSubscription()
    {
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(SubscriptionId.New(), MissingCursor),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
        await Assert.That(result.Checkpoint).IsNull();
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies null expired cursors recover without requiring an expired offer proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRecoversWhenExpiredCursorIsNull()
    {
        var subscriptionId = SubscriptionId.New();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload)),
            });
        _ = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var recovered = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, expiredCursor: null),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint).IsNotNull();
    }

    /// <summary>Verifies null materializer output is treated as validation rejection without a durable offer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsNullMaterializationWithoutDurableOffer()
    {
        var subscriptionId = SubscriptionId.New();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new NullSnapshotMaterializer(),
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var result = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(result.Checkpoint).IsNull();
    }

    /// <summary>Verifies materializer non-recovered statuses do not mutate durable offers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncMapsMaterializerStatusWithoutDurableOffer()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new SequencedSnapshotMaterializer(results:
            [
                new() { Status = ServerSnapshotMaterializationStatus.UnsupportedProjection, ReasonCode = "snapshot.unsupported" },
                new() { Status = ServerSnapshotMaterializationStatus.CapacityExceeded, ReasonCode = "snapshot.capacity" },
                new() { Status = ServerSnapshotMaterializationStatus.ValidationRejected, ReasonCode = SnapshotInvalidUtf16 },
                new() { Status = ServerSnapshotMaterializationStatus.UnsupportedProjection, ReasonCode = new('a', SnapshotOversizedReasonCodeLength) },
                new() { Status = ServerSnapshotMaterializationStatus.CapacityExceeded, ReasonCode = new('a', SnapshotOversizedReasonCodeLength) },
                new() { Status = (ServerSnapshotMaterializationStatus)SnapshotInvalidMaterializationStatus, ReasonCode = "snapshot.invalid_status" },
                new() { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = Payload(SnapshotClientPayload) },
            ]);
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var unsupported = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var capacity = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var malformedReason = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var oversizedUnsupportedReason = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var oversizedCapacityReason = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var invalidStatus = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var recovered = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);

        await Assert.That(unsupported.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.UnsupportedProjection);
        await Assert.That(capacity.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(malformedReason.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(malformedReason.ReasonCode).IsEqualTo("snapshot.validation_rejected");
        await Assert.That(oversizedUnsupportedReason.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(oversizedUnsupportedReason.ReasonCode).IsEqualTo("snapshot.validation_rejected");
        await Assert.That(oversizedCapacityReason.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(oversizedCapacityReason.ReasonCode).IsEqualTo("snapshot.capacity_exceeded");
        await Assert.That(invalidStatus.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(invalidStatus.ReasonCode).IsEqualTo("snapshot.invalid_status");
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
    }

    /// <summary>Verifies materialized recovery requires an explicit recovery authorization policy.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateHubRejectsSnapshotMaterializerWithoutRecoveryAuthorization()
    {
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
        {
            SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload)),
        };

        await Assert.That(() => ServerStreamHub.CreateInMemory(options)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies materialization runs outside the journal lock and stale offers are retried.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReturnsRetryableConcurrentChangeWhenStreamMutatesBeforeOffer()
    {
        var subscriptionId = SubscriptionId.New();
        var blocking = new BlockingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = blocking,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var recovery = ((IServerSnapshotRecoveryHub)hub)
            .GetSnapshotAsync(SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor), new(Tenant, Client), CancellationToken.None)
            .AsTask();

        try
        {
            await blocking.WaitUntilStartedAsync().WaitAsync(SnapshotGuardTimeout);
            _ = await hub.ApplyOperationsAsync(Batch(Operation(SnapshotThirdOperationSeed, PayloadA)), new(Tenant, Client), CancellationToken.None);
        }
        finally
        {
            blocking.Release();
        }

        var result = await recovery.WaitAsync(SnapshotGuardTimeout);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);
    }

    /// <summary>Verifies response byte bounds and configured payload bounds reject before durable mutation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsMaterializedCapacityFailuresWithoutDurableOffer()
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new SequencedSnapshotMaterializer(Payload("tiny"), Payload(SnapshotClientPayload), Payload("tiny"));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
                SnapshotRecoveryLimits = new() { MaximumPayloadBytes = SnapshotSmallPayloadBytes },
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var responseCapacity = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor) with { MaximumResponseBytes = 1 },
            new(Tenant, Client),
            CancellationToken.None);
        var payloadCapacity = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var recovered = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);

        await Assert.That(responseCapacity.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(payloadCapacity.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
    }

    /// <summary>Verifies recovered-result logical byte limits include null and non-null operation dispositions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncCountsDispositionsWhenRecoveredResultExceedsLogicalBytes()
    {
        var accepted = Operation(1, PayloadA);
        var unknown = Operation(SnapshotThirdOperationSeed, PayloadA);
        var requestBound = await RecoverWithLogicalByteLimitsAsync(
            SnapshotLargePayloadBytes,
            SnapshotSmallLogicalBytes,
            SnapshotMediumLogicalBytes,
            accepted,
            unknown);
        var recovered = await RecoverWithLogicalByteLimitsAsync(
            SnapshotSmallPayloadBytes,
            SnapshotMediumLogicalBytes,
            SnapshotMediumLogicalBytes,
            accepted,
            unknown);

        await Assert.That(requestBound.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(requestBound.Checkpoint).IsNull();
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.OperationDispositions[0].Result).IsNotNull();
        await Assert.That(recovered.OperationDispositions[1].Result).IsNull();
    }

    /// <summary>Verifies malformed materializer payloads reject validation without a durable offer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsInvalidMaterializerPayloadWithoutDurableOffer()
    {
        var subscriptionId = SubscriptionId.New();
        var invalidSchema = Payload(SnapshotClientPayload) with { SchemaVersion = 0 };
        var invalidContent = Payload(SnapshotClientPayload) with { ContentType = SnapshotInvalidUtf16 };
        var invalidHash = Payload(SnapshotClientPayload) with { PayloadHash = SnapshotInvalidUtf16 };
        var emptyContent = Payload(SnapshotClientPayload) with { ContentType = string.Empty };
        var emptyHash = Payload(SnapshotClientPayload) with { PayloadHash = string.Empty };
        var materializer = new SequencedSnapshotMaterializer(
            invalidSchema,
            invalidContent,
            invalidHash,
            emptyContent,
            emptyHash,
            Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var rejectedSchema = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var rejectedContent = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var rejectedHash = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var rejectedEmptyContent = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var rejectedEmptyHash = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);
        var recovered = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);

        await Assert.That(rejectedSchema.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(rejectedContent.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(rejectedHash.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(rejectedEmptyContent.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(rejectedEmptyHash.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.ValidationRejected);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.FrontierCursor).IsNotEqualTo(seed.ExpiredCursor);
    }

    /// <summary>Verifies a full subscription offer journal maps durable snapshot offer capacity to a public capacity result.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncMapsDurableOfferCapacityExceededWithoutDurableMutation()
    {
        var subscriptionId = SubscriptionId.New();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload)),
                JournalLimits = new() { MaximumSubscriptionOffers = SingleCount },
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);

        var capacity = await GetSnapshotAsync(hub, subscriptionId, seed.ExpiredCursor);

        await Assert.That(capacity.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.CapacityExceeded);
        await Assert.That(capacity.Checkpoint).IsNull();
    }

    /// <summary>Verifies a snapshot offer that collides with a retained receive offer is retried after the frontier advances.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRetriesSnapshotOfferThatCollidesWithNormalReceiveOffer()
    {
        var subscriptionId = SubscriptionId.New();
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload)),
            });
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        var retainedNormalOffer = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);

        var firstRejected = await GetSnapshotAsync(hub, subscriptionId, retainedNormalOffer.NextCursor);
        var secondRejected = await GetSnapshotAsync(hub, subscriptionId, retainedNormalOffer.NextCursor);
        _ = await hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None);
        var recovered = await GetSnapshotAsync(hub, subscriptionId, retainedNormalOffer.NextCursor);

        await Assert.That(firstRejected.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);
        await Assert.That(secondRejected.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetryableConcurrentChange);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.FrontierCursor).IsNotEqualTo(retainedNormalOffer.NextCursor);
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

    /// <summary>Verifies SQLite snapshot offers persist across reopen and can be acknowledged.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot did not include a cursor.</exception>
    [Test]
    public async Task GetSnapshotAsyncWithSqlitePersistsRecoveredOfferForAcknowledgementAfterReopen()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        string cursor;
        await using (var first = ServerStreamHub.CreateSqlite(
            database.Path,
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload)),
            }))
        {
            var seed = await SeedSnapshotRecoveryFrontierAsync(first, subscriptionId);
            var recovered = await GetSnapshotAsync(first, subscriptionId, seed.ExpiredCursor);
            cursor = recovered.Checkpoint?.FrontierCursor ?? throw new InvalidOperationException("Recovered snapshots must include a cursor.");

            await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        }

        await using var second = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler()));
        await second.AcknowledgeAsync(new(subscriptionId, Stream, cursor), new(Tenant, Client), CancellationToken.None);
        await second.AcknowledgeAsync(new(subscriptionId, Stream, cursor), new(Tenant, Client), CancellationToken.None);
    }

    /// <summary>Verifies SQLite recovery maps a subscription compacted between capture and offer to retention expired.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncWithSqliteReturnsRetentionExpiredWhenSubscriptionCompactsBeforeOffer()
    {
        using var database = new SqliteLease();
        var clock = new MutableSnapshotTimeProvider(Start);
        var subscriptionId = SubscriptionId.New();
        var blocking = new BlockingSnapshotMaterializer(Payload(SnapshotClientPayload));
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
        {
            TimeProvider = clock,
            JournalLimits = new() { OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes), SubscriptionRetention = TimeSpan.FromTicks(SingleCount) },
            SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
            SnapshotRecoveryMaterializer = blocking,
        };
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, options);
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var recovery = ((IServerSnapshotRecoveryHub)hub)
            .GetSnapshotAsync(SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor), new(Tenant, Client), CancellationToken.None)
            .AsTask();

        try
        {
            await blocking.WaitUntilStartedAsync().WaitAsync(SnapshotGuardTimeout);
            clock.SetUtcNow(Start.AddTicks(SnapshotThirdOperationSeed));
            using var compactor = new SqliteServerCommitJournal(
                database.Path,
                new() { TimeProvider = clock, OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes), SubscriptionRetention = TimeSpan.FromTicks(SingleCount) });
            _ = compactor.Compact();
        }
        finally
        {
            blocking.Release();
        }

        var result = await recovery.WaitAsync(SnapshotGuardTimeout);

        await Assert.That(result.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
        await Assert.That(result.Checkpoint).IsNull();
    }

    /// <summary>Verifies disposing the hub cancels a blocked snapshot materializer and drains the active call.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncCancelsBlockedSnapshotMaterializerAndDrainsCall()
    {
        var subscriptionId = SubscriptionId.New();
        var blocking = new BlockingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = blocking,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var recovery = ((IServerSnapshotRecoveryHub)hub)
            .GetSnapshotAsync(SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor), new(Tenant, Client), CancellationToken.None)
            .AsTask();

        try
        {
            await blocking.WaitUntilStartedAsync().WaitAsync(SnapshotGuardTimeout);
            await hub.DisposeAsync().AsTask().WaitAsync(SnapshotGuardTimeout);
        }
        finally
        {
            blocking.Release();
        }

        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => recovery.WaitAsync(SnapshotGuardTimeout));
    }

    /// <summary>Verifies snapshot recovery releases active-call capacity after a blocked materializer finishes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncReleasesActiveCallCapacityAfterMaterializationCompletes()
    {
        var subscriptionId = SubscriptionId.New();
        var blocking = new BlockingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                MaximumActiveCalls = SingleCount,
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = blocking,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        var recovery = ((IServerSnapshotRecoveryHub)hub)
            .GetSnapshotAsync(SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor), new(Tenant, Client), CancellationToken.None)
            .AsTask();

        try
        {
            await blocking.WaitUntilStartedAsync().WaitAsync(SnapshotGuardTimeout);
            _ = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
                () => ((IServerSnapshotRecoveryHub)hub)
                    .GetSnapshotAsync(SnapshotRecoveryRequest(SubscriptionId.New(), MissingCursor), new(Tenant, Client), CancellationToken.None)
                    .AsTask());
        }
        finally
        {
            blocking.Release();
        }

        var recovered = await recovery.WaitAsync(SnapshotGuardTimeout);
        var afterRelease = await GetSnapshotAsync(hub, SubscriptionId.New(), MissingCursor);

        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(afterRelease.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
    }

    /// <summary>Verifies caller cancellation is observed during blocked materialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncObservesCancellationDuringMaterialization()
    {
        var subscriptionId = SubscriptionId.New();
        var blocking = new BlockingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = blocking,
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId);
        using var cancellation = new CancellationTokenSource();
        var recovery = ((IServerSnapshotRecoveryHub)hub)
            .GetSnapshotAsync(SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor), new(Tenant, Client), cancellation.Token)
            .AsTask();

        try
        {
            await blocking.WaitUntilStartedAsync().WaitAsync(SnapshotGuardTimeout);
            await cancellation.CancelAsync();
        }
        finally
        {
            blocking.Release();
        }

        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => recovery.WaitAsync(SnapshotGuardTimeout));
    }

    /// <summary>Runs recovery with caller and global logical byte limits.</summary>
    /// <param name="payloadBytes">The materialized payload byte count.</param>
    /// <param name="maximumResponseBytes">The request response byte limit.</param>
    /// <param name="maximumLogicalBytes">The configured global logical byte limit.</param>
    /// <param name="pendingOperations">The pending operation intents.</param>
    /// <returns>The recovery result.</returns>
    private static async Task<RemoteSnapshotRecoveryResult> RecoverWithLogicalByteLimitsAsync(
        int payloadBytes,
        int maximumResponseBytes,
        int maximumLogicalBytes,
        params SyncOperation[] pendingOperations)
    {
        var subscriptionId = SubscriptionId.New();
        var materializer = new RecordingSnapshotMaterializer(PayloadWithByteCount(payloadBytes));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new AllowSnapshotRecoveryPolicy(Tenant),
                SnapshotRecoveryMaterializer = materializer,
                SnapshotRecoveryLimits = new() { MaximumPayloadBytes = SnapshotLogicalCapacityPayloadBytes, MaximumLogicalBytes = maximumLogicalBytes },
            });
        var seed = await SeedSnapshotRecoveryFrontierAsync(hub, subscriptionId, pendingOperations[0]);
        var request = SnapshotRecoveryRequest(subscriptionId, seed.ExpiredCursor, pendingOperations) with
        {
            MaximumResponseBytes = maximumResponseBytes,
        };

        return await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None);
    }

    /// <summary>Creates a retained expired proof, then advances the stream to a distinct recovery frontier.</summary>
    /// <param name="hub">The configured hub.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="firstOperation">The optional first pending operation.</param>
    /// <returns>The seeded expired cursor and current frontier cursor.</returns>
    private static async Task<SnapshotRecoverySeed> SeedSnapshotRecoveryFrontierAsync(
        ServerStreamHub hub,
        SubscriptionId subscriptionId,
        SyncOperation? firstOperation = null)
    {
        var operation = firstOperation ?? Operation(1, PayloadA);
        _ = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var expired = await ReadFirstBatchAsync(hub, new(Tenant, Client), subscriptionId);
        _ = await hub.ApplyOperationsAsync(Batch(Operation(SecondOperationSeed, PayloadB)), new(Tenant, Client), CancellationToken.None);
        return new(expired.NextCursor);
    }

    /// <summary>Creates a snapshot recovery request for the test stream.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expiredCursor">The expired cursor.</param>
    /// <param name="pendingOperations">The pending operation intents.</param>
    /// <returns>The snapshot recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest SnapshotRecoveryRequest(
        SubscriptionId subscriptionId,
        string? expiredCursor,
        params SyncOperation[] pendingOperations) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = SnapshotClientContract,
            ClientStateSchemaVersion = SingleCount,
            SnapshotFormatVersion = SingleCount,
            PendingOperations = pendingOperations,
            MaximumResponseBytes = SnapshotMaximumResponseBytes,
        };

    /// <summary>Runs snapshot recovery for the default test client.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expiredCursor">The expired cursor.</param>
    /// <returns>The recovery result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
        ServerStreamHub hub,
        SubscriptionId subscriptionId,
        string? expiredCursor) =>
        ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            SnapshotRecoveryRequest(subscriptionId, expiredCursor),
            new(Tenant, Client),
            CancellationToken.None);

    /// <summary>Creates a payload envelope with a short valid hash and a chosen byte count.</summary>
    /// <param name="byteCount">The payload byte count.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope PayloadWithByteCount(int byteCount) =>
        new(Contract, 1, ContentType, new byte[byteCount], SnapshotClientPayload);

    /// <summary>Asserts two payload envelopes have matching metadata and bytes.</summary>
    /// <param name="actual">The actual payload.</param>
    /// <param name="expected">The expected payload.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertSnapshotPayloadAsync(PayloadEnvelope? actual, PayloadEnvelope expected)
    {
        await Assert.That(actual?.ContractId).IsEqualTo(expected.ContractId);
        await Assert.That(actual?.SchemaVersion).IsEqualTo(expected.SchemaVersion);
        await Assert.That(actual?.ContentType).IsEqualTo(expected.ContentType);
        await Assert.That(actual?.PayloadHash).IsEqualTo(expected.PayloadHash);
        await Assert.That(actual?.PayloadLength).IsEqualTo(expected.PayloadLength);
        await Assert.That(actual?.Payload.ToArray().SequenceEqual(expected.Payload.ToArray())).IsTrue();
    }

    /// <summary>Records snapshot materialization contexts and returns a fixed payload.</summary>
    /// <param name="clientState">The materialized client state.</param>
    private sealed class RecordingSnapshotMaterializer(PayloadEnvelope clientState) : IServerSnapshotMaterializer
    {
        /// <summary>The observed materialization contexts.</summary>
        private readonly List<ServerSnapshotMaterializationContext> _contexts = [];

        /// <summary>Gets the observed materialization contexts.</summary>
        internal List<ServerSnapshotMaterializationContext> Contexts => _contexts;

        /// <summary>Gets the number of materialization calls.</summary>
        internal int CallCount => _contexts.Count;

        /// <inheritdoc/>
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            _contexts.Add(context);
            return ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = clientState });
        }
    }

    /// <summary>Returns a sequence of materialization results.</summary>
    /// <param name="results">The materialization results to return.</param>
    private sealed class SequencedSnapshotMaterializer(params ServerSnapshotMaterializationResult[] results) : IServerSnapshotMaterializer
    {
        /// <summary>The next materialization result index.</summary>
        private int _index;

        /// <summary>Initializes a new instance of the <see cref="SequencedSnapshotMaterializer"/> class.</summary>
        /// <param name="clientStates">The client states to materialize.</param>
        internal SequencedSnapshotMaterializer(params PayloadEnvelope[] clientStates)
            : this(CreateMaterializedResults(clientStates))
        {
        }

        /// <inheritdoc/>
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken)
        {
            _ = context;
            _ = cancellationToken;
            var currentIndex = _index;
            _index++;
            var index = Math.Min(currentIndex, results.Length - 1);
            return ValueTask.FromResult(results[index]);
        }

        /// <summary>Creates materialized results for a sequence of payloads.</summary>
        /// <param name="clientStates">The client states.</param>
        /// <returns>The materialized results.</returns>
        private static ServerSnapshotMaterializationResult[] CreateMaterializedResults(PayloadEnvelope[] clientStates)
        {
            var materialized = new ServerSnapshotMaterializationResult[clientStates.Length];
            for (var index = 0; index < materialized.Length; index++)
            {
                materialized[index] = new() { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = clientStates[index] };
            }

            return materialized;
        }
    }

    /// <summary>Returns null materialization output through the public materializer interface.</summary>
    private sealed class NullSnapshotMaterializer : IServerSnapshotMaterializer
    {
        /// <inheritdoc/>
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken)
        {
            _ = context;
            _ = cancellationToken;
            return ValueTask.FromResult<ServerSnapshotMaterializationResult>(null!);
        }
    }

    /// <summary>Allows snapshot recovery for one trusted tenant.</summary>
    /// <param name="tenant">The trusted tenant.</param>
    private sealed class AllowSnapshotRecoveryPolicy(string tenant) : IServerSnapshotRecoveryAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
            ServerAuthenticatedClient client,
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));
    }

    /// <summary>Returns a snapshot recovery tenant different from the host-authenticated principal.</summary>
    private sealed class TenantMismatchSnapshotRecoveryPolicy : IServerSnapshotRecoveryAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
            ServerAuthenticatedClient client,
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));
    }

    /// <summary>Blocks materialization until released by the test.</summary>
    /// <param name="clientState">The materialized client state.</param>
    private sealed class BlockingSnapshotMaterializer(PayloadEnvelope clientState) : IServerSnapshotMaterializer
    {
        /// <summary>Tracks materialization start.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases materialization.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken)
        {
            _ = context;
            _ = _started.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new() { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = clientState };
        }

        /// <summary>Releases the blocked materialization call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until materialization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;
    }

    /// <summary>Provides mutable UTC time for snapshot recovery compaction tests.</summary>
    /// <param name="utcNow">The initial UTC time.</param>
    private sealed class MutableSnapshotTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC time.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Sets the current UTC time.</summary>
        /// <param name="utcNow">The new UTC time.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }

    /// <summary>Stores the retained cursor proof for a seeded stream.</summary>
    /// <param name="ExpiredCursor">The retained cursor supplied as the expired proof.</param>
    private sealed record SnapshotRecoverySeed(string ExpiredCursor);
}
