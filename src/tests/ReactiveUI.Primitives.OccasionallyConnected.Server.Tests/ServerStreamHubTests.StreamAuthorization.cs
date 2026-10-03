// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Stream-scoped authorization tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The guard timeout that prevents stream authorization tests from hanging.</summary>
    private static readonly TimeSpan StreamAuthorizationGuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a batch for an unauthorized stream is rejected before any effect and does not poison later authorized work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsUnauthorizedStreamBeforeAnyEffect()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new StreamScopedPolicy(Tenant, Stream), domain));
        var forbidden = Operation(SecondOperationSeed, PayloadB) with { StreamId = OtherStream };

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => hub.ApplyOperationsAsync(Batch(forbidden), new(Tenant, Client), CancellationToken.None).AsTask());
        var afterDenial = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);

        await Assert.That(afterDenial.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
    }

    /// <summary>Verifies a subscription to an unauthorized stream is rejected before any event is read.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsUnauthorizedStreamBeforeLookup()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new StreamScopedPolicy(Tenant, Stream), new RecordingDomainHandler()));
        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        using var timeout = new CancellationTokenSource(StreamAuthorizationGuardTimeout);
        var enumerable = hub.SubscribeStreamAsync(
            new(OtherStream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(Tenant, Client),
            timeout.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(timeout.Token);

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(() => enumerator.MoveNextAsync().AsTask());
    }

    /// <summary>Verifies an acknowledgement for an unauthorized stream is rejected before persistence.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AcknowledgeAsyncRejectsUnauthorizedStreamBeforePersistence()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new StreamScopedPolicy(Tenant, Stream), new RecordingDomainHandler()));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => hub.AcknowledgeAsync(new(SubscriptionId.New(), OtherStream, MissingCursor), new(Tenant, Client), CancellationToken.None).AsTask());
    }

    /// <summary>Verifies snapshot recovery for an unauthorized stream is rejected before materialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetSnapshotAsyncRejectsUnauthorizedStreamBeforeMaterialization()
    {
        var materializer = new RecordingSnapshotMaterializer(Payload(SnapshotClientPayload));
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new StreamScopedPolicy(Tenant, Stream), new RecordingDomainHandler()) with
            {
                SnapshotRecoveryAuthorizationPolicy = new StreamScopedPolicy(Tenant, Stream),
                SnapshotRecoveryMaterializer = materializer,
            });
        var request = SnapshotRecoveryRequest(SubscriptionId.New(), MissingCursor) with { StreamId = OtherStream };

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(request, new(Tenant, Client), CancellationToken.None).AsTask());
        await Assert.That(materializer.CallCount).IsEqualTo(0);
    }

    /// <summary>Authorizes one tenant for one stream and denies every other stream.</summary>
    /// <param name="tenant">The trusted tenant.</param>
    /// <param name="allowedStream">The only authorized stream.</param>
    private sealed class StreamScopedPolicy(string tenant, StreamId allowedStream) : IServerStreamAuthorizationPolicy, IServerSnapshotRecoveryAuthorizationPolicy
    {
        /// <inheritdoc/>
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken)
        {
            for (var index = 0; index < batch.Operations.Count; index++)
            {
                Demand(batch.Operations[index].StreamId);
            }

            return Scope(client);
        }

        /// <inheritdoc/>
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken)
        {
            Demand(operation.StreamId);
            return Scope(client);
        }

        /// <inheritdoc/>
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            Demand(request.StreamId);
            return Scope(client);
        }

        /// <inheritdoc/>
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken)
        {
            Demand(acknowledgement.StreamId);
            return Scope(client);
        }

        /// <inheritdoc/>
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
            ServerAuthenticatedClient client,
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken)
        {
            Demand(request.StreamId);
            return Scope(client);
        }

        /// <summary>Denies every stream other than the authorized stream.</summary>
        /// <param name="streamId">The requested stream.</param>
        /// <exception cref="UnauthorizedAccessException">The stream is not authorized.</exception>
        private void Demand(StreamId streamId)
        {
            if (streamId != allowedStream)
            {
                throw new UnauthorizedAccessException("stream denied");
            }
        }

        /// <summary>Creates the trusted scope for the authenticated client.</summary>
        /// <param name="client">The authenticated client.</param>
        /// <returns>The trusted scope.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<ServerStreamAuthorizationScope> Scope(ServerAuthenticatedClient client) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(tenant, client.ClientId));
    }
}
