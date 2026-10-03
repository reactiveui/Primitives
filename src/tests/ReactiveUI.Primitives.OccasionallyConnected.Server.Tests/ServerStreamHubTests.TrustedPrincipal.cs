// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Trusted-principal authorization tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The guard timeout that prevents subscription tests from hanging.</summary>
    private const int SubscribeGuardTimeoutSeconds = 5;

    /// <summary>Verifies a publish scope must match the host-authenticated tenant before domain effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsPublishTenantMismatchBeforeEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new TenantMismatchPolicy(), domain));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => ApplyOperationsWithPrincipalAsync(hub, Batch(Operation(1, PayloadA)), Tenant, Client).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies an operation tenant mismatch rejects the whole publish before any operation effect.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsOperationTenantMismatchBeforeAnyEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new OperationTenantMismatchPolicy(), domain));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => ApplyOperationsWithPrincipalAsync(
                hub,
                Batch(Operation(1, PayloadA), Operation(SecondOperationSeed, PayloadB)),
                Tenant,
                Client).AsTask());
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies subscribe authorization cannot switch tenants before receive lookup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeStreamAsyncRejectsTenantMismatchBeforeLookup()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new TenantMismatchPolicy(), new RecordingDomainHandler()));
        _ = await ApplyOperationsWithPrincipalAsync(hub, Batch(Operation(1, PayloadA)), OtherTenant, Client);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(SubscribeGuardTimeoutSeconds));
        var enumerable = SubscribeWithPrincipal(
            hub,
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            Tenant,
            Client,
            timeout.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(timeout.Token);

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(() => enumerator.MoveNextAsync().AsTask());
    }

    /// <summary>Verifies acknowledgement authorization cannot switch tenants before acknowledgement persistence.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AcknowledgeAsyncRejectsTenantMismatchBeforePersistence()
    {
        await using var hub = ServerStreamHub.CreateInMemory(Options(new TenantMismatchPolicy(), new RecordingDomainHandler()));

        _ = await Assert.ThrowsExactlyAsync<UnauthorizedAccessException>(
            () => AcknowledgeWithPrincipalAsync(
                hub,
                new(SubscriptionId.New(), Stream, MissingCursor),
                Tenant,
                Client).AsTask());
    }

    /// <summary>Verifies a matching trusted principal still allows the authorized publish path.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncAllowsMatchingTrustedPrincipal()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));

        var result = await ApplyOperationsWithPrincipalAsync(hub, Batch(Operation(1, PayloadA)), Tenant, Client);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(domain.CallCount).IsEqualTo(1);
    }

    /// <summary>Invokes the trusted-principal publish overload.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="batch">The batch.</param>
    /// <param name="tenantId">The trusted tenant.</param>
    /// <param name="clientId">The trusted client.</param>
    /// <returns>The server result.</returns>
    private static async ValueTask<ServerSyncResult> ApplyOperationsWithPrincipalAsync(
        ServerStreamHub hub,
        SyncBatch batch,
        string tenantId,
        string clientId)
    {
        var principal = new ServerAuthenticatedClient(tenantId, clientId);
        return await hub.ApplyOperationsAsync(batch, principal, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Invokes the trusted-principal acknowledgement overload.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="acknowledgement">The acknowledgement.</param>
    /// <param name="tenantId">The trusted tenant.</param>
    /// <param name="clientId">The trusted client.</param>
    /// <returns>The acknowledgement task.</returns>
    private static async ValueTask AcknowledgeWithPrincipalAsync(
        ServerStreamHub hub,
        ReceiveAcknowledgement acknowledgement,
        string tenantId,
        string clientId)
    {
        var principal = new ServerAuthenticatedClient(tenantId, clientId);
        await hub.AcknowledgeAsync(acknowledgement, principal, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Invokes the trusted-principal subscribe overload.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="request">The subscribe request.</param>
    /// <param name="tenantId">The trusted tenant.</param>
    /// <param name="clientId">The trusted client.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The event batch sequence.</returns>
    private static IAsyncEnumerable<RemoteEventBatch> SubscribeWithPrincipal(
        ServerStreamHub hub,
        RemoteSubscribeRequest request,
        string tenantId,
        string clientId,
        CancellationToken cancellationToken)
    {
        var principal = new ServerAuthenticatedClient(tenantId, clientId);
        return hub.SubscribeStreamAsync(request, principal, cancellationToken);
    }

    /// <summary>Returns a tenant different from the host-authenticated principal.</summary>
    private sealed class TenantMismatchPolicy : IServerStreamAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));
    }
}
