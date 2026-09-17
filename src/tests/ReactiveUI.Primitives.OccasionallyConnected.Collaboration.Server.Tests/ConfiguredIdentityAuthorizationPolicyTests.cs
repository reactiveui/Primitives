// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="ConfiguredIdentityAuthorizationPolicy"/>.</summary>
public sealed class ConfiguredIdentityAuthorizationPolicyTests
{
    /// <summary>The authenticated tenant identifier used by policy tests.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The authenticated client identifier used by policy tests.</summary>
    private const string Client = "client-a";

    /// <summary>The initial client sequence used by operation authorization tests.</summary>
    private const long InitialClientSequence = 1;

    /// <summary>The deterministic operation timestamp used by operation authorization tests.</summary>
    private static readonly DateTimeOffset OperationTimestampUtc = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies the authorization policy preserves the host-authenticated identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AuthorizeSubscribeAsyncReturnsHostAuthenticatedScope()
    {
        var policy = new ConfiguredIdentityAuthorizationPolicy();
        var request = new RemoteSubscribeRequest(CollaborationStreamRegistrations.ActivityStream, SubscriptionId.New(), null, StartPosition.Latest);

        var scope = await policy.AuthorizeSubscribeAsync(new(Tenant, Client), request, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(scope.TenantId).IsEqualTo(Tenant);
        await Assert.That(scope.ClientId).IsEqualTo(Client);
    }

    /// <summary>Verifies publish authorization uses the same host-authenticated identity as subscriptions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AuthorizePublishAsyncReturnsHostAuthenticatedScope()
    {
        var policy = new ConfiguredIdentityAuthorizationPolicy();
        var operation = CreateOperation();
        var batch = new SyncBatch(Guid.NewGuid(), [operation]);

        var scope = await policy.AuthorizePublishAsync(new(Tenant, Client), batch, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(scope.TenantId).IsEqualTo(Tenant);
        await Assert.That(scope.ClientId).IsEqualTo(Client);
    }

    /// <summary>Verifies per-operation authorization uses the same host-authenticated identity as subscriptions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AuthorizeOperationAsyncReturnsHostAuthenticatedScope()
    {
        var policy = new ConfiguredIdentityAuthorizationPolicy();

        var scope = await policy.AuthorizeOperationAsync(new(Tenant, Client), CreateOperation(), CancellationToken.None).ConfigureAwait(false);

        await Assert.That(scope.TenantId).IsEqualTo(Tenant);
        await Assert.That(scope.ClientId).IsEqualTo(Client);
    }

    /// <summary>Verifies cancellation is preserved by the authorization boundary.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AuthorizeAcknowledgeAsyncPreservesCancellation()
    {
        var policy = new ConfiguredIdentityAuthorizationPolicy();
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() =>
            policy.AuthorizeAcknowledgeAsync(
                new(Tenant, Client),
                new(SubscriptionId.New(), CollaborationStreamRegistrations.ActivityStream, "cursor-1"),
                cancellation.Token).AsTask());
    }

    /// <summary>Creates a valid activity operation for authorization tests.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = InitialClientSequence,
            TimestampUtc = OperationTimestampUtc,
            Type = SyncOperationType.Custom,
            Payload = ActivityPayloads.Create("""{"status":"ready"}"""),
        };
}
