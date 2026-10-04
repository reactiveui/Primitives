// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// Scopes every call to the principal the host authenticated. A real server would also check stream ownership here.
/// </summary>
internal sealed class TrustedClientAuthorizationPolicy : IServerStreamAuthorizationPolicy, IHttpReplayAuthorizer
{
    /// <summary>Gets the shared instance.</summary>
    internal static TrustedClientAuthorizationPolicy Instance { get; } = new();

    /// <inheritdoc/>
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(ServerAuthenticatedClient client, ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
        Scope(client);

    /// <inheritdoc/>
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(ServerAuthenticatedClient client, SyncOperation operation, CancellationToken cancellationToken) =>
        Scope(client);

    /// <inheritdoc/>
    public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(ServerAuthenticatedClient client, SyncBatch batch, CancellationToken cancellationToken) =>
        Scope(client);

    /// <inheritdoc/>
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(ServerAuthenticatedClient client, RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
        Scope(client);

    /// <inheritdoc/>
    public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
        new(string.Equals(context.Client.TenantId, SampleServer.Tenant, StringComparison.Ordinal));

    private static ValueTask<ServerStreamAuthorizationScope> Scope(ServerAuthenticatedClient client) =>
        new(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
}
