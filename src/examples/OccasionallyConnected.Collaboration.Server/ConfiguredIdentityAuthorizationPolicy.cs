// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Authorizes hub operations to the identity already authenticated by the host boundary.</summary>
[System.Diagnostics.DebuggerDisplay("Configured host identity policy")]
public sealed class ConfiguredIdentityAuthorizationPolicy : IServerStreamAuthorizationPolicy
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
        ServerAuthenticatedClient client,
        SyncBatch batch,
        CancellationToken cancellationToken) =>
        AuthorizeAsync(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
        ServerAuthenticatedClient client,
        SyncOperation operation,
        CancellationToken cancellationToken) =>
        AuthorizeAsync(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
        ServerAuthenticatedClient client,
        RemoteSubscribeRequest request,
        CancellationToken cancellationToken) =>
        AuthorizeAsync(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
        ServerAuthenticatedClient client,
        ReceiveAcknowledgement acknowledgement,
        CancellationToken cancellationToken) =>
        AuthorizeAsync(client, cancellationToken);

    /// <summary>Creates the authorization scope for an authenticated caller.</summary>
    /// <param name="client">The host-authenticated caller.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The authorized operation scope.</returns>
    private static ValueTask<ServerStreamAuthorizationScope> AuthorizeAsync(
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(client);
        return ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }
}
