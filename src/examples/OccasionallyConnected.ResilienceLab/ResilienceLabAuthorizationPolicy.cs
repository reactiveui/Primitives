// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Authorizes every lab stream and snapshot recovery call for one trusted tenant.</summary>
/// <param name="tenantId">The trusted tenant identifier.</param>
[DebuggerDisplay("{_tenantId,nq}")]
internal sealed class ResilienceLabAuthorizationPolicy(string tenantId) : IServerStreamAuthorizationPolicy, IServerSnapshotRecoveryAuthorizationPolicy
{
    /// <summary>The trusted tenant identifier.</summary>
    private readonly string _tenantId = tenantId;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
        ServerAuthenticatedClient client,
        SyncBatch batch,
        CancellationToken cancellationToken) =>
        Authorize(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
        ServerAuthenticatedClient client,
        SyncOperation operation,
        CancellationToken cancellationToken) =>
        Authorize(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
        ServerAuthenticatedClient client,
        RemoteSubscribeRequest request,
        CancellationToken cancellationToken) =>
        Authorize(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
        ServerAuthenticatedClient client,
        ReceiveAcknowledgement acknowledgement,
        CancellationToken cancellationToken) =>
        Authorize(client, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
        ServerAuthenticatedClient client,
        RemoteSnapshotRecoveryRequest request,
        CancellationToken cancellationToken) =>
        Authorize(client, cancellationToken);

    /// <summary>Creates the authorized scope for one trusted client.</summary>
    /// <param name="client">The authenticated client.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The authorization scope.</returns>
    private ValueTask<ServerStreamAuthorizationScope> Authorize(
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(new ServerStreamAuthorizationScope(_tenantId, client.ClientId));
    }
}
