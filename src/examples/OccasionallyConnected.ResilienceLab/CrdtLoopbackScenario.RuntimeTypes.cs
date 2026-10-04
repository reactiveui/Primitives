// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Authorizes all lab operations for one trusted tenant.</summary>
    /// <param name="tenantId">The trusted tenant identifier.</param>
    [DebuggerDisplay("{_tenantId,nq}")]
    private sealed class LabAuthorizationPolicy(string tenantId) : IServerStreamAuthorizationPolicy
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

    /// <summary>Provides a deterministic mutable server clock.</summary>
    /// <param name="utcNow">The initial current time.</param>
    [DebuggerDisplay("{_utcNow,nq}")]
    private sealed class DeterministicTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current server time.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Advances the server clock by one deterministic tick.</summary>
        public void Advance() =>
            _utcNow = _utcNow.AddMilliseconds(ServerTickMilliseconds);
    }
}
