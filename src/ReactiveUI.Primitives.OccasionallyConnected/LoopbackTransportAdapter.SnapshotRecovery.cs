// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>In-memory transport adapter for occasionally connected synchronization tests and local loopback flows.</summary>
/// <content>Snapshot recovery implementation.</content>
public sealed partial class LoopbackTransportAdapter
{
    /// <summary>Represents one loopback transport session.</summary>
    /// <content>Snapshot recovery implementation.</content>
    private sealed partial class LoopbackTransportSession : IRemoteSnapshotRecoverySession
    {
        /// <inheritdoc/>
        /// <exception cref="ArgumentNullException"><paramref name="request"/> is <see langword="null"/>.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">
        /// The peer did not advertise snapshot recovery, or the session has reached its active request limit.
        /// </exception>
        public async ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(request);
            var snapshotHub = options.SnapshotRecoveryHub;
            if ((options.PeerCapabilities.Features & RemoteTransportCapabilities.SnapshotRecovery) == 0 || snapshotHub is null)
            {
                throw new InvalidOperationException("The loopback peer does not support snapshot recovery.");
            }

            using var lease = Admit(PushOperation, cancellationToken);
            return await snapshotHub.GetSnapshotAsync(request, options.AuthenticatedClient, lease.Token).ConfigureAwait(false);
        }
    }
}
