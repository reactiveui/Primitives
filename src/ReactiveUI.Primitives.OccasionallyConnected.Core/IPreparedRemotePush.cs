// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a validated remote push that has reserved transport capacity until sent or disposed.</summary>
public interface IPreparedRemotePush : IAsyncDisposable
{
    /// <summary>Gets the synchronization batch that was validated for this prepared push.</summary>
    SyncBatch Batch { get; }

    /// <summary>Gets the exact logical encoded size of the prepared batch in bytes.</summary>
    long EncodedSizeBytes { get; }

    /// <summary>Sends the prepared batch to the remote peer.</summary>
    /// <param name="cancellationToken">The token used to cancel the send.</param>
    /// <returns>The remote synchronization result.</returns>
    ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken);
}
