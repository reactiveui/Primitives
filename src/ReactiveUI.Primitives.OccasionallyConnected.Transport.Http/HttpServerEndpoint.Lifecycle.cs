// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Disposes endpoint-owned lifecycle resources and drains active requests.</summary>
    /// <returns>The asynchronous disposal operation.</returns>
    private async Task DisposeAsyncCore()
    {
        Exception? failure = null;
        try
        {
            await _shutdown.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        await _requestGate.DisposeAsync().ConfigureAwait(false);
        await _acknowledgementGate.DisposeAsync().ConfigureAwait(false);
        await _subscriptionGate.DisposeAsync().ConfigureAwait(false);
        await _replayCoordinator.DisposeAsync().ConfigureAwait(false);
        _shutdown.Dispose();

        _ = failure is null
            ? _disposeCompleted.TrySetResult(null)
            : _disposeCompleted.TrySetException(failure);
    }
}
