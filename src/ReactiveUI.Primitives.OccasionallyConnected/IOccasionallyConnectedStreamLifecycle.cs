// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Exposes lifecycle methods common to typed stream facades.</summary>
internal interface IOccasionallyConnectedStreamLifecycle : IAsyncDisposable
{
    /// <summary>Starts stream synchronization work.</summary>
    /// <param name="cancellationToken">The token used to cancel startup.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken);

    /// <summary>Stops stream synchronization work.</summary>
    /// <param name="cancellationToken">The token used to cancel shutdown waiting.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken);
}
