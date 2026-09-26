// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.Hosting;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting;

/// <summary>Starts the occasionally connected context with the host and stops it gracefully when the host shuts down.</summary>
/// <param name="context">The hosted context.</param>
internal sealed class OccasionallyConnectedHostedService(IOccasionallyConnectedContext context) : IHostedService
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task StartAsync(CancellationToken cancellationToken) => context.StartAsync(cancellationToken).AsTask();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task StopAsync(CancellationToken cancellationToken) => context.StopAsync(cancellationToken).AsTask();
}
