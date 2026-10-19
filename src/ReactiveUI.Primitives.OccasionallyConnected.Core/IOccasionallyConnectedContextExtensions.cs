// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Convenience overloads for <see cref="IOccasionallyConnectedContext"/> that use <see cref="CancellationToken.None"/>.</summary>
public static class IOccasionallyConnectedContextExtensions
{
    /// <summary>Convenience overloads for an occasionally connected context.</summary>
    /// <param name="context">The occasionally connected context.</param>
    extension(IOccasionallyConnectedContext context)
    {
        /// <summary>Starts synchronization and stream lifecycle work.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync() => context.StartAsync(CancellationToken.None);

        /// <summary>Stops synchronization and stream lifecycle work.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync() => context.StopAsync(CancellationToken.None);
    }
}
