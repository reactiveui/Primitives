// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.UI.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Convenience helpers for WinUI dispatcher queue schedulers.</summary>
public static class DispatcherQueueSequencerExtensions
{
    /// <summary>Extension methods for <see cref="DispatcherQueue"/>.</summary>
    /// <param name="dispatcherQueue">The dispatcher queue to adapt.</param>
    extension(DispatcherQueue dispatcherQueue)
    {
        /// <summary>Adapts a WinUI dispatcher queue to a coalescing <see cref="System.Reactive.Concurrency.IScheduler"/>.</summary>
        /// <returns>A scheduler that schedules through <paramref name="dispatcherQueue"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dispatcherQueue"/> is <see langword="null"/>.</exception>
        public DispatcherQueueSequencer ToSequencer()
        {
            ArgumentNullException.ThrowIfNull(dispatcherQueue);

            return new(dispatcherQueue);
        }
    }
}
