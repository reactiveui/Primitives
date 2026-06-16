// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Dispatching;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Convenience helpers for MAUI dispatcher schedulers.</summary>
public static class MauiDispatcherSequencerExtensions
{
    /// <summary>Extension methods for MAUI dispatchers.</summary>
    /// <param name="dispatcher">The dispatcher to adapt.</param>
    extension(IDispatcher dispatcher)
    {
        /// <summary>Adapts a MAUI dispatcher to a coalescing <see cref="System.Reactive.Concurrency.IScheduler"/>.</summary>
        /// <returns>A scheduler that schedules through <paramref name="dispatcher"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
        public MauiDispatcherSequencer ToSequencer()
        {
            ArgumentNullException.ThrowIfNull(dispatcher);

            return new(dispatcher);
        }
    }
}
