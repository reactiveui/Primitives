// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Dispatching;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Convenience helpers for MAUI dispatcher sequencers.
/// </summary>
public static class MauiDispatcherSequencerMixins
{
    /// <summary>
    /// Adapts a MAUI dispatcher to an <see cref="ISequencer"/>.
    /// </summary>
    /// <param name="dispatcher">The dispatcher to adapt.</param>
    /// <returns>A sequencer that schedules through <paramref name="dispatcher"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public static MauiDispatcherSequencer ToSequencer(this IDispatcher dispatcher)
    {
        if (dispatcher == null)
        {
            throw new ArgumentNullException(nameof(dispatcher));
        }

        return new(dispatcher);
    }
}
