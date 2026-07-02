// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Components;

namespace ReactiveUI.Primitives.Blazor.Concurrency;

/// <summary>Convenience helpers for Blazor renderer sequencers.</summary>
public static class BlazorRendererSequencerExtensions
{
    /// <summary>Extension methods for Blazor renderer dispatchers.</summary>
    /// <param name="dispatcher">The dispatcher to adapt.</param>
    extension(Dispatcher dispatcher)
    {
        /// <summary>Adapts a Blazor renderer dispatcher to a sequencer.</summary>
        /// <returns>A sequencer that schedules through <paramref name="dispatcher"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
        public BlazorRendererSequencer ToSequencer()
        {
            ArgumentExceptionHelper.ThrowIfNull(dispatcher);

            return new(dispatcher);
        }
    }
}
