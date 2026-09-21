// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Builds thread-pool sequencers whose immediate work runs on the calling thread.</summary>
internal static class InlineThreadPool
{
    /// <summary>Creates a sequencer that reads time and arms delays through <paramref name="timeProvider"/> and runs immediate work inline.</summary>
    /// <param name="timeProvider">The provider supplying time and the delay timer.</param>
    /// <returns>The sequencer.</returns>
    internal static ThreadPoolSequencer Create(TimeProvider timeProvider) =>
        new(timeProvider, static (callback, state) => callback(state));
}
