// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Maps the thread-pool sequencer onto System.Reactive's <see cref="ThreadPoolScheduler"/>.</summary>
internal static class ThreadPoolSequencer
{
    /// <summary>Gets the shared thread-pool scheduler instance.</summary>
    internal static IScheduler Instance => ThreadPoolScheduler.Instance;
}
