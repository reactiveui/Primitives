// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using RxScheduler = System.Reactive.Concurrency.Scheduler;

namespace ReactiveUI.Primitives.Reactive.Concurrency;

/// <summary>Maps the shared Extensions source's built-in sequencers onto the matching System.Reactive schedulers.</summary>
internal static class Sequencer
{
    /// <summary>Gets a scheduler that schedules work as soon as possible on the current thread.</summary>
    internal static IScheduler CurrentThread => CurrentThreadScheduler.Instance;

    /// <summary>Gets a scheduler that schedules work immediately on the current thread.</summary>
    internal static IScheduler Immediate => ImmediateScheduler.Instance;

    /// <summary>Gets the default queueing scheduler for background work.</summary>
    internal static IScheduler Default => TaskPoolScheduler.Default;

    /// <summary>Normalizes the specified <see cref="TimeSpan"/> value to a positive value.</summary>
    /// <param name="timeSpan">The value to normalize.</param>
    /// <returns><paramref name="timeSpan"/> when zero or positive; otherwise <see cref="TimeSpan.Zero"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TimeSpan Normalize(TimeSpan timeSpan) => RxScheduler.Normalize(timeSpan);
}
