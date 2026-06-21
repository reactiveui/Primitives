// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive.Signals;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>System.Reactive pulse aliases for periodic signal factories.</summary>
public static partial class Signal
{
    /// <summary>Emits monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Pulse(TimeSpan period)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        return new EverySignal(period, ThreadPoolSequencer.Instance);
    }

    /// <summary>Emits scheduled, monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The period between ticks.</param>
    /// <param name="scheduler">The scheduler used to emit ticks.</param>
    /// <returns>An observable sequence that emits periodic ticks.</returns>
    public static IObservable<long> Pulse(TimeSpan period, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return new EverySignal(period, scheduler);
    }
}
