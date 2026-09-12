// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an asynchronous observable sequence that emits a long integer value at each specified time interval.</summary>
    /// <param name="period">The time interval between emissions of values. Must be a positive duration.</param>
    /// <returns>An observable sequence that emits an increasing count, starting at 1, on every tick until the
    /// subscription is disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<long> Interval(TimeSpan period) =>
        new IntervalSignal(period, null);

    /// <summary>Creates an asynchronous observable sequence that emits a long integer value at each specified time interval.</summary>
    /// <param name="period">The time interval between emissions of values. Must be a positive duration.</param>
    /// <param name="timeProvider">An optional time provider used to control the timing of emissions. If null or set to TimeProvider.System, the
    /// system clock is used.</param>
    /// <returns>An observable sequence that emits an increasing count, starting at 1, on every tick until the
    /// subscription is disposed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<long> Interval(TimeSpan period, TimeProvider? timeProvider) =>
        new IntervalSignal(period, timeProvider);
}
