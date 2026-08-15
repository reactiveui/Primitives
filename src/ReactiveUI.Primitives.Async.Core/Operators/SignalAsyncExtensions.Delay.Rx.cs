// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible delay names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Delay operators that time-shift an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Time-shifts the observable sequence by the specified time span. Each element notification
        /// is delayed by the specified duration.
        /// </summary>
        /// <param name="delayInterval">The time span by which to delay each element notification. Must be non-negative.</param>
        /// <returns>An observable sequence with element notifications time-shifted by the specified duration.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="delayInterval"/> is negative.</exception>
        public IObservableAsync<T> Delay(TimeSpan delayInterval)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(delayInterval, TimeSpan.Zero);

            return delayInterval == TimeSpan.Zero
                ? source
                : new DelaySignal<T>(source, delayInterval, TimeProvider.System);
        }

        /// <summary>
        /// Time-shifts the observable sequence by the specified time span. Each element notification
        /// is delayed by the specified duration.
        /// </summary>
        /// <param name="delayInterval">The time span by which to delay each element notification. Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence with element notifications time-shifted by the specified duration.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="delayInterval"/> is negative.</exception>
        public IObservableAsync<T> Delay(TimeSpan delayInterval, TimeProvider? timeProvider)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(delayInterval, TimeSpan.Zero);

            return delayInterval == TimeSpan.Zero
                ? source
                : new DelaySignal<T>(source, delayInterval, timeProvider ?? TimeProvider.System);
        }
    }
}
