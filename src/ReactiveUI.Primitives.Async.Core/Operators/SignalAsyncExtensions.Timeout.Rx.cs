// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible timeout names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Timeout operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Applies a dueTime policy to the observable sequence. If the next element is not received within
        /// the specified time span, the sequence completes with a <see cref="TimeoutException"/>.
        /// </summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> if any inter-element interval exceeds the specified dueTime.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            var timeProvider = TimeProvider.System;
            return new TimeoutSignal<T>(@this, dueTime, timeProvider);
        }

        /// <summary>
        /// Applies a dueTime policy to the observable sequence. If the next element is not received within
        /// the specified time span, the sequence completes with a <see cref="TimeoutException"/>.
        /// </summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> if any inter-element interval exceeds the specified dueTime.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime, TimeProvider? timeProvider)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutSignal<T>(@this, dueTime, timeProvider ?? TimeProvider.System);
        }

        /// <summary>
        /// Applies a dueTime policy to the observable sequence. If the next element is not received within
        /// the specified time span, the sequence switches to the specified fallback observable.
        /// </summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="fallback">The fallback observable to switch to when a dueTime occurs. Cannot be null.</param>
        /// <returns>An observable sequence that mirrors the source, switching to the fallback sequence
        /// if any inter-element interval exceeds the specified dueTime.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="fallback"/> is null.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime, IObservableAsync<T> fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(fallback);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            var timeProvider = TimeProvider.System;
            return new TimeoutWithFallbackSignal<T>(@this, dueTime, fallback, timeProvider);
        }

        /// <summary>
        /// Applies a dueTime policy to the observable sequence. If the next element is not received within
        /// the specified time span, the sequence switches to the specified fallback observable.
        /// </summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="fallback">The fallback observable to switch to when a dueTime occurs. Cannot be null.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence that mirrors the source, switching to the fallback sequence
        /// if any inter-element interval exceeds the specified dueTime.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="fallback"/> is null.</exception>
        public IObservableAsync<T> Timeout(
            TimeSpan dueTime,
            IObservableAsync<T> fallback,
            TimeProvider? timeProvider)
        {
            ArgumentExceptionHelper.ThrowIfNull(fallback);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutWithFallbackSignal<T>(@this, dueTime, fallback, timeProvider ?? TimeProvider.System);
        }
    }
}
