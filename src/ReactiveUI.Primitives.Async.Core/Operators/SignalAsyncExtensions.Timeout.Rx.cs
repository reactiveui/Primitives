// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible timeout names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Timeout operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Completes with a TimeoutException when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> if any inter-element interval exceeds the specified interval.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutSignal<T>(source, dueTime, TimeProvider.System);
        }

        /// <summary>Completes with a TimeoutException when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> if any inter-element interval exceeds the specified interval.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime, TimeProvider? timeProvider)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutSignal<T>(source, dueTime, timeProvider ?? TimeProvider.System);
        }

        /// <summary>Switches to the fallback sequence when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="fallback">The fallback observable to switch to when a timeout occurs. Cannot be null.</param>
        /// <returns>An observable sequence that mirrors the source, switching to the fallback sequence
        /// if any inter-element interval exceeds the specified interval.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fallback"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> Timeout(TimeSpan dueTime, IObservableAsync<T> fallback)
        {
            ArgumentExceptionHelper.ThrowIfNull(fallback);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutWithFallbackSignal<T>(source, dueTime, fallback, TimeProvider.System);
        }

        /// <summary>Switches to the fallback sequence when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <param name="fallback">The fallback observable to switch to when a timeout occurs. Cannot be null.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence that mirrors the source, switching to the fallback sequence
        /// if any inter-element interval exceeds the specified interval.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative or zero.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="fallback"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> Timeout(
            TimeSpan dueTime,
            IObservableAsync<T> fallback,
            TimeProvider? timeProvider)
        {
            ArgumentExceptionHelper.ThrowIfNull(fallback);
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

            return new TimeoutWithFallbackSignal<T>(source, dueTime, fallback, timeProvider ?? TimeProvider.System);
        }
    }
}
