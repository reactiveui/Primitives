// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Throttle (debounce) extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Throttle (debounce) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Forwards the latest element after a full quiet period.</summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Throttle(TimeSpan dueTime) =>
            ThrottleSignal<T>.Create(source, dueTime);

        /// <summary>Forwards the latest element after a full quiet period.</summary>
        /// <param name="dueTime">The time span that must elapse after the last element before it is forwarded.
        /// Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider for controlling timing. If null, <see cref="TimeProvider.System"/>
        /// is used.</param>
        /// <returns>An observable sequence containing only those elements that are not followed by another
        /// element within the specified due time.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="dueTime"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Throttle(TimeSpan dueTime, TimeProvider? timeProvider) =>
            ThrottleSignal<T>.Create(source, dueTime, timeProvider);
    }

    /// <summary>Delay operators for a relative duration.</summary>
    /// <param name="delay">The duration to delay.</param>
    extension(TimeSpan delay)
    {
        /// <summary>Creates a cancellable delay using the supplied time provider.</summary>
        /// <param name="timeProvider">The time provider to use for the delay.</param>
        /// <param name="cancellationToken">A token to cancel the delay.</param>
        /// <returns>A <see cref="ValueTask"/> that completes after the specified delay.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask DelayAsync(
            TimeProvider timeProvider,
            CancellationToken cancellationToken) =>
            PooledDelaySource.DelayAsync(delay, timeProvider, cancellationToken);
    }
}
