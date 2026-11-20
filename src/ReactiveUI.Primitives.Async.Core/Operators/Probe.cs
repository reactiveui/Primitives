// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Probe extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Sampling operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Emits the latest element once the period has passed since the element that started the timer.</summary>
        /// <param name="period">The sampling period. Must be non-negative.</param>
        /// <returns>A sequence carrying the latest element once each period elapses; a quiet source sends nothing.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="period"/> is negative.</exception>
        /// <remarks>An element still waiting when the source completes is sent before the completion.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Probe(TimeSpan period) =>
            ProbeSignal<T>.Create(source, period);

        /// <summary>Emits the latest element once the period has passed since the element that started the timer.</summary>
        /// <param name="period">The sampling period. Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider. If null, <see cref="TimeProvider.System"/> is used.</param>
        /// <returns>A sequence carrying the latest element once each period elapses; a quiet source sends nothing.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="period"/> is negative.</exception>
        /// <remarks>An element still waiting when the source completes is sent before the completion.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Probe(TimeSpan period, TimeProvider? timeProvider) =>
            ProbeSignal<T>.Create(source, period, timeProvider);

        /// <summary>Another name for <c>Probe</c>.</summary>
        /// <param name="interval">The sampling period. Must be non-negative.</param>
        /// <returns>A sequence carrying the latest element once each period elapses.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="interval"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Sample(TimeSpan interval) =>
            ProbeSignal<T>.Create(source, interval);

        /// <summary>Another name for <c>Probe</c>, taking a time provider.</summary>
        /// <param name="interval">The sampling period. Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider. If null, <see cref="TimeProvider.System"/> is used.</param>
        /// <returns>A sequence carrying the latest element once each period elapses.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="interval"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Sample(TimeSpan interval, TimeProvider? timeProvider) =>
            ProbeSignal<T>.Create(source, interval, timeProvider);
    }
}
