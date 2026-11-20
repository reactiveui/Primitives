// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Debounces the source, dropping repeated values both before the delay and before each forward, so only the newest pending value is emitted.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The upstream observable.</param>
/// <param name="dueTime">The debounce window.</param>
/// <param name="timeProvider">The time provider for the debounce timer.</param>
[System.Diagnostics.DebuggerDisplay("ThrottleDistinctSignal<{typeof(T).Name,nq}>")]
public sealed class ThrottleDistinctSignal<T>(
    IObservableAsync<T> source,
    TimeSpan dueTime,
    TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="ThrottleDistinctSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="throttle">The non-negative debounce window.</param>
    /// <returns>A configured <see cref="ThrottleDistinctSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ThrottleDistinctSignal<T> Create(IObservableAsync<T> source, TimeSpan throttle) =>
        Create(source, throttle, null);

    /// <summary>Creates a <see cref="ThrottleDistinctSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="throttle">The non-negative debounce window.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="ThrottleDistinctSignal{T}"/>.</returns>
    public static ThrottleDistinctSignal<T> Create(
        IObservableAsync<T> source,
        TimeSpan throttle,
        TimeProvider? timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(throttle, TimeSpan.Zero);

        return new(source, throttle, timeProvider ?? TimeProvider.System);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new ThrottleDistinctWitness<T>(observer, dueTime, timeProvider, cancellationToken),
            observer,
            cancellationToken);
}
