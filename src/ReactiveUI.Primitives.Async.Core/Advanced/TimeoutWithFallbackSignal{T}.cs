// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that mirrors the source but switches to a fallback observable once an inter-element gap exceeds the configured interval.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="dueTime">The maximum allowed inter-element interval.</param>
/// <param name="fallback">The observable to switch to when the interval elapses.</param>
/// <param name="timeProvider">The time provider used to schedule the deadline.</param>
[System.Diagnostics.DebuggerDisplay("TimeoutWithFallbackSignal<{typeof(T).Name,nq}>")]
public sealed class TimeoutWithFallbackSignal<T>(
    IObservableAsync<T> source,
    TimeSpan dueTime,
    IObservableAsync<T> fallback,
    TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="TimeoutWithFallbackSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval. Must be positive.</param>
    /// <param name="fallback">The fallback sequence used on timeout.</param>
    /// <returns>A configured <see cref="TimeoutWithFallbackSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TimeoutWithFallbackSignal<T> Create(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        IObservableAsync<T> fallback) =>
        Create(source, dueTime, fallback, null);

    /// <summary>Creates a <see cref="TimeoutWithFallbackSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval. Must be positive.</param>
    /// <param name="fallback">The fallback sequence used on timeout.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="TimeoutWithFallbackSignal{T}"/>.</returns>
    public static TimeoutWithFallbackSignal<T> Create(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        IObservableAsync<T> fallback,
        TimeProvider? timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(fallback);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

        return new(source, dueTime, fallback, timeProvider ?? TimeProvider.System);
    }

    /// <summary>Subscribes the specified observer by wrapping the source with a deadline and a catch-to-fallback.</summary>
    /// <param name="observer">The observer to receive elements.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        TimeoutSignal<T> withTimeout = new(source, dueTime, timeProvider);
        CatchSignal<T> withFallback = new(
            withTimeout,
            ex => ex is TimeoutException ? fallback : new SignalAsync.ThrowSignalAsync<T>(ex),
            null);

        return ((IObservableAsync<T>)withFallback).SubscribeAsync(observer.Wrap(), cancellationToken);
    }
}
