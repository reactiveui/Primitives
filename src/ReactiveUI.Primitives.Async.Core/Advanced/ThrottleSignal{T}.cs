// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that debounces the source sequence, only forwarding elements that are not followed by another element within the specified due time.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence to throttle.</param>
/// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
/// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
[System.Diagnostics.DebuggerDisplay("ThrottleSignal<{typeof(T).Name,nq}>")]
public sealed class ThrottleSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="ThrottleSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The source observable sequence to throttle.</param>
    /// <param name="dueTime">The non-negative quiet period.</param>
    /// <returns>A configured <see cref="ThrottleSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ThrottleSignal<T> Create(IObservableAsync<T> source, TimeSpan dueTime) =>
        Create(source, dueTime, null);

    /// <summary>Creates a <see cref="ThrottleSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The source observable sequence to throttle.</param>
    /// <param name="dueTime">The non-negative quiet period.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="ThrottleSignal{T}"/>.</returns>
    public static ThrottleSignal<T> Create(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(dueTime, TimeSpan.Zero);

        return new(source, dueTime, timeProvider ?? TimeProvider.System);
    }

    /// <summary>Subscribes the specified observer with throttle behavior applied.</summary>
    /// <param name="observer">The observer to receive throttled elements.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ThrottleWitness<T> throttleObserver = new(observer, dueTime, timeProvider);
        return WitnessSubscription.SubscribeAsync(source, throttleObserver, observer, cancellationToken);
    }
}
