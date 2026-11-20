// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that mirrors the source but completes with a <see cref="TimeoutException"/> once an inter-element gap exceeds the configured interval.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="dueTime">The maximum allowed inter-element interval.</param>
/// <param name="timeProvider">The time provider used to schedule the deadline.</param>
[System.Diagnostics.DebuggerDisplay("TimeoutSignal<{typeof(T).Name,nq}>")]
public sealed class TimeoutSignal<T>(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="TimeoutSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval. Must be positive.</param>
    /// <returns>A configured <see cref="TimeoutSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TimeoutSignal<T> Create(IObservableAsync<T> source, TimeSpan dueTime) =>
        Create(source, dueTime, null);

    /// <summary>Creates a <see cref="TimeoutSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="dueTime">The maximum allowed inter-element interval. Must be positive.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="TimeoutSignal{T}"/>.</returns>
    public static TimeoutSignal<T> Create(IObservableAsync<T> source, TimeSpan dueTime, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThanOrEqual(dueTime, TimeSpan.Zero);

        return new(source, dueTime, timeProvider ?? TimeProvider.System);
    }

    /// <summary>Subscribes the specified observer and starts the deadline timer.</summary>
    /// <param name="observer">The observer to receive elements from the source.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        TimeoutWitness<T> timeoutObserver = new(observer, dueTime, timeProvider);
        var subscription = await WitnessSubscription.SubscribeAsync(source, timeoutObserver, observer, cancellationToken).ConfigureAwait(false);
        timeoutObserver.StartTimer(cancellationToken);
        return subscription;
    }
}
