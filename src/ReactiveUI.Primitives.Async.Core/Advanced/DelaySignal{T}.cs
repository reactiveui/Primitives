// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that delays each element notification by a specified duration.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="delayInterval">The time span by which to delay each element notification.</param>
/// <param name="timeProvider">The time provider that schedules the delay.</param>
[System.Diagnostics.DebuggerDisplay("DelaySignal<{typeof(T).Name,nq}>")]
public sealed class DelaySignal<T>(IObservableAsync<T> source, TimeSpan delayInterval, TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a time-shifted sequence using <see cref="TimeProvider.System"/>, returning <paramref name="source"/> unchanged when <paramref name="delayInterval"/> is zero.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="delayInterval">The non-negative delay applied to each element.</param>
    /// <returns>An observable sequence with element notifications delayed by <paramref name="delayInterval"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(IObservableAsync<T> source, TimeSpan delayInterval) =>
        Create(source, delayInterval, null);

    /// <summary>Creates a time-shifted sequence using the supplied time provider, returning <paramref name="source"/> unchanged when <paramref name="delayInterval"/> is zero.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="delayInterval">The non-negative delay applied to each element.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>An observable sequence with element notifications delayed by <paramref name="delayInterval"/>.</returns>
    public static IObservableAsync<T> Create(IObservableAsync<T> source, TimeSpan delayInterval, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(delayInterval, TimeSpan.Zero);

        return delayInterval == TimeSpan.Zero
            ? source
            : new DelaySignal<T>(source, delayInterval, timeProvider ?? TimeProvider.System);
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        DelayWitness<T> delayObserver = new(observer, delayInterval, timeProvider, cancellationToken);
        return WitnessSubscription.SubscribeAsync(source, delayObserver, observer, cancellationToken);
    }
}
