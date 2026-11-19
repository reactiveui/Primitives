// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes asynchronous side-effect callbacks for each notification.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="onNext">An asynchronous callback to invoke for each element, or null to take no action on elements.</param>
/// <param name="onErrorResume">An asynchronous callback to invoke on error, or null to take no action on errors.</param>
/// <param name="onCompleted">An asynchronous callback to invoke on completion, or null to take no action on completion.</param>
[System.Diagnostics.DebuggerDisplay("TapAsyncSignal<{typeof(T).Name,nq}>")]
public sealed class TapAsyncSignal<T>(
    IObservableAsync<T> source,
    Func<T, CancellationToken, ValueTask>? onNext,
    Func<Exception, CancellationToken, ValueTask>? onErrorResume,
    Func<Result, ValueTask>? onCompleted) : IObservableAsync<T>
{
    /// <summary>Creates a side-effect sequence for element notifications, returning <paramref name="source"/> unchanged when <paramref name="onNext"/> is <see langword="null"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The asynchronous callback invoked for each element.</param>
    /// <returns>An observable sequence invoking <paramref name="onNext"/> when non-null.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext) =>
        onNext is null ? source : new TapAsyncSignal<T>(source, onNext, null, null);

    /// <summary>Creates a side-effect sequence for notifications, returning <paramref name="source"/> unchanged when all callbacks are <see langword="null"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The asynchronous callback invoked for each element.</param>
    /// <param name="onErrorResume">The asynchronous callback invoked on resumable error.</param>
    /// <param name="onCompleted">The asynchronous callback invoked on completion.</param>
    /// <returns>An observable sequence invoking any supplied callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask>? onNext,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        Func<Result, ValueTask>? onCompleted) =>
        onNext is null && onErrorResume is null && onCompleted is null
            ? source
            : new TapAsyncSignal<T>(source, onNext, onErrorResume, onCompleted);

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        AsyncSideEffectWitness<T> doObserver = new(observer, onNext, onErrorResume, onCompleted);
        return WitnessSubscription.SubscribeAsync(source, doObserver, observer, cancellationToken);
    }
}
