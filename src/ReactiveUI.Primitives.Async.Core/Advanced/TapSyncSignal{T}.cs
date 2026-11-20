// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes synchronous side-effect callbacks for each notification.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="onNext">An action to invoke for each element, or null to take no action on elements.</param>
/// <param name="onErrorResume">An action to invoke on error, or null to take no action on errors.</param>
/// <param name="onCompleted">An action to invoke on completion, or null to take no action on completion.</param>
[System.Diagnostics.DebuggerDisplay("TapSyncSignal<{typeof(T).Name,nq}>")]
public sealed class TapSyncSignal<T>(
    IObservableAsync<T> source,
    Action<T>? onNext,
    Action<Exception>? onErrorResume,
    Action<Result>? onCompleted) : IObservableAsync<T>
{
    /// <summary>Returns <paramref name="source"/> unchanged without attaching side effects.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>The supplied <paramref name="source"/> sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Identity(IObservableAsync<T> source) => source;

    /// <summary>Creates a side-effect sequence for element notifications, returning <paramref name="source"/> unchanged when <paramref name="onNext"/> is <see langword="null"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The action invoked for each element.</param>
    /// <returns>An observable sequence invoking <paramref name="onNext"/> when non-null.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(IObservableAsync<T> source, Action<T>? onNext) =>
        onNext is null ? source : new TapSyncSignal<T>(source, onNext, null, null);

    /// <summary>Creates a <see cref="TapSyncSignal{T}"/> that invokes parameterless completion alongside element and error callbacks.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The action invoked for each element.</param>
    /// <param name="onError">The action invoked on error.</param>
    /// <param name="onCompleted">The action invoked on completion.</param>
    /// <returns>A configured <see cref="TapSyncSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TapSyncSignal<T> Create(
        IObservableAsync<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        new(source, onNext, onError, _ => onCompleted());

    /// <summary>Creates a side-effect sequence for notifications, returning <paramref name="source"/> unchanged when all callbacks are <see langword="null"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The action invoked for each element.</param>
    /// <param name="onErrorResume">The action invoked on resumable error.</param>
    /// <param name="onCompleted">The action invoked on completion.</param>
    /// <returns>An observable sequence invoking any supplied callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(
        IObservableAsync<T> source,
        Action<T>? onNext,
        Action<Exception>? onErrorResume,
        Action<Result>? onCompleted) =>
        onNext is null && onErrorResume is null && onCompleted is null
            ? source
            : new TapSyncSignal<T>(source, onNext, onErrorResume, onCompleted);

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        SyncSideEffectWitness<T> doObserver = new(observer, onNext, onErrorResume, onCompleted);
        return WitnessSubscription.SubscribeAsync(source, doObserver, observer, cancellationToken);
    }
}
