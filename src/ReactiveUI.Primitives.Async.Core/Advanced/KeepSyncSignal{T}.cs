// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Filters source elements synchronously using <paramref name="firstPredicate"/> without an async state machine.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="firstPredicate">The synchronous predicate.</param>
[System.Diagnostics.DebuggerDisplay("KeepSyncSignal<{typeof(T).Name,nq}>")]
public sealed class KeepSyncSignal<T>(
    IObservableAsync<T> source,
    Func<T, bool> firstPredicate) : IObservableAsync<T>, IMapFusableAsync<T>, IKeepFusableAsync<T>
{
    /// <summary>Builds or fuses a synchronous filter operator over <paramref name="source"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="predicate">The synchronous filter predicate.</param>
    /// <returns>An observable sequence of matching values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<T> Create(IObservableAsync<T> source, Func<T, bool> predicate)
    {
        if (predicate is not null)
        {
            if (source is SignalAsync.EmptySignalAsync<T> or SignalAsync.NeverSignalAsync<T>)
            {
                return source;
            }

            if (source is IKeepFusableAsync<T> fusable && fusable.TryFuseKeep(predicate) is { } fused)
            {
                return fused;
            }
        }

        return new KeepSyncSignal<T>(source, predicate!);
    }

    /// <inheritdoc/>
    IObservableAsync<TDest>? IMapFusableAsync<T>.TryFuseMap<TDest>(Func<T, TDest> selector) =>
        source is not null && firstPredicate is not null
            ? new KeepMapSyncSignal<T, TDest>(source, firstPredicate, selector)
            : null;

    /// <inheritdoc/>
    IObservableAsync<T>? IKeepFusableAsync<T>.TryFuseKeep(Func<T, bool> predicate) =>
        source is not null && firstPredicate is not null
            ? new KeepKeepSyncSignal<T>(source, firstPredicate, predicate)
            : null;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new KeepSyncWitness<T>(observer, firstPredicate, cancellationToken),
            observer,
            cancellationToken);
}
