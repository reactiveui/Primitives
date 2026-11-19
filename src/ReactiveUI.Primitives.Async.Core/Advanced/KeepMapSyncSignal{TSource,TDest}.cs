// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Fuses a synchronous filter followed by a synchronous projection into one witness layer.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TDest">The projected element type.</typeparam>
/// <param name="source">The upstream observable sequence.</param>
/// <param name="predicate">The synchronous filter predicate.</param>
/// <param name="selector">The synchronous projection.</param>
[System.Diagnostics.DebuggerDisplay("KeepMapSyncSignal<{typeof(TSource).Name,nq}>")]
public sealed class KeepMapSyncSignal<TSource, TDest>(
    IObservableAsync<TSource> source,
    Func<TSource, bool> predicate,
    Func<TSource, TDest> selector) : IObservableAsync<TDest>
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
        IObserverAsync<TDest> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new KeepMapSyncWitness<TSource, TDest>(observer, predicate, selector, cancellationToken),
            observer,
            cancellationToken);
}
