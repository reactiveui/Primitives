// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Fuses two consecutive synchronous projections into one witness layer without closure allocation.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TMiddle">The intermediate projected type.</typeparam>
/// <typeparam name="TDest">The final projected type.</typeparam>
/// <param name="source">The upstream observable sequence.</param>
/// <param name="first">The first synchronous selector.</param>
/// <param name="second">The second synchronous selector.</param>
[System.Diagnostics.DebuggerDisplay("MapMapSyncSignal<{typeof(TSource).Name,nq}>")]
public sealed class MapMapSyncSignal<TSource, TMiddle, TDest>(
    IObservableAsync<TSource> source,
    Func<TSource, TMiddle> first,
    Func<TMiddle, TDest> second) : IObservableAsync<TDest>
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
        IObserverAsync<TDest> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new MapMapSyncWitness<TSource, TMiddle, TDest>(observer, first, second, cancellationToken),
            observer,
            cancellationToken);
}
