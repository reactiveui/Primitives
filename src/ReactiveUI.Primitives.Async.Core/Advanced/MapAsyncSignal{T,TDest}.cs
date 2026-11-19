// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Applies an asynchronous selector to each source value, allocating one observer per subscription.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <typeparam name="TDest">The projected element type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The asynchronous selector.</param>
[System.Diagnostics.DebuggerDisplay("MapAsyncSignal<{typeof(T).Name,nq}>")]
public sealed class MapAsyncSignal<T, TDest>(
    IObservableAsync<T> source,
    Func<T, CancellationToken, ValueTask<TDest>> selector) : IObservableAsync<TDest>
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
        IObserverAsync<TDest> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new MapAsyncWitness<T, TDest>(observer, selector, cancellationToken),
            observer,
            cancellationToken);
}
