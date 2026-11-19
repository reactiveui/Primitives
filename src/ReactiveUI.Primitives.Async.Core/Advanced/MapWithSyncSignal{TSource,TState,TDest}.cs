// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Applies a stateful synchronous selector without closure allocation.</summary>
/// <typeparam name="TSource">The upstream element type.</typeparam>
/// <typeparam name="TState">The caller-supplied state type.</typeparam>
/// <typeparam name="TDest">The projected element type.</typeparam>
/// <param name="source">The upstream observable sequence.</param>
/// <param name="state">The caller-supplied state.</param>
/// <param name="selector">The stateful projection.</param>
[System.Diagnostics.DebuggerDisplay("MapWithSyncSignal<{typeof(TSource).Name,nq}>")]
public sealed class MapWithSyncSignal<TSource, TState, TDest>(
    IObservableAsync<TSource> source,
    TState state,
    Func<TState, TSource, TDest> selector) : IObservableAsync<TDest>
{
    /// <summary>Creates a stateful projection sequence over <paramref name="source"/>, short-circuiting empty and never singletons.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="state">The caller-supplied state.</param>
    /// <param name="selector">The stateful projection.</param>
    /// <returns>An observable sequence of projected values.</returns>
    public static IObservableAsync<TDest> Create(
        IObservableAsync<TSource> source,
        TState state,
        Func<TState, TSource, TDest> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return source switch
        {
            SignalAsync.EmptySignalAsync<TSource> => SignalAsync.EmptySignalAsync<TDest>.Instance,
            SignalAsync.NeverSignalAsync<TSource> => SignalAsync.NeverSignalAsync<TDest>.Instance,
            _ => new MapWithSyncSignal<TSource, TState, TDest>(source, state, selector),
        };
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
        IObserverAsync<TDest> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new MapWithSyncWitness<TSource, TState, TDest>(observer, state, selector, cancellationToken),
            observer,
            cancellationToken);
}
