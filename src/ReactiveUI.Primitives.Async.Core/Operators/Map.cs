// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and transforming asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Projection (Map/Select) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Projects each element of the observable sequence into a new form using the specified asynchronous selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function and produced by the resulting observable sequence.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a value of type <typeparamref
        /// name="TDest"/> asynchronously. The function receives the source element and a cancellation token.</param>
        /// <returns>An observable sequence of type <typeparamref name="TDest"/> containing the results of applying the selector
        /// function to each element of the source sequence.</returns>
        /// <remarks>The selector runs for each element as it is observed; a thrown exception or a faulted task is
        /// propagated to the observer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Map<TDest>(
            Func<T, CancellationToken, ValueTask<TDest>> selector) =>
            new MapAsyncSignal<T, TDest>(source, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a new value. Cannot be null.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the selector function on each element of
        /// the source sequence.</returns>
        /// <remarks>The selector runs for each element as it is observed; a thrown exception is propagated to the
        /// observer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Map<TDest>(Func<T, TDest> selector) =>
            MapSyncSignal<T, TDest>.Create(source, selector);

        /// <summary>Projects each value using caller-supplied state.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <typeparam name="TDest">The result element type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the selector.</param>
        /// <param name="selector">The projection applied to each value and the state.</param>
        /// <returns>An observable sequence of projected values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> MapWith<TState, TDest>(
            TState state,
            Func<TState, T, TDest> selector) =>
            MapWithSyncSignal<T, TState, TDest>.Create(source, state, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified asynchronous selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function and produced by the resulting observable sequence.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a value.</param>
        /// <returns>An observable sequence containing the results of applying the selector function to each source element.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Select<TDest>(
            Func<T, CancellationToken, ValueTask<TDest>> selector) =>
            new MapAsyncSignal<T, TDest>(source, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a new value.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the selector function.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Select<TDest>(Func<T, TDest> selector) =>
            MapSyncSignal<T, TDest>.Create(source, selector);
    }
}
