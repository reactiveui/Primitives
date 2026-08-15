// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible select-many names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>SelectMany operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Projects each element of the observable sequence to an asynchronous observable sequence and
        /// merges the resulting sequences into one observable sequence.
        /// </summary>
        /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
        /// <param name="selector">A transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the merged projection results.</returns>
        public IObservableAsync<TResult> SelectMany<TResult>(Func<T, IObservableAsync<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new FlatMapSignal<T, TResult>(source, selector);
        }

        /// <summary>
        /// Projects each element of the observable sequence to an asynchronous observable sequence using
        /// an asynchronous selector and merges the resulting sequences into one observable sequence.
        /// </summary>
        /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
        /// <param name="selector">An asynchronous transform function to apply to each element.</param>
        /// <returns>An observable sequence whose elements are the merged projection results.</returns>
        public IObservableAsync<TResult> SelectMany<TResult>(
            Func<T, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new FlatMapSignal<T, TResult>(source, selector);
        }

        /// <summary>
        /// Projects each element of the observable sequence to an asynchronous observable sequence,
        /// merges the resulting sequences, and applies a result selector to each pair of source and
        /// inner element.
        /// </summary>
        /// <typeparam name="TCollection">The type of the elements in the intermediate inner sequences.</typeparam>
        /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
        /// <param name="collectionSelector">A transform function to apply to each element to produce an intermediate
        /// observable sequence.</param>
        /// <param name="resultSelector">A transform function to apply to each pair of source element and
        /// collection element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
        /// function on each element of the source sequence, and then mapping each pair of source and collection
        /// element through the result selector.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="collectionSelector"/> or
        /// <paramref name="resultSelector"/> is null.</exception>
        public IObservableAsync<TResult> SelectMany<TCollection, TResult>(
            Func<T, IObservableAsync<TCollection>> collectionSelector,
            Func<T, TCollection, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(collectionSelector);
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new FlatMapSignal<T, TResult>(
                source,
                source => new MapSyncSignal<TCollection, TResult>(
                    collectionSelector(source),
                    collection => resultSelector(source, collection)));
        }
    }
}
