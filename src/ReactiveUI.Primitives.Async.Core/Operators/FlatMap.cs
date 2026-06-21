// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides SelectMany (flat map) extension methods for asynchronous observable sequences.</summary>
/// <remarks>SelectMany projects each element of an observable sequence to an observable sequence and
/// merges the resulting observable sequences into one observable sequence. This is the monadic bind
/// operation for observables and is essential for composing chains of asynchronous operations.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>FlatMap/SelectMany operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Projects each element of the observable sequence to an asynchronous observable sequence and
        /// merges the resulting sequences into one observable sequence.
        /// </summary>
        /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
        /// <param name="selector">A transform function to apply to each element; it returns an observable sequence
        /// for each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
        /// function on each element of the source sequence and merging the results.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="selector"/> is null.</exception>
        public IObservableAsync<TResult> FlatMap<TResult>(Func<T, IObservableAsync<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new FlatMapSignal<T, TResult>(@this, selector);
        }

        /// <summary>
        /// Projects each element of the observable sequence to an asynchronous observable sequence using
        /// an asynchronous selector and merges the resulting sequences into one observable sequence.
        /// </summary>
        /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
        /// <param name="selector">An asynchronous transform function to apply to each element; it returns an observable
        /// sequence for each element.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
        /// function on each element of the source sequence and merging the results.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="selector"/> is null.</exception>
        public IObservableAsync<TResult> FlatMap<TResult>(
            Func<T, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new FlatMapSignal<T, TResult>(@this, selector);
        }

    }
}
