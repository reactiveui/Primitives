// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides SelectMany (flat map) extension methods for asynchronous observable sequences.
/// </summary>
/// <remarks>SelectMany projects each element of an observable sequence to an observable sequence and
/// merges the resulting observable sequences into one observable sequence. This is the monadic bind
/// operation for observables and is essential for composing chains of asynchronous operations.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Projects each element of the observable sequence to an asynchronous observable sequence and
    /// merges the resulting sequences into one observable sequence.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="selector">A transform function to apply to each element; it returns an observable sequence
    /// for each element.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
    /// function on each element of the source sequence and merging the results.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="selector"/> is null.</exception>
    public static IObservableAsync<TResult> SelectMany<T, TResult>(this IObservableAsync<T> @this, Func<T, IObservableAsync<TResult>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return @this.Select((x, _) => new ValueTask<IObservableAsync<TResult>>(selector(x))).Merge();
    }

    /// <summary>
    /// Projects each element of the observable sequence to an asynchronous observable sequence using
    /// an asynchronous selector and merges the resulting sequences into one observable sequence.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the projected inner sequences.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="selector">An asynchronous transform function to apply to each element; it returns an observable
    /// sequence for each element.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
    /// function on each element of the source sequence and merging the results.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="selector"/> is null.</exception>
    public static IObservableAsync<TResult> SelectMany<T, TResult>(
        this IObservableAsync<T> @this,
        Func<T, CancellationToken, ValueTask<IObservableAsync<TResult>>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return @this.Select(selector).Merge();
    }

    /// <summary>
    /// Projects each element of the observable sequence to an asynchronous observable sequence,
    /// merges the resulting sequences, and applies a result selector to each pair of source and
    /// inner element.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <typeparam name="TCollection">The type of the elements in the intermediate inner sequences.</typeparam>
    /// <typeparam name="TResult">The type of the elements in the result sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="collectionSelector">A transform function to apply to each element to produce an intermediate
    /// observable sequence.</param>
    /// <param name="resultSelector">A transform function to apply to each pair of source element and
    /// collection element.</param>
    /// <returns>An observable sequence whose elements are the result of invoking the one-to-many transform
    /// function on each element of the source sequence, and then mapping each pair of source and collection
    /// element through the result selector.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="collectionSelector"/> or
    /// <paramref name="resultSelector"/> is null.</exception>
    public static IObservableAsync<TResult> SelectMany<T, TCollection, TResult>(
        this IObservableAsync<T> @this,
        Func<T, IObservableAsync<TCollection>> collectionSelector,
        Func<T, TCollection, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(collectionSelector);
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        return @this.SelectMany(source =>
            collectionSelector(source).Select(collection => resultSelector(source, collection)));
    }
}
