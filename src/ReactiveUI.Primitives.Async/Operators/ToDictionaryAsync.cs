// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for asynchronously converting an observable sequence to a dictionary.
/// </summary>
/// <remarks>The methods in this class enable the transformation of an asynchronous observable sequence into a
/// dictionary, using user-supplied key and element selector functions. These operations are performed asynchronously
/// and support cancellation via a CancellationToken. All methods throw an exception if duplicate keys are encountered
/// in the source sequence, consistent with the behavior of Dictionary{TKey, TValue}.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector
    /// function.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
    /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
    /// used.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
    /// elements from the sequence.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the keySelector parameter is null.</exception>
    public static async ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(
        this IObservableAsync<T> @this,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        cancellationToken.ThrowIfCancellationRequested();

        var observer = new ToDictionaryTaskWitness<T, TKey, T>(keySelector, x => x, comparer, cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector
    /// function and the default equality comparer for the key type.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
    /// elements from the sequence.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the keySelector parameter is null.</exception>
    public static ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<T, TKey>(this IObservableAsync<T> @this, Func<T, TKey> keySelector)
        where TKey : notnull =>
        @this.ToDictionaryAsync(keySelector, null, CancellationToken.None);

    /// <summary>
    /// Asynchronously creates a dictionary from the elements of the sequence using the specified key and element
    /// selector functions.
    /// </summary>
    /// <remarks>If multiple elements produce the same key, an exception may be thrown. The operation
    /// is performed asynchronously and can be cancelled using the provided cancellation token.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
    /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
    /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
    /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
    /// used.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
    /// values as defined by the selector functions.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> or <paramref name="elementSelector"/> is null.</exception>
    public static async ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(
        this IObservableAsync<T> @this,
        Func<T, TKey> keySelector,
        Func<T, TValue> elementSelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        ArgumentExceptionHelper.ThrowIfNull(elementSelector);
        cancellationToken.ThrowIfCancellationRequested();

        var observer =
            new ToDictionaryTaskWitness<T, TKey, TValue>(
                keySelector,
                elementSelector,
                comparer,
                cancellationToken);
        await using var subscription = await @this.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously creates a dictionary from the elements of the sequence using the specified key and element
    /// selector functions, with the default equality comparer for the key type.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
    /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
    /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
    /// values as defined by the selector functions.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> or <paramref name="elementSelector"/> is null.</exception>
    public static ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<T, TKey, TValue>(
        this IObservableAsync<T> @this,
        Func<T, TKey> keySelector,
        Func<T, TValue> elementSelector)
        where TKey : notnull =>
        @this.ToDictionaryAsync(keySelector, elementSelector, null, CancellationToken.None);

    /// <summary>
    /// Witness that builds a dictionary from the elements of a sequence using key and element selectors.
    /// </summary>
    /// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values.</typeparam>
    /// <param name="keySelector">A function to extract a key from each element.</param>
    /// <param name="elementSelector">A function to extract a value from each element.</param>
    /// <param name="comparer">An optional equality comparer for keys.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    internal sealed class ToDictionaryTaskWitness<TSource, TKey, TValue>(
        Func<TSource, TKey> keySelector,
        Func<TSource, TValue> elementSelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken)
        : TaskResultWitnessAsyncBase<TSource, Dictionary<TKey, TValue>>(cancellationToken)
        where TKey : notnull
    {
        /// <summary>
        /// The dictionary that accumulates key-value pairs from the source sequence.
        /// </summary>
        private readonly Dictionary<TKey, TValue> _map = comparer is null ? new() : new(comparer);

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
        {
            var key = keySelector(value);
            _map.Add(key, elementSelector(value));
            return default;
        }

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            => SetExceptionAndDisposeAsync(error);

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Result result)
            => !result.IsSuccess ? SetExceptionAndDisposeAsync(result.Exception) : SetResultAndDisposeAsync(_map);
    }
}
