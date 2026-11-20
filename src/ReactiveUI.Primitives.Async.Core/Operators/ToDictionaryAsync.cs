// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for asynchronously converting an observable sequence to a dictionary.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous dictionary-materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector function.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
        /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
        /// used.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// elements from the sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull =>
            ToDictionaryTaskWitness<T, TKey, T>.RunIdentityAsync(source, keySelector, comparer, cancellationToken);

        /// <summary>Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector function and the default equality comparer for the key type.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// elements from the sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector)
            where TKey : notnull =>
            ToDictionaryTaskWitness<T, TKey, T>.RunIdentityAsync(source, keySelector);

        /// <summary>Asynchronously creates a dictionary from the elements of the sequence using the specified key and element selector functions.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
        /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
        /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
        /// used.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// values as defined by the selector functions.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
            Func<T, TKey> keySelector,
            Func<T, TValue> elementSelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull =>
            ToDictionaryTaskWitness<T, TKey, TValue>.RunAsync(
                source,
                keySelector,
                elementSelector,
                comparer,
                cancellationToken);

        /// <summary>
        /// Asynchronously creates a dictionary from the elements of the sequence using the specified key and element
        /// selector functions, with the default equality comparer for the key type.
        /// </summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
        /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// values as defined by the selector functions.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
            Func<T, TKey> keySelector,
            Func<T, TValue> elementSelector)
            where TKey : notnull =>
            ToDictionaryTaskWitness<T, TKey, TValue>.RunAsync(source, keySelector, elementSelector);
    }
}
