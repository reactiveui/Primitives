// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of static methods for creating and composing asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Distinctness operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns a sequence that contains only distinct elements from the source sequence, using the default equality comparer for the element type.</summary>
        /// <returns>An observable sequence that contains distinct elements from the source sequence.</returns>
        /// <remarks>Only the first occurrence of each element reaches observers, in source order; the set of seen
        /// elements lives for the whole subscription.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Distinct() =>
            new DistinctSignal<T>(source);

        /// <summary>Returns an observable sequence that contains only distinct elements from the source sequence, using the specified equality comparer to determine uniqueness.</summary>
        /// <param name="equalityComparer">An equality comparer to compare values for equality.</param>
        /// <returns>An observable sequence that emits each distinct element from the source sequence, in the order in which they
        /// are received.</returns>
        /// <remarks>Only the first occurrence of each element, as judged by <paramref name="equalityComparer"/>,
        /// reaches observers.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Distinct(IEqualityComparer<T> equalityComparer) =>
            new DistinctSignal<T>(source, equalityComparer);

        /// <summary>Returns a sequence that contains distinct elements from the source sequence according to a specified key selector function.</summary>
        /// <typeparam name="TKey">The type of the key returned by the key selector function.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the key
        /// selector.</returns>
        /// <remarks>Keys are compared with the default equality comparer for <typeparamref name="TKey"/>.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DistinctBy<TKey>(Func<T, TKey> keySelector) =>
            new DistinctBySignal<T, TKey>(source, keySelector);

        /// <summary>Returns an observable sequence that contains only distinct elements from the source sequence, comparing values based on a specified key and equality comparer.</summary>
        /// <typeparam name="TKey">The type of the key used to determine the distinctness of elements.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <param name="equalityComparer">An equality comparer to compare keys for equality. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the
        /// specified key selector and equality comparer.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="equalityComparer"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> DistinctBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> equalityComparer) =>
            new DistinctBySignal<T, TKey>(source, keySelector, equalityComparer);
    }
}
