// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences, enabling operations such as suppressing consecutive duplicate elements.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Consecutive-distinctness operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an observable sequence that emits only distinct consecutive elements, suppressing duplicates that are equal to the previous element.</summary>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not equal to their
        /// immediate predecessor.</returns>
        /// <remarks>Uses the default equality comparer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Unique() =>
            new UniqueSignal<T>(source);

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence only when the current element is
        /// not equal to the previous element, as determined by the specified equality comparer.
        /// </summary>
        /// <param name="equalityComparer">An equality comparer used to determine whether consecutive elements are considered equal.</param>
        /// <returns>An observable sequence that contains only distinct consecutive elements from the source sequence, as
        /// determined by the specified equality comparer.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="equalityComparer"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Unique(IEqualityComparer<T> equalityComparer) =>
            new UniqueSignal<T>(source, equalityComparer);

        /// <summary>Returns an observable sequence that emits elements from the source sequence, suppressing consecutive duplicates as determined by a key selector function.</summary>
        /// <typeparam name="TKey">The type of the key used to determine whether consecutive elements are considered duplicates.</typeparam>
        /// <param name="keySelector">A function that extracts the comparison key from each element in the source sequence.</param>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not consecutive
        /// duplicates according to the specified key.</returns>
        /// <remarks>Compares keys with the default equality comparer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> UniqueBy<TKey>(Func<T, TKey> keySelector) =>
            new UniqueBySignal<T, TKey>(source, keySelector);

        /// <summary>Returns an observable sequence that emits elements from the source sequence, suppressing consecutive duplicates as determined by a key selector and equality comparer.</summary>
        /// <typeparam name="TKey">The type of the key used to determine whether consecutive elements are considered duplicates.</typeparam>
        /// <param name="keySelector">A function that extracts the comparison key from each element in the source sequence.</param>
        /// <param name="equalityComparer">An equality comparer used to compare keys for equality.</param>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not consecutive
        /// duplicates according to the specified key and comparer.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> or <paramref name="equalityComparer"/> is null.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> UniqueBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> equalityComparer) =>
            new UniqueBySignal<T, TKey>(source, keySelector, equalityComparer);
    }
}
