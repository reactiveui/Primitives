// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and manipulating asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Grouping operators for an observable source sequence.</summary>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<TValue>(IObservableAsync<TValue> source)
    {
        /// <summary>Groups the elements of an asynchronous observable sequence according to a specified key selector function.</summary>
        /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
        /// <returns>An asynchronous observable sequence of grouped observables, each containing elements that share a common key.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <remarks>A group is emitted when its first element arrives, and each grouped observable can be subscribed
        /// independently.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<GroupedAsyncSignal<TKey, TValue>> GroupBy<TKey>(Func<TValue, TKey> keySelector)
            where TKey : notnull =>
            new GroupByAsyncSignal<TKey, TValue>(source, keySelector);

        /// <summary>Groups the elements of an asynchronous observable sequence according to a specified key selector function and returns an observable sequence of grouped observables.</summary>
        /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-null.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
        /// <param name="groupSignalSelector">A function that provides a signal for each group, given its key. Used to control how elements are published
        /// within each group.</param>
        /// <returns>An asynchronous observable sequence containing grouped observables, each representing a collection of elements
        /// that share a common key.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <remarks>The signal returned by <paramref name="groupSignalSelector"/> decides how a group's elements are
        /// buffered or multicast to its subscribers.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<GroupedAsyncSignal<TKey, TValue>> GroupBy<TKey>(
            Func<TValue, TKey> keySelector,
            Func<TKey, ISignalAsync<TValue>> groupSignalSelector)
            where TKey : notnull =>
            new GroupByAsyncSignal<TKey, TValue>(source, keySelector, groupSignalSelector);
    }
}
