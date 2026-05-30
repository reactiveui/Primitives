// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for composing and concatenating asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable fluent composition of asynchronous observables, allowing multiple
/// sequences to be combined into a single sequence that emits items in order. These methods are intended for use with
/// types that implement asynchronous, push-based data streams.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Concatenates a sequence of asynchronous observable sequences into a single observable sequence, subscribing to
    /// each inner sequence in order only after the previous one completes.
    /// </summary>
    /// <remarks>If any inner observable sequence signals an error, the resulting sequence will propagate that
    /// error and terminate immediately. The concatenation is performed in a deferred and sequential manner, ensuring
    /// that only one inner sequence is active at a time.</remarks>
    /// <typeparam name="T">The type of the elements emitted by the inner observable sequences.</typeparam>
    /// <param name="this">The source observable sequence whose elements are themselves observable sequences to be concatenated. Cannot be
    /// null.</param>
    /// <returns>An observable sequence that emits the elements of each inner observable sequence in order, waiting for each to
    /// complete before subscribing to the next.</returns>
    public static IObservableAsync<T> Concat<T>(this IObservableAsync<IObservableAsync<T>> @this) =>
        new ConcatSignalSourcesSignal<T>(@this);

    /// <summary>
    /// Concatenates multiple asynchronous observable sequences into a single sequence that emits items from each source
    /// in order.
    /// </summary>
    /// <remarks>Each source sequence is subscribed to only after the previous one completes. If any source
    /// sequence signals an error, concatenation stops and the error is propagated to the observer.</remarks>
    /// <typeparam name="T">The type of the elements in the observable sequences.</typeparam>
    /// <param name="this">A collection of asynchronous observable sequences to concatenate. Cannot be null.</param>
    /// <returns>An asynchronous observable sequence that emits all items from each source sequence in the order they appear in
    /// the collection.</returns>
    public static IObservableAsync<T> Concat<T>(this IEnumerable<IObservableAsync<T>> @this) =>
        new ConcatEnumerableSignal<T>(@this);

    /// <summary>
    /// Concatenates two asynchronous observable sequences into a single sequence that emits all elements from the first
    /// sequence, followed by all elements from the second sequence.
    /// </summary>
    /// <remarks>The resulting sequence emits all items from the first observable before subscribing to and
    /// emitting items from the second observable. If either sequence signals an error, the concatenation terminates and
    /// the error is propagated to observers.</remarks>
    /// <typeparam name="T">The type of the elements in the observable sequences.</typeparam>
    /// <param name="this">The first observable sequence to concatenate. Cannot be null.</param>
    /// <param name="second">The second observable sequence to concatenate. Cannot be null.</param>
    /// <returns>An observable sequence that emits all elements from the first sequence, followed by all elements from the second
    /// sequence.</returns>
    public static IObservableAsync<T> Concat<T>(this IObservableAsync<T> @this, IObservableAsync<T> second) =>
        new ConcatEnumerableSignal<T>([@this, second]);
}
