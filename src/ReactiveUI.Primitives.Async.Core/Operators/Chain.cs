// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and concatenating asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable fluent composition of asynchronous observables, allowing multiple
/// sequences to be combined into a single sequence that emits items in order. These methods are intended for use with
/// types that implement asynchronous, push-based data streams.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Chain/Concat operators for a collection of observable sequences.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequences.</typeparam>
    /// <param name="sources">A collection of asynchronous observable sequences to concatenate. Cannot be null.</param>
    extension<T>(IEnumerable<IObservableAsync<T>> sources)
    {
        /// <summary>
        /// Concatenates multiple asynchronous observable sequences into a single sequence that emits items from each source
        /// in order.
        /// </summary>
        /// <returns>An asynchronous observable sequence that emits all items from each source sequence in the order they appear in
        /// the collection.</returns>
        /// <remarks>Each source sequence is subscribed to only after the previous one completes. If any source
        /// sequence signals an error, concatenation stops and the error is propagated to the observer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Chain() =>
            new ChainEnumerableSignal<T>(sources);

        /// <summary>
        /// Concatenates multiple asynchronous observable sequences into a single sequence that emits items from each source
        /// in order.
        /// </summary>
        /// <returns>An asynchronous observable sequence that emits all items from each source sequence in order.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Concat() =>
            new ChainEnumerableSignal<T>(sources);
    }

    /// <summary>Chain/Concat operators for an observable sequence of inner observable sequences.</summary>
    /// <typeparam name="T">The type of the elements emitted by the inner observable sequences.</typeparam>
    /// <param name="source">The source observable sequence whose elements are themselves observable sequences to be concatenated. Cannot be
    /// null.</param>
    extension<T>(IObservableAsync<IObservableAsync<T>> source)
    {
        /// <summary>
        /// Concatenates a sequence of asynchronous observable sequences into a single observable sequence, subscribing to
        /// each inner sequence in order only after the previous one completes.
        /// </summary>
        /// <returns>An observable sequence that emits the elements of each inner observable sequence in order, waiting for each to
        /// complete before subscribing to the next.</returns>
        /// <remarks>If any inner observable sequence signals an error, the resulting sequence will propagate that
        /// error and terminate immediately. The concatenation is performed in a deferred and sequential manner, ensuring
        /// that only one inner sequence is active at a time.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Chain() =>
            new ChainSignalSourcesSignal<T>(source);

        /// <summary>
        /// Concatenates a sequence of asynchronous observable sequences into a single observable sequence, subscribing to
        /// each inner sequence in order only after the previous one completes.
        /// </summary>
        /// <returns>An observable sequence that emits the elements of each inner observable sequence in order.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Concat() =>
            new ChainSignalSourcesSignal<T>(source);
    }

    /// <summary>Chain/Concat operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequences.</typeparam>
    /// <param name="source">The first observable sequence to concatenate. Cannot be null.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Concatenates two asynchronous observable sequences into a single sequence that emits all elements from the first
        /// sequence, followed by all elements from the second sequence.
        /// </summary>
        /// <param name="second">The second observable sequence to concatenate. Cannot be null.</param>
        /// <returns>An observable sequence that emits all elements from the first sequence, followed by all elements from the second
        /// sequence.</returns>
        /// <remarks>The resulting sequence emits all items from the first observable before subscribing to and
        /// emitting items from the second observable. If either sequence signals an error, the concatenation terminates and
        /// the error is propagated to observers.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Chain(IObservableAsync<T> second) =>
            new ChainEnumerableSignal<T>([source, second]);

        /// <summary>
        /// Concatenates two asynchronous observable sequences into a single sequence that emits all elements from the first
        /// sequence, followed by all elements from the second sequence.
        /// </summary>
        /// <param name="second">The second observable sequence to concatenate.</param>
        /// <returns>An observable sequence that emits all elements from the first sequence, followed by all elements from the second.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Concat(IObservableAsync<T> second) =>
            new ChainEnumerableSignal<T>([source, second]);
    }
}
