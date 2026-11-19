// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of static methods for composing and merging asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Blend/Merge operators for an enumerable collection of observable source sequences.</summary>
    /// <typeparam name="T">The type of the elements produced by the observable sequences.</typeparam>
    /// <param name="sources">A collection of asynchronous observable sequences to be merged.</param>
    extension<T>(IEnumerable<IObservableAsync<T>> sources)
    {
        /// <summary>Combines multiple asynchronous observable sequences into a single observable sequence that emits items from all source sequences as they arrive.</summary>
        /// <returns>An observable sequence that emits items from all input sequences as they are produced.</returns>
        /// <remarks>Emissions interleave as the sources produce them; the result completes once every source has
        /// completed, and an error from any source propagates and terminates it.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Blend() =>
            new BlendEnumerableSignal<T>(sources);

        /// <summary>Combines multiple asynchronous observable sequences into a single observable sequence that emits items from all source sequences as they arrive.</summary>
        /// <returns>An observable sequence that emits items from all input sequences as they are produced.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Merge() =>
            new BlendEnumerableSignal<T>(sources);
    }

    /// <summary>Blend/Merge operators for an observable source sequence of inner observable sequences.</summary>
    /// <typeparam name="T">The type of the elements emitted by the inner observable sequences.</typeparam>
    /// <param name="source">The source asynchronous observable sequence whose elements are themselves observable sequences to be merged.
    /// Cannot be null.</param>
    extension<T>(IObservableAsync<IObservableAsync<T>> source)
    {
        /// <summary>Merges multiple asynchronous observable sequences into a single observable sequence that emits items from all inner sequences as they arrive.</summary>
        /// <returns>An asynchronous observable sequence that emits items from all inner observable sequences as they are produced.</returns>
        /// <remarks>All inner sources remain subscribed; completion waits for the outer source and every inner source,
        /// and any error terminates immediately.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Blend() =>
            new BlendSignalSourcesSignal<T>(source);

        /// <summary>Merges multiple asynchronous observable sequences into a single observable sequence that emits items from all inner sequences as they arrive.</summary>
        /// <returns>An asynchronous observable sequence that emits items from all inner observable sequences as they are produced.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Merge() =>
            new BlendSignalSourcesSignal<T>(source);

        /// <summary>Merges the emissions of multiple asynchronous observable sequences into a single observable sequence, limiting the number of concurrent subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of inner observable sequences to subscribe to concurrently.</param>
        /// <returns>An observable sequence that emits the items from the merged inner observable sequences.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Merge(int maxConcurrent) =>
            new BlendSignalSourcesSignalWithMaxConcurrency<T>(source, maxConcurrent);
    }

    /// <summary>Blend/Merge operators that combine an observable source sequence with another sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequences.</typeparam>
    /// <param name="source">The first asynchronous observable sequence to merge.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Combines the elements of two asynchronous observable sequences into a single sequence by merging their emissions.</summary>
        /// <param name="other">The second asynchronous observable sequence to merge with the first.</param>
        /// <returns>An SignalAsync{T} that emits the elements from both input sequences as they arrive.</returns>
        /// <remarks>The result completes once both sequences have completed; an error from either propagates and
        /// terminates it.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Blend(IObservableAsync<T> other) =>
            new BlendEnumerableSignal<T>([source, other]);

        /// <summary>Combines the elements of two asynchronous observable sequences into a single sequence by merging their emissions.</summary>
        /// <param name="other">The second asynchronous observable sequence to merge with the first.</param>
        /// <returns>An observable that emits the elements from both input sequences as they arrive.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Merge(IObservableAsync<T> other) =>
            new BlendEnumerableSignal<T>([source, other]);
    }
}
