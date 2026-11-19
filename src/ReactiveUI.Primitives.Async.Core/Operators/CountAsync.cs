// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for performing asynchronous operations on observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous element-counting operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously counts the number of elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <returns>A task that represents the asynchronous count operation. The task result contains the number of elements
        /// that match the predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> CountAsync(Func<T, bool>? predicate) =>
            CountTaskWitness<T>.RunAsync(source, predicate);

        /// <summary>Asynchronously counts the number of elements that satisfy a specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous count operation. The task result contains the number of elements
        /// that match the predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> CountAsync(Func<T, bool>? predicate, CancellationToken cancellationToken) =>
            CountTaskWitness<T>.RunAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously returns the total number of elements in the data source.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements in the
        /// data source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> CountAsync() =>
            CountTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously returns the total number of elements in the data source.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous count operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements in the
        /// data source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<int> CountAsync(CancellationToken cancellationToken) =>
            CountTaskWitness<T>.RunAsync(source, cancellationToken);
    }
}
