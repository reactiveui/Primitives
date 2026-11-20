// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous 64-bit element-counting operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
        /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync(Func<T, bool>? predicate) =>
            LongCountTaskWitness<T>.RunAsync(source, predicate);

        /// <summary>Asynchronously returns the number of elements in the sequence that satisfy an optional predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, all elements are counted.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the number of elements that
        /// satisfy the predicate, or the total number of elements if the predicate is null.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync(
            Func<T, bool>? predicate,
            CancellationToken cancellationToken) =>
            LongCountTaskWitness<T>.RunAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously returns the total number of elements in the sequence as a 64-bit integer.</summary>
        /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
        /// sequence as a 64-bit integer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync() =>
            LongCountTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously returns the total number of elements in the sequence as a 64-bit integer.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task representing the asynchronous operation. The result contains the number of elements in the
        /// sequence as a 64-bit integer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<long> LongCountAsync(CancellationToken cancellationToken) =>
            LongCountTaskWitness<T>.RunAsync(source, cancellationToken);
    }
}
