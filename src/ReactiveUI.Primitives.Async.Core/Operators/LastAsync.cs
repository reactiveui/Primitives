// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Last-element operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the last element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> LastAsync(Func<T, bool> predicate) =>
            LastTaskWitness<T>.RunAsync(source, predicate);

        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the last element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> LastAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            LastTaskWitness<T>.RunAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously returns the last element of the sequence.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> LastAsync() =>
            LastTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously returns the last element of the sequence.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> LastAsync(CancellationToken cancellationToken) =>
            LastTaskWitness<T>.RunAsync(source, cancellationToken);
    }
}
