// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>First-element operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the first element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync(Func<T, bool> predicate) =>
            FirstTaskWitness<T>.FirstAsync(source, predicate);

        /// <summary>Asynchronously returns the first element in the sequence that satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the first element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without a matching element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            FirstTaskWitness<T>.FirstAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously returns the first element of the sequence.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync() =>
            FirstTaskWitness<T>.FirstAsync(source);

        /// <summary>Asynchronously returns the first element of the sequence.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the first element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The sequence completes without producing an element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> FirstAsync(CancellationToken cancellationToken) =>
            FirstTaskWitness<T>.FirstAsync(source, cancellationToken);
    }
}
