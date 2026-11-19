// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for asynchronous observable sequences, enabling operations such as retrieving a single element that matches a specified condition.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Single-element operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the one element of the sequence that satisfies the specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">No element satisfies <paramref name="predicate"/>, or more than one does.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(Func<T, bool> predicate) =>
            SingleElementWitness<T>.SingleAsync(source, predicate);

        /// <summary>Asynchronously returns the one element of the sequence that satisfies the specified condition.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element that matches
        /// the predicate.</returns>
        /// <exception cref="InvalidOperationException">No element satisfies <paramref name="predicate"/>, or more than one does.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            SingleElementWitness<T>.SingleAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously returns the one element of a sequence that contains exactly one element.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The source sequence is empty or contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync() =>
            SingleElementWitness<T>.SingleAsync(source);

        /// <summary>Asynchronously returns the one element of a sequence that contains exactly one element.</summary>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence.</returns>
        /// <exception cref="InvalidOperationException">The source sequence is empty or contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T> SingleAsync(CancellationToken cancellationToken) =>
            SingleElementWitness<T>.SingleAsync(source, cancellationToken);
    }
}
