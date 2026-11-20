// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Last-or-default operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue) =>
            LastOrDefaultTaskWitness<T>.RunAsync(source, predicate, defaultValue);

        /// <summary>Asynchronously returns the last element in the sequence that satisfies the specified predicate, or a default value if no such element is found.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the last element for which this
        /// predicate returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the predicate.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The result contains the last element that matches
        /// the predicate, or <paramref name="defaultValue"/> if no such element is found.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue,
            CancellationToken cancellationToken) =>
            LastOrDefaultTaskWitness<T>.RunAsync(source, predicate, defaultValue, cancellationToken);

        /// <summary>Asynchronously returns the last element of a sequence, or a default value if the sequence contains no elements.</summary>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync() =>
            LastOrDefaultTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously returns the last element of a sequence, or a default value if the sequence contains no elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or the default value for type T if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(CancellationToken cancellationToken) =>
            LastOrDefaultTaskWitness<T>.RunAsync(source, cancellationToken);

        /// <summary>Asynchronously returns the last element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(T? defaultValue) =>
            LastOrDefaultTaskWitness<T>.RunAsync(source, defaultValue);

        /// <summary>Asynchronously returns the last element of the sequence, or a specified default value if the sequence contains no elements.</summary>
        /// <param name="defaultValue">The value to return if the sequence is empty.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the last element of the
        /// sequence, or <paramref name="defaultValue"/> if the sequence is empty.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> LastOrDefaultAsync(T? defaultValue, CancellationToken cancellationToken) =>
            LastOrDefaultTaskWitness<T>.RunAsync(source, defaultValue, cancellationToken);
    }
}
