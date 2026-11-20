// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Single-or-default operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously returns the only element of the sequence that satisfies a condition, or <paramref name="defaultValue"/> when no element does.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the condition specified by <paramref
        /// name="predicate"/>.</param>
        /// <returns>A value task whose result is the single matching element, or <paramref name="defaultValue"/> when no
        /// element matches.</returns>
        /// <exception cref="InvalidOperationException">More than one element satisfies <paramref name="predicate"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue) =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source, predicate, defaultValue);

        /// <summary>Asynchronously returns the only element of the sequence that satisfies a condition, or <paramref name="defaultValue"/> when no element does.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method returns the element for which this predicate
        /// returns <see langword="true"/>.</param>
        /// <param name="defaultValue">The value to return if no element in the sequence satisfies the condition specified by <paramref
        /// name="predicate"/>.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A value task whose result is the single matching element, or <paramref name="defaultValue"/> when no
        /// element matches.</returns>
        /// <exception cref="InvalidOperationException">More than one element satisfies <paramref name="predicate"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync(
            Func<T, bool> predicate,
            T? defaultValue,
            CancellationToken cancellationToken) =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source, predicate, defaultValue, cancellationToken);

        /// <summary>Asynchronously returns the only element of the sequence, or the default of <typeparamref name="T"/> when the sequence is empty.</summary>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence, or the default value of <typeparamref name="T"/> if the sequence is empty.</returns>
        /// <exception cref="InvalidOperationException">The source sequence contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync() =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source);

        /// <summary>Asynchronously returns the only element of the sequence, or the default of <typeparamref name="T"/> when the sequence is empty.</summary>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A value task that represents the asynchronous operation. The task result contains the single element of the
        /// sequence, or the default value of <typeparamref name="T"/> if the sequence is empty.</returns>
        /// <exception cref="InvalidOperationException">The source sequence contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync(CancellationToken cancellationToken) =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source, cancellationToken);

        /// <summary>Asynchronously returns the single element of the sequence, or <paramref name="defaultValue"/> when the sequence is empty.</summary>
        /// <param name="defaultValue">The value to return if the sequence contains no elements.</param>
        /// <returns>A value task whose result is the single element of the sequence, or <paramref name="defaultValue"/> when
        /// the sequence is empty.</returns>
        /// <exception cref="InvalidOperationException">The source sequence contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync(T? defaultValue) =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source, defaultValue);

        /// <summary>Asynchronously returns the single element of the sequence, or <paramref name="defaultValue"/> when the sequence is empty.</summary>
        /// <param name="defaultValue">The value to return if the sequence contains no elements.</param>
        /// <param name="cancellationToken">The token that cancels the operation.</param>
        /// <returns>A value task whose result is the single element of the sequence, or <paramref name="defaultValue"/> when
        /// the sequence is empty.</returns>
        /// <exception cref="InvalidOperationException">The source sequence contains more than one element.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T?> SingleOrDefaultAsync(T? defaultValue, CancellationToken cancellationToken) =>
            SingleElementWitness<T>.SingleOrDefaultAsync(source, defaultValue, cancellationToken);
    }
}
