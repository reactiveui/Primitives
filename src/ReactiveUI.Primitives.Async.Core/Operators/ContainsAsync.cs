// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous containment operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, IEqualityComparer<T>? comparer) =>
            ContainsTaskWitness<T>.RunAsync(source, value, comparer);

        /// <summary>Asynchronously determines whether the sequence contains a specified value using the given equality comparer.</summary>
        /// <param name="value">The value to locate in the sequence.</param>
        /// <param name="comparer">The equality comparer to use for comparing values, or null to use the default equality comparer for the
        /// type.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the sequence; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(
            T value,
            IEqualityComparer<T>? comparer,
            CancellationToken cancellationToken) =>
            ContainsTaskWitness<T>.RunAsync(source, value, comparer, cancellationToken);

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value) =>
            ContainsTaskWitness<T>.RunAsync(source, value);

        /// <summary>Asynchronously determines whether the collection contains a specified value.</summary>
        /// <param name="value">The value to locate in the collection.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// value is found in the collection; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> ContainsAsync(T value, CancellationToken cancellationToken) =>
            ContainsTaskWitness<T>.RunAsync(source, value, cancellationToken);
    }
}
