// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous quantifier operators that evaluate elements of an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously determines whether any element in the sequence satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. If null, the method checks whether the sequence contains
        /// any elements.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if any
        /// element satisfies the predicate or, if the predicate is null, if the sequence contains any elements;
        /// otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync(Func<T, bool>? predicate, CancellationToken cancellationToken) =>
            AnyTaskWitness<T>.RunAsync(source, predicate, cancellationToken);

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync() =>
            AnyTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously determines whether the source contains any elements.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if the
        /// source contains any elements; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync(CancellationToken cancellationToken) =>
            AnyTaskWitness<T>.RunAsync(source, cancellationToken);

        /// <summary>Asynchronously determines whether any element in the sequence satisfies the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if any
        /// element of the sequence passes the test in the specified predicate; otherwise, <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AnyAsync(Func<T, bool> predicate) =>
            AnyTaskWitness<T>.RunAsync(source, predicate);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AllAsync(Func<T, bool> predicate) =>
            AllTaskWitness<T>.RunAsync(source, predicate);

        /// <summary>Asynchronously determines whether all elements in the sequence satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The method evaluates this predicate for each element in the
        /// sequence.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if every
        /// element of the sequence passes the test in the specified predicate, or if the sequence is empty; otherwise,
        /// <see langword="false"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AllAsync(Func<T, bool> predicate, CancellationToken cancellationToken) =>
            AllTaskWitness<T>.RunAsync(source, predicate, cancellationToken);
    }
}
