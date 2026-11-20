// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and manipulating asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Filtering (Keep/Where) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Creates a new observable sequence that contains only the elements from the source sequence that satisfy the specified asynchronous predicate.</summary>
        /// <param name="predicate">A function that evaluates each element and its associated cancellation token, returning a ValueTask that
        /// resolves to <see langword="true"/> to include the element in the resulting sequence; otherwise, <see
        /// langword="false"/>.</param>
        /// <returns>An observable sequence that emits only those elements for which the predicate returns <see
        /// langword="true"/>.</returns>
        /// <remarks>An exception thrown by <paramref name="predicate"/>, or a faulted task from it, propagates to the
        /// observers of the resulting sequence.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Keep(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new KeepAsyncSignal<T>(source, predicate);

        /// <summary>Creates a new observable sequence that contains only the elements from the current sequence that satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The element is included in the resulting sequence if the
        /// function returns <see langword="true"/>.</param>
        /// <returns>An observable sequence that contains elements from the current sequence that satisfy the specified
        /// predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Keep(Func<T, bool> predicate) =>
            KeepSyncSignal<T>.Create(source, predicate);

        /// <summary>Keeps values that satisfy a stateful predicate.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the predicate.</param>
        /// <param name="predicate">The predicate that values and the state must satisfy.</param>
        /// <returns>An observable sequence of values that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> KeepWith<TState>(
            TState state,
            Func<TState, T, bool> predicate) =>
            KeepWithSyncSignal<T, TState>.Create(source, state, predicate);

        /// <summary>Creates a new observable sequence that contains only the elements from the source sequence that satisfy the specified asynchronous predicate.</summary>
        /// <param name="predicate">A function that evaluates each element and cancellation token.</param>
        /// <returns>An observable sequence that emits only those elements for which the predicate returns <see langword="true"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Where(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new KeepAsyncSignal<T>(source, predicate);

        /// <summary>Creates a new observable sequence that contains only the elements from the current sequence that satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the current sequence that satisfy the predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Where(Func<T, bool> predicate) =>
            KeepSyncSignal<T>.Create(source, predicate);
    }
}
