// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides TakeWhile extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>TakeWhile operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns elements from the observable sequence as long as the specified asynchronous condition is true, then completes.</summary>
        /// <param name="predicate">An asynchronous function to test each element for a condition. Receives the element
        /// and a cancellation token.</param>
        /// <returns>An observable sequence that contains elements from the source sequence that satisfy the
        /// condition, completing as soon as the predicate returns false.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeWhile(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new TakeWhileAsyncSignal<T>(source, predicate);

        /// <summary>Returns elements from the observable sequence as long as the specified condition is true, then completes.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the source sequence that satisfy the
        /// condition, completing as soon as the predicate returns false.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> TakeWhile(Func<T, bool> predicate) =>
            new TakeWhileSyncSignal<T>(source, predicate);
    }
}
