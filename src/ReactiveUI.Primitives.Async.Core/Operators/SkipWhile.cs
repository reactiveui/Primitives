// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides SkipWhile extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>SkipWhile operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Bypasses elements in the observable sequence as long as the specified asynchronous condition is true, then emits all remaining elements.</summary>
        /// <param name="predicate">An asynchronous function to test each element for a condition. Receives the element
        /// and a cancellation token.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> SkipWhile(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new SkipWhileAsyncSignal<T>(source, predicate);

        /// <summary>Bypasses elements in the observable sequence as long as the specified condition is true, then emits all remaining elements.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> SkipWhile(Func<T, bool> predicate) =>
            new SkipWhileSyncSignal<T>(source, predicate);
    }
}
