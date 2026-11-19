// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Element-skipping operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns a new observable sequence that skips the specified number of elements from the start of the source sequence.</summary>
        /// <param name="count">The number of elements to skip. Must be greater than or equal to 0.</param>
        /// <returns>An observable sequence that contains the elements of the source sequence after the specified number of
        /// elements have been skipped. If the count is 0, the original sequence is returned.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Skip(int count) =>
            SkipSignal<T>.Create(source, count);
    }
}
