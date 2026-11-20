// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Extension methods for <see cref="IObserver{T}"/>.</summary>
public static class ObserverExtensions
{
    /// <summary>Fast-path iteration helpers for emitting collections to an observer.</summary>
    /// <typeparam name="T">The type of elements in the source collection.</typeparam>
    /// <param name="observer">The observer to emit elements to.</param>
    extension<T>(IObserver<T> observer)
    {
        /// <summary>
        /// Emits every element of <paramref name="source"/> to the observer in order through
        /// <see cref="IObserver{T}.OnNext(T)"/>, indexing arrays, <see cref="List{T}"/> and the list interfaces directly
        /// and enumerating anything else.
        /// </summary>
        /// <param name="source">The source collection to iterate.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void FastForEach(IEnumerable<T> source) =>
            ObserverArrayHelpers.FastForEach(observer, source);
    }
}
