// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous list-materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> CollectListAsync() =>
            ToListTaskWitness<T>.RunListAsync(source);

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> ToListAsync() =>
            ToListTaskWitness<T>.RunListAsync(source);

        /// <summary>Asynchronously collects all elements from the source sequence into a list.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all elements in the
        /// source sequence, in the order they were received.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken) =>
            ToListTaskWitness<T>.RunListAsync(source, cancellationToken);

        /// <summary>Collects all values into an array.</summary>
        /// <returns>A task that completes with the collected array of values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<T[]> CollectArrayAsync() =>
            ToListTaskWitness<T>.RunArrayAsync(source);
    }
}
