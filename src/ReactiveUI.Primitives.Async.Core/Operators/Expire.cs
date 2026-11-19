// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Timeout extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Timeout operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Completes with a TimeoutException when the next element misses the deadline.</summary>
        /// <param name="dueTime">The maximum time span allowed between consecutive elements. Must be positive.</param>
        /// <returns>An observable sequence that mirrors the source but completes with a
        /// <see cref="TimeoutException"/> once an inter-element gap exceeds <paramref name="dueTime"/>.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="dueTime"/> is negative or zero.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Expire(TimeSpan dueTime) =>
            TimeoutSignal<T>.Create(source, dueTime);
    }
}
