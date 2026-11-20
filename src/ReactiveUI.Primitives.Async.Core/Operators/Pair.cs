// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Zip extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Pair/Zip operators for a first observable source sequence.</summary>
    /// <typeparam name="T1">The type of elements in the first source sequence.</typeparam>
    /// <param name="src1">The first observable sequence.</param>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>Combines two observable sequences element-by-element using the specified result selector.</summary>
        /// <typeparam name="T2">The type of elements in the second source sequence.</typeparam>
        /// <typeparam name="TResult">The type of elements in the result sequence.</typeparam>
        /// <param name="second">The second observable sequence. Cannot be null.</param>
        /// <param name="resultSelector">A function to apply to each pair of elements. Cannot be null.</param>
        /// <returns>An observable sequence whose elements are the result of pair-wise combining the source
        /// elements using the result selector.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="src1"/>, <paramref name="second"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
        /// <remarks>Values wait for their partner at the same index; the result completes when either source completes
        /// with no pending pair.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TResult> Pair<T2, TResult>(
            IObservableAsync<T2> second,
            Func<T1, T2, TResult> resultSelector) =>
            new ZipSignal<T1, T2, TResult>(src1, second, resultSelector);
    }
}
