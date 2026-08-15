// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides the arity-3 (<c>three</c>-source) <c>CombineLatest</c> extension methods.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Combines the latest values from multiple asynchronous observable sources.</summary>
    /// <typeparam name="T1">The element type of source 1.</typeparam>
    /// <param name="src1">Source observable 1 whose latest value is combined.</param>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>
        /// Combines the latest values from 3 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TResult> SyncLatest<T2, T3, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            Func<T1, T2, T3, TResult> selector) =>
            new SyncLatest3Signal<T1, T2, T3, TResult>(
                new(src1, src2, src3),
                selector);

        /// <summary>
        /// Combines the latest values from 3 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TResult> CombineLatest<T2, T3, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            Func<T1, T2, T3, TResult> selector) =>
            new SyncLatest3Signal<T1, T2, T3, TResult>(
                new(src1, src2, src3),
                selector);
    }
}
