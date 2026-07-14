// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides the arity-2 (<c>two</c>-source) <c>CombineLatest</c> extension methods.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Combines the latest values from multiple asynchronous observable sources.</summary>
    /// <param name="src1">Source observable 1 whose latest value is combined.</param>
    /// <typeparam name="T1">The element type of source 1.</typeparam>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>
        /// Combines the latest values from two asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <remarks>
        /// The returned sequence does not produce a value until every source has emitted at least
        /// once. After that, each new value from any source produces a fresh projection using the
        /// most recent value from each. Completion / failure of any source propagates downstream.
        /// </remarks>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        public IObservableAsync<TResult> SyncLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            new SyncLatest2Signal<T1, T2, TResult>(
                new(src1, src2),
                selector);

        /// <summary>
        /// Combines the latest values from two asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        public IObservableAsync<TResult> CombineLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            new SyncLatest2Signal<T1, T2, TResult>(
                new(src1, src2),
                selector);

        /// <summary>Combines latest values from two sources.</summary>
        /// <typeparam name="T2">The second element type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="src2">The second source sequence.</param>
        /// <param name="selector">The function combining the latest values.</param>
        /// <returns>An observable sequence of combined values.</returns>
        public IObservableAsync<TResult> PairLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            new SyncLatest2Signal<T1, T2, TResult>(
                new(src1, src2),
                selector);
    }
}
