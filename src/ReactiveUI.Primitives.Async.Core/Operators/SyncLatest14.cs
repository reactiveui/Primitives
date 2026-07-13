// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides the arity-14 (<c>fourteen</c>-source) <c>CombineLatest</c> extension methods.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Combines the latest values from multiple asynchronous observable sources.</summary>
    /// <param name="src1">Source observable 1 whose latest value is combined.</param>
    /// <typeparam name="T1">The element type of source 1.</typeparam>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>
        /// Combines the latest values from 14 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <typeparam name="T10">The element type of source 10.</typeparam>
        /// <typeparam name="T11">The element type of source 11.</typeparam>
        /// <typeparam name="T12">The element type of source 12.</typeparam>
        /// <typeparam name="T13">The element type of source 13.</typeparam>
        /// <typeparam name="T14">The element type of source 14.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="src4">Source observable 4 whose latest value is combined.</param>
        /// <param name="src5">Source observable 5 whose latest value is combined.</param>
        /// <param name="src6">Source observable 6 whose latest value is combined.</param>
        /// <param name="src7">Source observable 7 whose latest value is combined.</param>
        /// <param name="src8">Source observable 8 whose latest value is combined.</param>
        /// <param name="src9">Source observable 9 whose latest value is combined.</param>
        /// <param name="src10">Source observable 10 whose latest value is combined.</param>
        /// <param name="src11">Source observable 11 whose latest value is combined.</param>
        /// <param name="src12">Source observable 12 whose latest value is combined.</param>
        /// <param name="src13">Source observable 13 whose latest value is combined.</param>
        /// <param name="src14">Source observable 14 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification =
                "An arity-N combinator takes N distinctly-typed sources; a parameter object would need the same N type arguments.")]
        public IObservableAsync<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            IObservableAsync<T4> src4,
            IObservableAsync<T5> src5,
            IObservableAsync<T6> src6,
            IObservableAsync<T7> src7,
            IObservableAsync<T8> src8,
            IObservableAsync<T9> src9,
            IObservableAsync<T10> src10,
            IObservableAsync<T11> src11,
            IObservableAsync<T12> src12,
            IObservableAsync<T13> src13,
            IObservableAsync<T14> src14,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
            new SyncLatest14Signal<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
                new(src1, src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13, src14),
                selector);

        /// <summary>
        /// Combines the latest values from 14 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <typeparam name="T10">The element type of source 10.</typeparam>
        /// <typeparam name="T11">The element type of source 11.</typeparam>
        /// <typeparam name="T12">The element type of source 12.</typeparam>
        /// <typeparam name="T13">The element type of source 13.</typeparam>
        /// <typeparam name="T14">The element type of source 14.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="src4">Source observable 4 whose latest value is combined.</param>
        /// <param name="src5">Source observable 5 whose latest value is combined.</param>
        /// <param name="src6">Source observable 6 whose latest value is combined.</param>
        /// <param name="src7">Source observable 7 whose latest value is combined.</param>
        /// <param name="src8">Source observable 8 whose latest value is combined.</param>
        /// <param name="src9">Source observable 9 whose latest value is combined.</param>
        /// <param name="src10">Source observable 10 whose latest value is combined.</param>
        /// <param name="src11">Source observable 11 whose latest value is combined.</param>
        /// <param name="src12">Source observable 12 whose latest value is combined.</param>
        /// <param name="src13">Source observable 13 whose latest value is combined.</param>
        /// <param name="src14">Source observable 14 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification =
                "An arity-N combinator takes N distinctly-typed sources; a parameter object would need the same N type arguments.")]
        public IObservableAsync<TResult>
            CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
                IObservableAsync<T2> src2,
                IObservableAsync<T3> src3,
                IObservableAsync<T4> src4,
                IObservableAsync<T5> src5,
                IObservableAsync<T6> src6,
                IObservableAsync<T7> src7,
                IObservableAsync<T8> src8,
                IObservableAsync<T9> src9,
                IObservableAsync<T10> src10,
                IObservableAsync<T11> src11,
                IObservableAsync<T12> src12,
                IObservableAsync<T13> src13,
                IObservableAsync<T14> src14,
                Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
            new SyncLatest14Signal<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
                new(src1, src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13, src14),
                selector);
    }
}
