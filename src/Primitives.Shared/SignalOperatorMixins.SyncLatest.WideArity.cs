// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Primitives-named latest-value combination over ten through sixteen observable sources.</summary>
public static partial class LinqExtensions
{
    /// <summary>Primitives-named latest-value combination operators for ten through sixteen observable sources.</summary>
    /// <param name="source">Source observable 1 whose latest value is combined.</param>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>
        /// Combines the latest values from 10 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 11 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 12 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="source12">Source observable 12 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(source12);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                source12,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 13 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="source12">Source observable 12 whose latest value is combined.</param>
        /// <param name="source13">Source observable 13 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(source12);
            ArgumentExceptionHelper.ThrowIfNull(source13);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                source12,
                source13,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 14 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="source12">Source observable 12 whose latest value is combined.</param>
        /// <param name="source13">Source observable 13 whose latest value is combined.</param>
        /// <param name="source14">Source observable 14 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(source12);
            ArgumentExceptionHelper.ThrowIfNull(source13);
            ArgumentExceptionHelper.ThrowIfNull(source14);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                source12,
                source13,
                source14,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 15 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="T15">The element type of source 15.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="source12">Source observable 12 whose latest value is combined.</param>
        /// <param name="source13">Source observable 13 whose latest value is combined.</param>
        /// <param name="source14">Source observable 14 whose latest value is combined.</param>
        /// <param name="source15">Source observable 15 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            IObservable<T15> source15,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(source12);
            ArgumentExceptionHelper.ThrowIfNull(source13);
            ArgumentExceptionHelper.ThrowIfNull(source14);
            ArgumentExceptionHelper.ThrowIfNull(source15);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                source12,
                source13,
                source14,
                source15,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 16 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
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
        /// <typeparam name="T15">The element type of source 15.</typeparam>
        /// <typeparam name="T16">The element type of source 16.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <param name="source11">Source observable 11 whose latest value is combined.</param>
        /// <param name="source12">Source observable 12 whose latest value is combined.</param>
        /// <param name="source13">Source observable 13 whose latest value is combined.</param>
        /// <param name="source14">Source observable 14 whose latest value is combined.</param>
        /// <param name="source15">Source observable 15 whose latest value is combined.</param>
        /// <param name="source16">Source observable 16 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11,
            IObservable<T12> source12,
            IObservable<T13> source13,
            IObservable<T14> source14,
            IObservable<T15> source15,
            IObservable<T16> source16,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
            ArgumentExceptionHelper.ThrowIfNull(source9);
            ArgumentExceptionHelper.ThrowIfNull(source10);
            ArgumentExceptionHelper.ThrowIfNull(source11);
            ArgumentExceptionHelper.ThrowIfNull(source12);
            ArgumentExceptionHelper.ThrowIfNull(source13);
            ArgumentExceptionHelper.ThrowIfNull(source14);
            ArgumentExceptionHelper.ThrowIfNull(source15);
            ArgumentExceptionHelper.ThrowIfNull(source16);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                source11,
                source12,
                source13,
                source14,
                source15,
                source16,
                selector);
        }
    }
}
