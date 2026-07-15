// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Primitives-named multi-source latest-value combination operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Primitives-named latest-value combination operators for multiple observable sources.</summary>
    /// <param name="source">Source observable 1 whose latest value is combined.</param>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>
        /// Combines the latest values from 3 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<T2, T3, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            Func<T, T2, T3, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 4 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<T2, T3, T4, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            Func<T, T2, T3, T4, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 5 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            Func<T, T2, T3, T4, T5, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 6 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            Func<T, T2, T3, T4, T5, T6, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 7 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            Func<T, T2, T3, T4, T5, T6, T7, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return CombineLatestSignal<TResult>.Create(
                source,
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                selector);
        }

        /// <summary>
        /// Combines the latest values from 8 observable sources into a single sequence,
        /// projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            Func<T, T2, T3, T4, T5, T6, T7, T8, TResult> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(source2);
            ArgumentExceptionHelper.ThrowIfNull(source3);
            ArgumentExceptionHelper.ThrowIfNull(source4);
            ArgumentExceptionHelper.ThrowIfNull(source5);
            ArgumentExceptionHelper.ThrowIfNull(source6);
            ArgumentExceptionHelper.ThrowIfNull(source7);
            ArgumentExceptionHelper.ThrowIfNull(source8);
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
                selector);
        }

        /// <summary>
        /// Combines the latest values from 9 observable sources into a single sequence,
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
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        /// <exception cref="ArgumentNullException">A source or selector is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        public IObservable<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult> selector)
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
                selector);
        }
    }
}
