// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>System.Reactive-named tuple-returning CombineLatest parity operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named tuple-returning latest-value combination operators.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <param name="source">Source observable 1 whose latest value is combined.</param>
    /// <remarks>Lower overload priority preserves existing selector calls that pass an untyped null selector.</remarks>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Combines latest values from 2 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(T First, T2 Second)> CombineLatest<T2>(IObservable<T2> source2) =>
            source.CombineLatest(source2, static (v1, v2) =>
                    (v1, v2));

        /// <summary>Combines latest values from 3 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(T First, T2 Second, T3 Third)> CombineLatest<T2, T3>(
            IObservable<T2> source2,
            IObservable<T3> source3) =>
            source.CombineLatest(
                source2,
                source3,
                static (v1, v2, v3) =>
                    (v1, v2, v3));

        /// <summary>Combines latest values from 4 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(T First, T2 Second, T3 Third, T4 Fourth)> CombineLatest<T2, T3, T4>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                static (v1, v2, v3, v4) =>
                    (v1, v2, v3, v4));

        /// <summary>Combines latest values from 5 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(T First, T2 Second, T3 Third, T4 Fourth, T5 Fifth)> CombineLatest<T2, T3, T4, T5>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                static (v1, v2, v3, v4, v5) =>
                    (v1, v2, v3, v4, v5));

        /// <summary>Combines latest values from 6 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth)> CombineLatest<T2, T3, T4, T5, T6>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                source6,
                static (v1, v2, v3, v4, v5, v6) =>
                    (v1, v2, v3, v4, v5, v6));

        /// <summary>Combines latest values from 7 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh)> CombineLatest<T2, T3, T4, T5, T6, T7>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                static (v1, v2, v3, v4, v5, v6, v7) =>
                    (v1, v2, v3, v4, v5, v6, v7));

        /// <summary>Combines latest values from 8 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                static (v1, v2, v3, v4, v5, v6, v7, v8) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8));

        /// <summary>Combines latest values from 9 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9));

        /// <summary>Combines latest values from 10 observable sources into tuple values.</summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <typeparam name="T10">The element type of source 10.</typeparam>
        /// <param name="source2">Source observable 2 whose latest value is combined.</param>
        /// <param name="source3">Source observable 3 whose latest value is combined.</param>
        /// <param name="source4">Source observable 4 whose latest value is combined.</param>
        /// <param name="source5">Source observable 5 whose latest value is combined.</param>
        /// <param name="source6">Source observable 6 whose latest value is combined.</param>
        /// <param name="source7">Source observable 7 whose latest value is combined.</param>
        /// <param name="source8">Source observable 8 whose latest value is combined.</param>
        /// <param name="source9">Source observable 9 whose latest value is combined.</param>
        /// <param name="source10">Source observable 10 whose latest value is combined.</param>
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10) =>
            source.CombineLatest(
                source2,
                source3,
                source4,
                source5,
                source6,
                source7,
                source8,
                source9,
                source10,
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10));

        /// <summary>Combines latest values from 11 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            IObservable<T11> source11) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11));

        /// <summary>Combines latest values from 12 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh,
            T12 Twelfth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(
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
            IObservable<T12> source12) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12));

        /// <summary>Combines latest values from 13 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh,
            T12 Twelfth,
            T13 Thirteenth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(
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
            IObservable<T13> source13) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13));

        /// <summary>Combines latest values from 14 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh,
            T12 Twelfth,
            T13 Thirteenth,
            T14 Fourteenth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
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
            IObservable<T14> source14) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14));

        /// <summary>Combines latest values from 15 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh,
            T12 Twelfth,
            T13 Thirteenth,
            T14 Fourteenth,
            T15 Fifteenth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
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
            IObservable<T15> source15) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15));

        /// <summary>Combines latest values from 16 observable sources into tuple values.</summary>
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
        /// <returns>An observable sequence of latest-value tuples.</returns>
        /// <exception cref="ArgumentNullException">A source is <see langword="null"/>.</exception>
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [OverloadResolutionPriority(-1)]
        public IObservable<(
            T First,
            T2 Second,
            T3 Third,
            T4 Fourth,
            T5 Fifth,
            T6 Sixth,
            T7 Seventh,
            T8 Eighth,
            T9 Ninth,
            T10 Tenth,
            T11 Eleventh,
            T12 Twelfth,
            T13 Thirteenth,
            T14 Fourteenth,
            T15 Fifteenth,
            T16 Sixteenth)> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
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
            IObservable<T16> source16) =>
            source.CombineLatest(
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
                static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16) =>
                    (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16));

    }
}
