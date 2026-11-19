// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Static factory methods for collection-returning and tuple-returning combine-latest signals.</summary>
public static partial class CombineLatestSignal
{
    /// <summary>Combines the latest value of every source in <paramref name="sources"/> into a list.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources whose latest values are combined.</param>
    /// <returns>An observable sequence of latest-value lists, or an empty sequence when <paramref name="sources"/> is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> or one of its elements is <see langword="null"/>.</exception>
    public static IObservable<IList<T>> CreateList<T>(IEnumerable<IObservable<T>> sources)
    {
        var materialized = MaterializeSources(sources);
        return materialized.Length == 0
            ? ImmutableEmptySignal<IList<T>>.Instance
            : CombineLatestSignal<IList<T>>.Create(materialized, CombineLatestSignal<IList<T>>.Snapshot);
    }

    /// <summary>Creates an arity-3 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        Func<T, T2, T3, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            selector);

    /// <summary>Creates an arity-4 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        Func<T, T2, T3, T4, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            source4,
            selector);

    /// <summary>Creates an arity-5 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        Func<T, T2, T3, T4, T5, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            source4,
            source5,
            selector);

    /// <summary>Creates an arity-6 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        Func<T, T2, T3, T4, T5, T6, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            selector);

    /// <summary>Creates an arity-7 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        Func<T, T2, T3, T4, T5, T6, T7, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            selector);

    /// <summary>Creates an arity-8 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        Func<T, T2, T3, T4, T5, T6, T7, T8, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8,
            selector);

    /// <summary>Creates an arity-9 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <typeparam name="T9">The element type of source 9.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        IObservable<T9> source9,
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult> selector) =>
        CombineLatestSignal<TResult>.Create(
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

    /// <summary>Combines latest values from 2 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(T First, T2 Second)> CreateTuple<T, T2>(
        IObservable<T> source,
        IObservable<T2> source2) =>
        CombineLatestSignal<T, T2, (T First, T2 Second)>.CreateCombineLatest(
            source,
            source2,
            static (v1, v2) => (v1, v2));

    /// <summary>Combines latest values from 3 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third)> CreateTuple<T, T2, T3>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3) =>
        Create(
            source,
            source2,
            source3,
            static (v1, v2, v3) =>
                (v1, v2, v3));

    /// <summary>Combines latest values from 4 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth)> CreateTuple<T, T2, T3, T4>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4) =>
        Create(
            source,
            source2,
            source3,
            source4,
            static (v1, v2, v3, v4) =>
                (v1, v2, v3, v4));

    /// <summary>Combines latest values from 5 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth,
        T5 Fifth)> CreateTuple<T, T2, T3, T4, T5>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5) =>
        Create(
            source,
            source2,
            source3,
            source4,
            source5,
            static (v1, v2, v3, v4, v5) =>
                (v1, v2, v3, v4, v5));

    /// <summary>Combines latest values from 6 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth,
        T5 Fifth,
        T6 Sixth)> CreateTuple<T, T2, T3, T4, T5, T6>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6) =>
        Create(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            static (v1, v2, v3, v4, v5, v6) =>
                (v1, v2, v3, v4, v5, v6));

    /// <summary>Combines latest values from 7 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth,
        T5 Fifth,
        T6 Sixth,
        T7 Seventh)> CreateTuple<T, T2, T3, T4, T5, T6, T7>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7) =>
        Create(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            static (v1, v2, v3, v4, v5, v6, v7) =>
                (v1, v2, v3, v4, v5, v6, v7));

    /// <summary>Combines latest values from 8 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth,
        T5 Fifth,
        T6 Sixth,
        T7 Seventh,
        T8 Eighth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8) =>
        Create(
            source,
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
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <typeparam name="T9">The element type of source 9.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <returns>An observable sequence of latest-value tuples.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<(
        T First,
        T2 Second,
        T3 Third,
        T4 Fourth,
        T5 Fifth,
        T6 Sixth,
        T7 Seventh,
        T8 Eighth,
        T9 Ninth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        IObservable<T9> source9) =>
        Create(
            source,
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

    /// <summary>Materializes and validates the sources supplied to a collection-based combine-latest operator.</summary>
    /// <typeparam name="T">The element type shared by every source.</typeparam>
    /// <param name="sources">The observable sources.</param>
    /// <returns>The validated source array.</returns>
    internal static IObservable<T>[] MaterializeSources<T>(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        if (sources is not IObservable<T>[] materialized)
        {
            List<IObservable<T>> buffer = [];
            buffer.AddRange(sources);
            materialized = buffer.ToArray();
        }

        for (var i = 0; i < materialized.Length; i++)
        {
            ArgumentExceptionHelper.ThrowIfNull(materialized[i]);
        }

        return materialized;
    }
}
