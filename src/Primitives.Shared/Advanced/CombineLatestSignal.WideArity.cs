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

/// <summary>Wide-arity static factory methods for combine-latest signals over ten through sixteen sources.</summary>
public static partial class CombineLatestSignal
{
    /// <summary>Creates an arity-10 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <typeparam name="T9">The element type of source 9.</typeparam>
    /// <typeparam name="T10">The element type of source 10.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        IObservable<T9> source9,
        IObservable<T10> source10,
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> selector) =>
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
            source10,
            selector);

    /// <summary>Creates an arity-11 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult> selector) =>
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
            source10,
            source11,
            selector);

    /// <summary>Creates an arity-12 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult> selector) =>
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
            source10,
            source11,
            source12,
            selector);

    /// <summary>Creates an arity-13 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) =>
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
            source10,
            source11,
            source12,
            source13,
            selector);

    /// <summary>Creates an arity-14 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
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
            source10,
            source11,
            source12,
            source13,
            source14,
            selector);

    /// <summary>Creates an arity-15 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
    /// <param name="source15">Source observable 15.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult> selector) =>
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
            source10,
            source11,
            source12,
            source13,
            source14,
            source15,
            selector);

    /// <summary>Creates an arity-16 combine-latest signal with result-type inference.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
    /// <param name="source15">Source observable 15.</param>
    /// <param name="source16">Source observable 16.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static CombineLatestSignal<TResult> Create<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>(
        IObservable<T> source,
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
        Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) =>
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
            source10,
            source11,
            source12,
            source13,
            source14,
            source15,
            source16,
            selector);

    /// <summary>Combines latest values from 10 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
    /// <typeparam name="T2">The element type of source 2.</typeparam>
    /// <typeparam name="T3">The element type of source 3.</typeparam>
    /// <typeparam name="T4">The element type of source 4.</typeparam>
    /// <typeparam name="T5">The element type of source 5.</typeparam>
    /// <typeparam name="T6">The element type of source 6.</typeparam>
    /// <typeparam name="T7">The element type of source 7.</typeparam>
    /// <typeparam name="T8">The element type of source 8.</typeparam>
    /// <typeparam name="T9">The element type of source 9.</typeparam>
    /// <typeparam name="T10">The element type of source 10.</typeparam>
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
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
        T9 Ninth,
        T10 Tenth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
        IObservable<T> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        IObservable<T9> source9,
        IObservable<T10> source10) =>
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
            source10,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10));

    /// <summary>Combines latest values from 11 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
        IObservable<T> source,
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
            source10,
            source11,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11));

    /// <summary>Combines latest values from 12 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh,
        T12 Twelfth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(
        IObservable<T> source,
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
            source10,
            source11,
            source12,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12));

    /// <summary>Combines latest values from 13 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh,
        T12 Twelfth,
        T13 Thirteenth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(
        IObservable<T> source,
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
            source10,
            source11,
            source12,
            source13,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13));

    /// <summary>Combines latest values from 14 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh,
        T12 Twelfth,
        T13 Thirteenth,
        T14 Fourteenth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
        IObservable<T> source,
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
            source10,
            source11,
            source12,
            source13,
            source14,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14));

    /// <summary>Combines latest values from 15 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
    /// <param name="source15">Source observable 15.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh,
        T12 Twelfth,
        T13 Thirteenth,
        T14 Fourteenth,
        T15 Fifteenth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
        IObservable<T> source,
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
            source10,
            source11,
            source12,
            source13,
            source14,
            source15,
            static (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15) =>
                (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15));

    /// <summary>Combines latest values from 16 observable sources into tuple values.</summary>
    /// <typeparam name="T">The element type of source 1.</typeparam>
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
    /// <param name="source">Source observable 1.</param>
    /// <param name="source2">Source observable 2.</param>
    /// <param name="source3">Source observable 3.</param>
    /// <param name="source4">Source observable 4.</param>
    /// <param name="source5">Source observable 5.</param>
    /// <param name="source6">Source observable 6.</param>
    /// <param name="source7">Source observable 7.</param>
    /// <param name="source8">Source observable 8.</param>
    /// <param name="source9">Source observable 9.</param>
    /// <param name="source10">Source observable 10.</param>
    /// <param name="source11">Source observable 11.</param>
    /// <param name="source12">Source observable 12.</param>
    /// <param name="source13">Source observable 13.</param>
    /// <param name="source14">Source observable 14.</param>
    /// <param name="source15">Source observable 15.</param>
    /// <param name="source16">Source observable 16.</param>
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
        T9 Ninth,
        T10 Tenth,
        T11 Eleventh,
        T12 Twelfth,
        T13 Thirteenth,
        T14 Fourteenth,
        T15 Fifteenth,
        T16 Sixteenth)> CreateTuple<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
        IObservable<T> source,
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
