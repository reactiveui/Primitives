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

/// <summary>Combine-latest factories for the wide arities, ten through sixteen sources.</summary>
public static partial class LinqExtensions
{
    /// <summary>A combine-latest signal, carrying the factories for ten through sixteen sources.</summary>
    private sealed partial class CombineLatestSignal<TResult>
    {
        /// <summary>Creates an arity-10 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
            IObservable<T1> source,
            IObservable<T2> source2,
            IObservable<T3> source3,
            IObservable<T4> source4,
            IObservable<T5> source5,
            IObservable<T6> source6,
            IObservable<T7> source7,
            IObservable<T8> source8,
            IObservable<T9> source9,
            IObservable<T10> source10,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value);
            });

        /// <summary>Creates an arity-11 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
            IObservable<T1> source,
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
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value);
            });

        /// <summary>Creates an arity-12 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(
            IObservable<T1> source,
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
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);
                var slot12 = coordinator.Attach(source12);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value,
                    slot12.Value);
            });

        /// <summary>Creates an arity-13 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(
            IObservable<T1> source,
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
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);
                var slot12 = coordinator.Attach(source12);
                var slot13 = coordinator.Attach(source13);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value,
                    slot12.Value,
                    slot13.Value);
            });

        /// <summary>Creates an arity-14 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult>
            Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
                IObservable<T1> source,
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
                Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);
                var slot12 = coordinator.Attach(source12);
                var slot13 = coordinator.Attach(source13);
                var slot14 = coordinator.Attach(source14);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value,
                    slot12.Value,
                    slot13.Value,
                    slot14.Value);
            });

        /// <summary>Creates an arity-15 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <typeparam name="T15">The fifteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="source15">The fifteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
            IObservable<T1> source,
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
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);
                var slot12 = coordinator.Attach(source12);
                var slot13 = coordinator.Attach(source13);
                var slot14 = coordinator.Attach(source14);
                var slot15 = coordinator.Attach(source15);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value,
                    slot12.Value,
                    slot13.Value,
                    slot14.Value,
                    slot15.Value);
            });

        /// <summary>Creates an arity-16 combine-latest signal.</summary>
        /// <typeparam name="T1">The first source element type.</typeparam>
        /// <typeparam name="T2">The second source element type.</typeparam>
        /// <typeparam name="T3">The third source element type.</typeparam>
        /// <typeparam name="T4">The fourth source element type.</typeparam>
        /// <typeparam name="T5">The fifth source element type.</typeparam>
        /// <typeparam name="T6">The sixth source element type.</typeparam>
        /// <typeparam name="T7">The seventh source element type.</typeparam>
        /// <typeparam name="T8">The eighth source element type.</typeparam>
        /// <typeparam name="T9">The ninth source element type.</typeparam>
        /// <typeparam name="T10">The tenth source element type.</typeparam>
        /// <typeparam name="T11">The eleventh source element type.</typeparam>
        /// <typeparam name="T12">The twelfth source element type.</typeparam>
        /// <typeparam name="T13">The thirteenth source element type.</typeparam>
        /// <typeparam name="T14">The fourteenth source element type.</typeparam>
        /// <typeparam name="T15">The fifteenth source element type.</typeparam>
        /// <typeparam name="T16">The sixteenth source element type.</typeparam>
        /// <param name="source">The first source observable.</param>
        /// <param name="source2">The second source observable.</param>
        /// <param name="source3">The third source observable.</param>
        /// <param name="source4">The fourth source observable.</param>
        /// <param name="source5">The fifth source observable.</param>
        /// <param name="source6">The sixth source observable.</param>
        /// <param name="source7">The seventh source observable.</param>
        /// <param name="source8">The eighth source observable.</param>
        /// <param name="source9">The ninth source observable.</param>
        /// <param name="source10">The tenth source observable.</param>
        /// <param name="source11">The eleventh source observable.</param>
        /// <param name="source12">The twelfth source observable.</param>
        /// <param name="source13">The thirteenth source observable.</param>
        /// <param name="source14">The fourteenth source observable.</param>
        /// <param name="source15">The fifteenth source observable.</param>
        /// <param name="source16">The sixteenth source observable.</param>
        /// <param name="selector">The selector that combines latest values from all sources.</param>
        /// <returns>The combine-latest signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Maintainability",
            "SST1472:Signatures should not declare too many parameters",
            Justification = "An arity-N combinator takes one observable per source; a parameter object would erase the element type each source contributes to the selector.")]
        internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
            IObservable<T1> source,
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
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) =>
            new(coordinator =>
            {
                var slot = coordinator.Attach(source);
                var slot2 = coordinator.Attach(source2);
                var slot3 = coordinator.Attach(source3);
                var slot4 = coordinator.Attach(source4);
                var slot5 = coordinator.Attach(source5);
                var slot6 = coordinator.Attach(source6);
                var slot7 = coordinator.Attach(source7);
                var slot8 = coordinator.Attach(source8);
                var slot9 = coordinator.Attach(source9);
                var slot10 = coordinator.Attach(source10);
                var slot11 = coordinator.Attach(source11);
                var slot12 = coordinator.Attach(source12);
                var slot13 = coordinator.Attach(source13);
                var slot14 = coordinator.Attach(source14);
                var slot15 = coordinator.Attach(source15);
                var slot16 = coordinator.Attach(source16);

                return () => selector(
                    slot.Value,
                    slot2.Value,
                    slot3.Value,
                    slot4.Value,
                    slot5.Value,
                    slot6.Value,
                    slot7.Value,
                    slot8.Value,
                    slot9.Value,
                    slot10.Value,
                    slot11.Value,
                    slot12.Value,
                    slot13.Value,
                    slot14.Value,
                    slot15.Value,
                    slot16.Value);
            });
    }
}
