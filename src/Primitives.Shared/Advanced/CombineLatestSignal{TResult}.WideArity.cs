// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>A combine-latest signal, carrying the factories for ten through sixteen sources.</summary>
public sealed partial class CombineLatestSignal<TResult>
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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            return () => selector(h.Slot1.Value, h.Slot2.Value, h.Slot3.Value, h.Slot4.Value, h.Slot5.Value, h.Slot6.Value, h.Slot7.Value, h.Slot8.Value, s9.Value, s10.Value);
        });
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(source11);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            var s11 = coordinator.Attach(source11);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                s9.Value,
                s10.Value,
                s11.Value);
        });
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(source11);
        ArgumentExceptionHelper.ThrowIfNull(source12);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            var s11 = coordinator.Attach(source11);
            var s12 = coordinator.Attach(source12);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                s9.Value,
                s10.Value,
                s11.Value,
                s12.Value);
        });
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(source11);
        ArgumentExceptionHelper.ThrowIfNull(source12);
        ArgumentExceptionHelper.ThrowIfNull(source13);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            var s11 = coordinator.Attach(source11);
            var s12 = coordinator.Attach(source12);
            var s13 = coordinator.Attach(source13);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                s9.Value,
                s10.Value,
                s11.Value,
                s12.Value,
                s13.Value);
        });
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(source11);
        ArgumentExceptionHelper.ThrowIfNull(source12);
        ArgumentExceptionHelper.ThrowIfNull(source13);
        ArgumentExceptionHelper.ThrowIfNull(source14);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return CreateFromOctetAndSix(head, source9, source10, source11, source12, source13, source14, selector);
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        ArgumentExceptionHelper.ThrowIfNull(source9);
        ArgumentExceptionHelper.ThrowIfNull(source10);
        ArgumentExceptionHelper.ThrowIfNull(source11);
        ArgumentExceptionHelper.ThrowIfNull(source12);
        ArgumentExceptionHelper.ThrowIfNull(source13);
        ArgumentExceptionHelper.ThrowIfNull(source14);
        ArgumentExceptionHelper.ThrowIfNull(source15);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return CreateFromOctetAndSeven(head, source9, source10, source11, source12, source13, source14, source15, selector);
    }

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
    /// <exception cref="ArgumentNullException">A source or <paramref name="selector"/> is <see langword="null"/>.</exception>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    public static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
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
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector)
    {
        var head = CombineLatestWideHelper.ValidateOctet(new CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
            source,
            source2,
            source3,
            source4,
            source5,
            source6,
            source7,
            source8));
        const int secondOctetStartIndex = 9;
        var tail = CombineLatestWideHelper.ValidateOctet(
            new CombineLatestOctet<T9, T10, T11, T12, T13, T14, T15, T16>(
                source9,
                source10,
                source11,
                source12,
                source13,
                source14,
                source15,
                source16),
            nameof(source9),
            secondOctetStartIndex);
        ArgumentExceptionHelper.ThrowIfNull(selector);
        return CreateFromTwoOctets(head, tail, selector);
    }

    /// <summary>Creates an arity-14 signal from a validated leading octet and six trailing sources.</summary>
    /// <typeparam name="T1">Source 1 element type.</typeparam>
    /// <typeparam name="T2">Source 2 element type.</typeparam>
    /// <typeparam name="T3">Source 3 element type.</typeparam>
    /// <typeparam name="T4">Source 4 element type.</typeparam>
    /// <typeparam name="T5">Source 5 element type.</typeparam>
    /// <typeparam name="T6">Source 6 element type.</typeparam>
    /// <typeparam name="T7">Source 7 element type.</typeparam>
    /// <typeparam name="T8">Source 8 element type.</typeparam>
    /// <typeparam name="T9">Source 9 element type.</typeparam>
    /// <typeparam name="T10">Source 10 element type.</typeparam>
    /// <typeparam name="T11">Source 11 element type.</typeparam>
    /// <typeparam name="T12">Source 12 element type.</typeparam>
    /// <typeparam name="T13">Source 13 element type.</typeparam>
    /// <typeparam name="T14">Source 14 element type.</typeparam>
    /// <param name="head">The first eight validated sources.</param>
    /// <param name="source9">Source 9.</param>
    /// <param name="source10">Source 10.</param>
    /// <param name="source11">Source 11.</param>
    /// <param name="source12">Source 12.</param>
    /// <param name="source13">Source 13.</param>
    /// <param name="source14">Source 14.</param>
    /// <param name="selector">The 14-argument projection.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "Attaches the validated head octet and six trailing sources.")]
    private static CombineLatestSignal<TResult> CreateFromOctetAndSix<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>(
        CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> head,
        IObservable<T9> source9,
        IObservable<T10> source10,
        IObservable<T11> source11,
        IObservable<T12> source12,
        IObservable<T13> source13,
        IObservable<T14> source14,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector) =>
        new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            var s11 = coordinator.Attach(source11);
            var s12 = coordinator.Attach(source12);
            var s13 = coordinator.Attach(source13);
            var s14 = coordinator.Attach(source14);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                s9.Value,
                s10.Value,
                s11.Value,
                s12.Value,
                s13.Value,
                s14.Value);
        });

    /// <summary>Creates an arity-15 signal from a validated leading octet and seven trailing sources.</summary>
    /// <typeparam name="T1">Source 1 element type.</typeparam>
    /// <typeparam name="T2">Source 2 element type.</typeparam>
    /// <typeparam name="T3">Source 3 element type.</typeparam>
    /// <typeparam name="T4">Source 4 element type.</typeparam>
    /// <typeparam name="T5">Source 5 element type.</typeparam>
    /// <typeparam name="T6">Source 6 element type.</typeparam>
    /// <typeparam name="T7">Source 7 element type.</typeparam>
    /// <typeparam name="T8">Source 8 element type.</typeparam>
    /// <typeparam name="T9">Source 9 element type.</typeparam>
    /// <typeparam name="T10">Source 10 element type.</typeparam>
    /// <typeparam name="T11">Source 11 element type.</typeparam>
    /// <typeparam name="T12">Source 12 element type.</typeparam>
    /// <typeparam name="T13">Source 13 element type.</typeparam>
    /// <typeparam name="T14">Source 14 element type.</typeparam>
    /// <typeparam name="T15">Source 15 element type.</typeparam>
    /// <param name="head">The first eight validated sources.</param>
    /// <param name="source9">Source 9.</param>
    /// <param name="source10">Source 10.</param>
    /// <param name="source11">Source 11.</param>
    /// <param name="source12">Source 12.</param>
    /// <param name="source13">Source 13.</param>
    /// <param name="source14">Source 14.</param>
    /// <param name="source15">Source 15.</param>
    /// <param name="selector">The 15-argument projection.</param>
    /// <returns>The combine-latest signal.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "Attaches the validated head octet and seven trailing sources.")]
    private static CombineLatestSignal<TResult> CreateFromOctetAndSeven<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>(
        CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> head,
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
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var s9 = coordinator.Attach(source9);
            var s10 = coordinator.Attach(source10);
            var s11 = coordinator.Attach(source11);
            var s12 = coordinator.Attach(source12);
            var s13 = coordinator.Attach(source13);
            var s14 = coordinator.Attach(source14);
            var s15 = coordinator.Attach(source15);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                s9.Value,
                s10.Value,
                s11.Value,
                s12.Value,
                s13.Value,
                s14.Value,
                s15.Value);
        });

    /// <summary>Creates an arity-16 signal from two validated octets.</summary>
    /// <typeparam name="T1">Source 1 element type.</typeparam>
    /// <typeparam name="T2">Source 2 element type.</typeparam>
    /// <typeparam name="T3">Source 3 element type.</typeparam>
    /// <typeparam name="T4">Source 4 element type.</typeparam>
    /// <typeparam name="T5">Source 5 element type.</typeparam>
    /// <typeparam name="T6">Source 6 element type.</typeparam>
    /// <typeparam name="T7">Source 7 element type.</typeparam>
    /// <typeparam name="T8">Source 8 element type.</typeparam>
    /// <typeparam name="T9">Source 9 element type.</typeparam>
    /// <typeparam name="T10">Source 10 element type.</typeparam>
    /// <typeparam name="T11">Source 11 element type.</typeparam>
    /// <typeparam name="T12">Source 12 element type.</typeparam>
    /// <typeparam name="T13">Source 13 element type.</typeparam>
    /// <typeparam name="T14">Source 14 element type.</typeparam>
    /// <typeparam name="T15">Source 15 element type.</typeparam>
    /// <typeparam name="T16">Source 16 element type.</typeparam>
    /// <param name="head">The first eight validated sources.</param>
    /// <param name="tail">The trailing eight validated sources.</param>
    /// <param name="selector">The 16-argument projection.</param>
    /// <returns>The combine-latest signal.</returns>
    private static CombineLatestSignal<TResult> CreateFromTwoOctets<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>(
        CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> head,
        CombineLatestOctet<T9, T10, T11, T12, T13, T14, T15, T16> tail,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) =>
        new(coordinator =>
        {
            var h = CombineLatestWideHelper.AttachOctet(coordinator, head);
            var t = CombineLatestWideHelper.AttachOctet(coordinator, tail);
            return () => selector(
                h.Slot1.Value,
                h.Slot2.Value,
                h.Slot3.Value,
                h.Slot4.Value,
                h.Slot5.Value,
                h.Slot6.Value,
                h.Slot7.Value,
                h.Slot8.Value,
                t.Slot1.Value,
                t.Slot2.Value,
                t.Slot3.Value,
                t.Slot4.Value,
                t.Slot5.Value,
                t.Slot6.Value,
                t.Slot7.Value,
                t.Slot8.Value);
        });
}
