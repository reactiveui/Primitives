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

/// <summary>Observable implementation for generated multi-source combine-latest overloads.</summary>
/// <typeparam name="TResult">The projected result type.</typeparam>
public sealed partial class CombineLatestSignal<TResult> : IObservable<TResult>
{
    /// <summary>Creates this subscription's typed slots and the projection that reads them.</summary>
    private readonly Func<CombineLatestCoordinator<TResult>, Func<TResult>> _connect;

    /// <summary>Initializes a new instance of the <see cref="CombineLatestSignal{TResult}"/> class.</summary>
    /// <param name="connect">
    /// Creates one typed slot per source against a fresh coordinator and returns the projection that reads them.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="connect"/> is <see langword="null"/>.</exception>
    public CombineLatestSignal(Func<CombineLatestCoordinator<TResult>, Func<TResult>> connect)
    {
        ArgumentExceptionHelper.ThrowIfNull(connect);

        _connect = connect;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CombineLatestCoordinator<TResult> coordinator = new(observer);
        return coordinator.Run(_connect(coordinator));
    }

    /// <summary>Creates an arity-3 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        Func<T1, T2, T3, TResult> selector) =>
        new(coordinator =>
        {
            var slot = coordinator.Attach(source);
            var slot2 = coordinator.Attach(source2);
            var slot3 = coordinator.Attach(source3);

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value);
        });

    /// <summary>Creates an arity-4 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        Func<T1, T2, T3, T4, TResult> selector) =>
        new(coordinator =>
        {
            var slot = coordinator.Attach(source);
            var slot2 = coordinator.Attach(source2);
            var slot3 = coordinator.Attach(source3);
            var slot4 = coordinator.Attach(source4);

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value);
        });

    /// <summary>Creates an arity-5 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="source5">The fifth source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        Func<T1, T2, T3, T4, T5, TResult> selector) =>
        new(coordinator =>
        {
            var slot = coordinator.Attach(source);
            var slot2 = coordinator.Attach(source2);
            var slot3 = coordinator.Attach(source3);
            var slot4 = coordinator.Attach(source4);
            var slot5 = coordinator.Attach(source5);

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value,
                slot5.Value);
        });

    /// <summary>Creates an arity-6 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="source5">The fifth source observable.</param>
    /// <param name="source6">The sixth source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        Func<T1, T2, T3, T4, T5, T6, TResult> selector) =>
        new(coordinator =>
        {
            var slot = coordinator.Attach(source);
            var slot2 = coordinator.Attach(source2);
            var slot3 = coordinator.Attach(source3);
            var slot4 = coordinator.Attach(source4);
            var slot5 = coordinator.Attach(source5);
            var slot6 = coordinator.Attach(source6);

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value,
                slot5.Value,
                slot6.Value);
        });

    /// <summary>Creates an arity-7 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <typeparam name="T7">The seventh source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="source5">The fifth source observable.</param>
    /// <param name="source6">The sixth source observable.</param>
    /// <param name="source7">The seventh source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        Func<T1, T2, T3, T4, T5, T6, T7, TResult> selector) =>
        new(coordinator =>
        {
            var slot = coordinator.Attach(source);
            var slot2 = coordinator.Attach(source2);
            var slot3 = coordinator.Attach(source3);
            var slot4 = coordinator.Attach(source4);
            var slot5 = coordinator.Attach(source5);
            var slot6 = coordinator.Attach(source6);
            var slot7 = coordinator.Attach(source7);

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value,
                slot5.Value,
                slot6.Value,
                slot7.Value);
        });

    /// <summary>Creates an arity-8 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <typeparam name="T7">The seventh source element type.</typeparam>
    /// <typeparam name="T8">The eighth source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="source5">The fifth source observable.</param>
    /// <param name="source6">The sixth source observable.</param>
    /// <param name="source7">The seventh source observable.</param>
    /// <param name="source8">The eighth source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> selector) =>
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

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value,
                slot5.Value,
                slot6.Value,
                slot7.Value,
                slot8.Value);
        });

    /// <summary>Creates an arity-9 combine-latest signal.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <typeparam name="T7">The seventh source element type.</typeparam>
    /// <typeparam name="T8">The eighth source element type.</typeparam>
    /// <typeparam name="T9">The ninth source element type.</typeparam>
    /// <param name="source">The first source observable.</param>
    /// <param name="source2">The second source observable.</param>
    /// <param name="source3">The third source observable.</param>
    /// <param name="source4">The fourth source observable.</param>
    /// <param name="source5">The fifth source observable.</param>
    /// <param name="source6">The sixth source observable.</param>
    /// <param name="source7">The seventh source observable.</param>
    /// <param name="source8">The eighth source observable.</param>
    /// <param name="source9">The ninth source observable.</param>
    /// <param name="selector">The selector that combines latest values from all sources.</param>
    /// <returns>The combine-latest signal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Maintainability",
        "SST1472:Signatures should not declare too many parameters",
        Justification = "An arity-N combinator takes one observable per source.")]
    internal static CombineLatestSignal<TResult> Create<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
        IObservable<T1> source,
        IObservable<T2> source2,
        IObservable<T3> source3,
        IObservable<T4> source4,
        IObservable<T5> source5,
        IObservable<T6> source6,
        IObservable<T7> source7,
        IObservable<T8> source8,
        IObservable<T9> source9,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> selector) =>
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

            return () => selector(
                slot.Value,
                slot2.Value,
                slot3.Value,
                slot4.Value,
                slot5.Value,
                slot6.Value,
                slot7.Value,
                slot8.Value,
                slot9.Value);
        });
}
