// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Creates a signal that emits a single value on the supplied scheduler and completes.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <param name="scheduler">The scheduler the value is emitted on.</param>
    /// <returns>A signal that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<T> Emit<T>(T value, ISequencer scheduler) => scheduler == Sequencer.Immediate
        ? new ImmediateReturnSignal<T>(value)
        : new ReturnSignal<T>(value, scheduler);

    /// <summary>Creates a signal that emits a single value to each subscriber and completes, without scheduling.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>A signal that emits <paramref name="value"/> and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Emit<T>(T value) =>
        new ImmediateReturnSignal<T>(value);

    /// <summary>Returns the shared signal that emits the unit value and completes, without allocating.</summary>
    /// <param name="value">The unit value to emit.</param>
    /// <returns>A signal that emits <see cref="RxVoid.Default"/> and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<RxVoid> Emit(RxVoid value) =>
        ImmutableReturnRxVoidSignal.Instance;

    /// <summary>Returns one of the two shared Boolean signals that emit the value and complete, without allocating.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>A signal that emits <paramref name="value"/> and completes.</returns>
    public static IObservable<bool> Emit(bool value) =>
        value
            ? ImmutableReturnTrueSignal.Instance
            : ImmutableReturnFalseSignal.Instance;

    /// <summary>Creates a signal that emits a single 32-bit integer and completes, without scheduling.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>A signal that emits <paramref name="value"/> and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<int> Emit(int value) =>
        new ImmediateReturnSignal<int>(value);

    /// <summary>Returns the shared signal that emits the unit value and completes, without allocating.</summary>
    /// <returns>A signal that emits <see cref="RxVoid.Default"/> and completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<RxVoid> EmitRxVoid() =>
        ImmutableReturnRxVoidSignal.Instance;
}
