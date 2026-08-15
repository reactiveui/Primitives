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
    /// <summary>Emit a single value on the specified scheduler.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Emit<T>(T value, ISequencer scheduler) => scheduler == Sequencer.Immediate
        ? new ImmediateReturnSignal<T>(value)
        : new ReturnSignal<T>(value, scheduler);

    /// <summary>Emit a single value immediately.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Emit<T>(T value) =>
        new ImmediateReturnSignal<T>(value);

    /// <summary>Emit a single RxVoid value immediately, optimized for no allocation.</summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<RxVoid> Emit(RxVoid value) =>
        ImmutableReturnRxVoidSignal.Instance;

    /// <summary>Emit a single Boolean value immediately, optimized for no allocation.</summary>
    /// <param name="value">if set to <c>true</c> [value].</param>
    /// <returns>An Signals.</returns>
    public static IObservable<bool> Emit(bool value) =>
        value
            ? ImmutableReturnTrueSignal.Instance
            : ImmutableReturnFalseSignal.Instance;

    /// <summary>Emit a single Int32 value immediately, optimized for cached values.</summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<int> Emit(int value) =>
        new ImmediateReturnSignal<int>(value);

    /// <summary>Same as Signals.Emit(RxVoid.Default); but no allocate memory.</summary>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<RxVoid> EmitRxVoid() =>
        ImmutableReturnRxVoidSignal.Instance;
}
