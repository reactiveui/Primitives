// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Emit a single value on the specified scheduler.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Emit<T>(T value, ISequencer scheduler)
    {
        if (scheduler == Sequencer.Immediate)
        {
            return new ImmediateReturnSignal<T>(value);
        }

        return new ReturnSignal<T>(value, scheduler);
    }

    /// <summary>Emit a single value immediately.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Emit<T>(T value) =>
        Emit(value, Sequencer.Immediate);

    /// <summary>Emit a single RxVoid value immediately, optimized for no allocation.</summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<RxVoid> Emit(RxVoid value) =>
        ImmutableReturnRxVoidSignal.Instance;

    /// <summary>Emit a single Boolean value immediately, optimized for no allocation.</summary>
    /// <param name="value">if set to <c>true</c> [value].</param>
    /// <returns>An Signals.</returns>
    public static IObservable<bool> Emit(bool value) => value
            ? ImmutableReturnTrueSignal.Instance
            : ImmutableReturnFalseSignal.Instance;

    /// <summary>Emit a single Int32 value immediately, optimized for cached values.</summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<int> Emit(int value) =>
        ImmutableReturnInt32Signal.GetInt32Signals(value);

    /// <summary>Same as Signals.Emit(RxVoid.Default); but no allocate memory.</summary>
    /// <returns>An Signals.</returns>
    public static IObservable<RxVoid> EmitRxVoid() =>
        ImmutableReturnRxVoidSignal.Instance;
}
