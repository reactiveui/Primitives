// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Signals.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Return single sequence on specified scheduler.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Return<T>(T value, ISequencer scheduler)
    {
        if (scheduler == Sequencer.Immediate)
        {
            return new ImmediateReturnSignal<T>(value);
        }

        return new ReturnSignal<T>(value, scheduler);
    }

    /// <summary>
    /// Return single sequence Immediately.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Return<T>(T value) =>
        Return<T>(value, Sequencer.Immediate);

    /// <summary>
    /// Return single sequence Immediately, optimized for RxVoid(no allocate memory).
    /// </summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
#pragma warning disable RCS1163 // Unused parameter.
    public static IObservable<RxVoid> Return(RxVoid value) =>
        ImmutableReturnRxVoidSignal.Instance;
#pragma warning restore RCS1163 // Unused parameter.

    /// <summary>
    /// Return single sequence Immediately, optimized for Boolean(no allocate memory).
    /// </summary>
    /// <param name="value">if set to <c>true</c> [value].</param>
    /// <returns>An Signals.</returns>
    public static IObservable<bool> Return(bool value) => value
            ? ImmutableReturnTrueSignal.Instance
            : ImmutableReturnFalseSignal.Instance;

    /// <summary>
    /// Return single sequence Immediately, optimized for Int32.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<int> Return(int value) =>
        ImmutableReturnInt32Signal.GetInt32Signals(value);

    /// <summary>
    /// Same as Signals.Return(RxVoid.Default); but no allocate memory.
    /// </summary>
    /// <returns>An Signals.</returns>
    public static IObservable<RxVoid> ReturnRxVoid() =>
        ImmutableReturnRxVoidSignal.Instance;
}
