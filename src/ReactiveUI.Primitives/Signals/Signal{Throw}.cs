// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
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
    /// Empty Signals. Returns only onError on specified scheduler.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
#pragma warning disable S4018 // Result type is intentionally explicit for Rx-style factory APIs.
    public static IObservable<T> Throw<T>(Exception error, ISequencer scheduler) =>
        new ThrowSignal<T>(error, scheduler);

    /// <summary>
    /// Empty Signals. Returns only onError.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Throw<T>(Exception error) =>
        Throw<T>(error, Sequencer.Immediate);
#pragma warning restore S4018

    /// <summary>
    /// Empty Signals. Returns only onError. witness if for Type inference.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
#pragma warning disable RCS1163 // Unused parameter.
    public static IObservable<T> Throw<T>(Exception error, T witness) =>
        Throw<T>(error, Sequencer.Immediate);

    /// <summary>
    /// Empty Signals. Returns only onError on specified scheduler. witness if for Type inference.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Throw<T>(Exception error, ISequencer scheduler, T witness) =>
        Throw<T>(error, scheduler);
#pragma warning restore RCS1163 // Unused parameter.
}
