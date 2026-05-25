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
    /// Empty Signals. Returns only OnCompleted on specified scheduler.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Empty<T>(ISequencer scheduler)
    {
        if (scheduler == Sequencer.Immediate)
        {
            return ImmutableEmptySignal<T>.Instance;
        }

        return new EmptySignal<T>(scheduler);
    }

    /// <summary>
    /// Empty Signals. Returns only OnCompleted on specified scheduler. witness is for type inference.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
#pragma warning disable RCS1163 // Unused parameter.
    public static IObservable<T> Empty<T>(ISequencer scheduler, T witness) =>
        Empty<T>(scheduler);
#pragma warning restore RCS1163 // Unused parameter.

    /// <summary>
    /// Empty Signals. Returns only OnCompleted.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <returns>An Signals.</returns>
    public static IObservable<T> Empty<T>() =>
        Empty<T>(Sequencer.Immediate);

    /// <summary>
    /// Empty Signals. Returns only OnCompleted. witness is for type inference.
    /// </summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
#pragma warning disable RCS1163 // Unused parameter.
    public static IObservable<T> Empty<T>(T witness) =>
        Empty<T>(Sequencer.Immediate);
#pragma warning restore RCS1163 // Unused parameter.
}
