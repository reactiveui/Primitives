// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Provides static factory and operator methods for signals.</summary>
public static partial class Signal
{
    /// <summary>Empty Signals. Returns only OnCompleted on specified scheduler.</summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> None<T>(ISequencer scheduler) => scheduler == Sequencer.Immediate
        ? ImmutableEmptySignal<T>.Instance
        : new EmptySignal<T>(scheduler);

    /// <summary>Empty Signals. Returns only OnCompleted on specified scheduler. witness is for type inference.</summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "The witness parameter exists only so callers can let T be inferred; it is unused, so the body "
            + "intentionally mirrors the scheduler None overload. They are distinct Rx-parity overloads that build "
            + "the signal directly rather than forwarding.")]
    public static IObservable<T> None<T>(ISequencer scheduler, T witness) => scheduler == Sequencer.Immediate
        ? ImmutableEmptySignal<T>.Instance
        : new EmptySignal<T>(scheduler);

    /// <summary>Empty Signals. Returns only OnCompleted.</summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> None<T>() =>
        ImmutableEmptySignal<T>.Instance;

    /// <summary>Empty Signals. Returns only OnCompleted. witness is for type inference.</summary>
    /// <typeparam name="T">The Type.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    public static IObservable<T> None<T>(T witness) =>
        ImmutableEmptySignal<T>.Instance;
}
