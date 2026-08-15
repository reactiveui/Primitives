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
    /// <summary>Empty Signals. Returns only onError on specified scheduler.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Fail<T>(Exception error, ISequencer scheduler) => scheduler == Sequencer.Immediate
        ? new ImmediateThrowSignal<T>(error)
        : new ThrowSignal<T>(error, scheduler);

    /// <summary>Empty Signals. Returns only onError.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Fail<T>(Exception error) =>
        new ImmediateThrowSignal<T>(error);

    /// <summary>Empty Signals. Returns only onError. witness if for Type inference.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "The witness parameter exists only so callers can let T be inferred; it is unused, so the body "
            + "intentionally mirrors the witness-less Fail overload. They are distinct Rx-parity overloads that build "
            + "the signal directly rather than forwarding.")]
    public static IObservable<T> Fail<T>(Exception error, T witness) =>
        new ImmediateThrowSignal<T>(error);

    /// <summary>Empty Signals. Returns only onError on specified scheduler. witness if for Type inference.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="error">The error.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "The witness parameter exists only so callers can let T be inferred; it is unused, so the body "
            + "intentionally mirrors the scheduler Fail overload. They are distinct Rx-parity overloads that build "
            + "the signal directly rather than forwarding.")]
    public static IObservable<T> Fail<T>(Exception error, ISequencer scheduler, T witness) =>
        scheduler == Sequencer.Immediate
            ? new ImmediateThrowSignal<T>(error)
            : new ThrowSignal<T>(error, scheduler);
}
