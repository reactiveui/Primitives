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
    /// <summary>Creates a signal that emits no values and completes on the supplied scheduler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="scheduler">The scheduler the completion is emitted on.</param>
    /// <returns>A signal that completes without emitting a value.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from the arguments.")]
    public static IObservable<T> None<T>(ISequencer scheduler) => scheduler.IsImmediate
        ? ImmutableEmptySignal<T>.Instance
        : new EmptySignal<T>(scheduler);

    /// <summary>Creates a signal that emits no values and completes on the supplied scheduler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="scheduler">The scheduler the completion is emitted on.</param>
    /// <param name="witness">An unobserved value whose type fixes <typeparamref name="T"/>.</param>
    /// <returns>A signal that completes without emitting a value.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The witness parameter only fixes the element type, so this overload builds the same signal.")]
    public static IObservable<T> None<T>(ISequencer scheduler, T witness) => scheduler.IsImmediate
        ? ImmutableEmptySignal<T>.Instance
        : new EmptySignal<T>(scheduler);

    /// <summary>Returns the shared signal that emits no values and completes on subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <returns>A signal that completes without emitting a value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from the arguments.")]
    public static IObservable<T> None<T>() =>
        ImmutableEmptySignal<T>.Instance;

    /// <summary>Returns the shared signal that emits no values and completes on subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="witness">An unobserved value whose type fixes <typeparamref name="T"/>.</param>
    /// <returns>A signal that completes without emitting a value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> None<T>(T witness) =>
        ImmutableEmptySignal<T>.Instance;
}
