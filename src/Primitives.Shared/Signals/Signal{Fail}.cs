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
    /// <summary>Creates a signal that emits no values and fails with the supplied error on the supplied scheduler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error the signal terminates with.</param>
    /// <param name="scheduler">The scheduler the error is emitted on.</param>
    /// <returns>A signal that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from the arguments.")]
    public static IObservable<T> Fail<T>(Exception error, ISequencer scheduler) => scheduler.IsImmediate
        ? new ImmediateThrowSignal<T>(error)
        : new ThrowSignal<T>(error, scheduler);

    /// <summary>Creates a signal that emits no values and fails with the supplied error on subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error the signal terminates with.</param>
    /// <returns>A signal that terminates with <paramref name="error"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from the arguments.")]
    public static IObservable<T> Fail<T>(Exception error) =>
        new ImmediateThrowSignal<T>(error);

    /// <summary>Creates a signal that emits no values and fails with the supplied error on subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error the signal terminates with.</param>
    /// <param name="witness">An unobserved value whose type fixes <typeparamref name="T"/>.</param>
    /// <returns>A signal that terminates with <paramref name="error"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The witness parameter only fixes the element type, so this overload builds the same signal.")]
    public static IObservable<T> Fail<T>(Exception error, T witness) =>
        new ImmediateThrowSignal<T>(error);

    /// <summary>Creates a signal that emits no values and fails with the supplied error on the supplied scheduler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error the signal terminates with.</param>
    /// <param name="scheduler">The scheduler the error is emitted on.</param>
    /// <param name="witness">An unobserved value whose type fixes <typeparamref name="T"/>.</param>
    /// <returns>A signal that terminates with <paramref name="error"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The witness parameter only fixes the element type, so this overload builds the same signal.")]
    public static IObservable<T> Fail<T>(Exception error, ISequencer scheduler, T witness) =>
        scheduler.IsImmediate
            ? new ImmediateThrowSignal<T>(error)
            : new ThrowSignal<T>(error, scheduler);
}
