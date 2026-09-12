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
    /// <summary>Creates a signal that emits nothing and never terminates.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <returns>A signal that produces no notifications.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "The element type cannot be inferred from the arguments.")]
    public static IObservable<T> Silent<T>() => ImmutableNeverSignal<T>.Instance;

    /// <summary>Creates a signal that emits nothing and never terminates.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="witness">An unobserved value whose type fixes <typeparamref name="T"/>.</param>
    /// <returns>A signal that produces no notifications.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Silent<T>(T witness) => ImmutableNeverSignal<T>.Instance;
}
