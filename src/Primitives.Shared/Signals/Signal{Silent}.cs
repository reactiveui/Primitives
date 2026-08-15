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
    /// <summary>Non-Terminating Signals. It's no returns, never finish.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<T> Silent<T>() => ImmutableNeverSignal<T>.Instance;

    /// <summary>Non-Terminating Signals. It's no returns, never finish. witness is for type inference.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <returns>An Signals.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Silent<T>(T witness) => ImmutableNeverSignal<T>.Instance;
}
