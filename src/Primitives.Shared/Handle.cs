// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Shared delegate handlers.</summary>
internal static class Handle
{
    /// <summary>Action that does nothing.</summary>
    internal static readonly Action Nop = static () => { };

    /// <summary>Error handler that throws the supplied exception.</summary>
    internal static readonly Action<Exception> Throw = static ex => ex.Throw();

    /// <summary>Converts an error into an empty observable sequence.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="ex">Ignored exception.</param>
    /// <returns>An empty sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification =
            "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    internal static IObservable<TSource> CatchIgnore<TSource>(Exception ex) =>
        Signal.None<TSource>();
}
