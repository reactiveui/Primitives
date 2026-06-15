// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Shared delegate handlers.</summary>
internal static class Handle
{
    /// <summary>Action that does nothing.</summary>
    public static readonly Action Nop = () => { };

    /// <summary>Error handler that throws the supplied exception.</summary>
    public static readonly Action<Exception> Throw = ex => ex.Throw();

    /// <summary>Converts an error into an empty observable sequence.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="ex">Ignored exception.</param>
    /// <returns>An empty sequence.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "The type parameter defines the element type for this Rx-style factory and cannot be inferred from the arguments.")]
    public static IObservable<TSource> CatchIgnore<TSource>(Exception ex) =>
        Signal.None<TSource>();
}
