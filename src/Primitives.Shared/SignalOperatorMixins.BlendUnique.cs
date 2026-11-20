// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Concurrently merges sources and suppresses values equal to the last forwarded value.</summary>
public static partial class LinqExtensions
{
    /// <summary>Concurrently merges sources and suppresses adjacent duplicate values using the default equality comparer.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    /// <remarks>The first source error terminates the result; successful completion waits for every source.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> BlendUnique<T>(params IObservable<T>[] sources) =>
        BlendUniqueSignal<T>.Create(sources);

    /// <summary>
    /// Concurrently merges the supplied sources and forwards only values that differ from the last forwarded
    /// value, using the supplied comparer (or the default when <see langword="null"/>).
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="comparer">The equality comparer used to suppress duplicates, or <see langword="null"/> for the default.</param>
    /// <returns>An observable of the distinct merged values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> BlendUnique<T>(IObservable<T>[] sources, IEqualityComparer<T>? comparer) =>
        BlendUniqueSignal<T>.Create(sources, comparer);
}
