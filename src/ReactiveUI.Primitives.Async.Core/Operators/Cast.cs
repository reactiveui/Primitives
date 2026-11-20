// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Type-casting operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Projects each element of the observable sequence to the specified result type by performing a runtime cast.</summary>
        /// <typeparam name="TResult">The type to which the elements of the sequence are cast.</typeparam>
        /// <returns>An observable sequence whose elements are the result of casting each element of the source sequence to
        /// <typeparamref name="TResult"/>.</returns>
        /// <remarks>A failed cast does not throw at the call site: the sequence completes with a failure carrying the
        /// cast exception.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> Cast<TResult>() =>
            new CastSignal<T, TResult>(source);
    }

    /// <summary>Type-casting operators for an untyped observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<object?> source)
    {
        /// <summary>Casts each value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to cast to.</typeparam>
        /// <returns>An observable sequence of values cast to <typeparamref name="TResult"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> CastTo<TResult>() =>
            new CastSignal<object?, TResult>(source);
    }
}
