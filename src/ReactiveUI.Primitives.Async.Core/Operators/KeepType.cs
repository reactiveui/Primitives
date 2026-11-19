// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Type-filtering operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Projects each element of the observable sequence to the specified reference type and filters out elements that are not of that type.</summary>
        /// <typeparam name="TResult">The reference type to filter and project elements to. Must be a class.</typeparam>
        /// <returns>An observable sequence containing only the elements of type TResult from the original sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> OfType<TResult>()
            where TResult : class =>
            new OfTypeSignal<T, TResult>(source);
    }

    /// <summary>Type-filtering operators for an untyped observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<object?> source)
    {
        /// <summary>Keeps values assignable to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to keep.</typeparam>
        /// <returns>An observable sequence of values assignable to <typeparamref name="TResult"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> KeepType<TResult>()
            where TResult : class =>
            new OfTypeSignal<object?, TResult>(source);
    }
}
