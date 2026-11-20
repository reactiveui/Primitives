// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for composing and managing asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Disposal-callback operators that run an action when the observable source subscription is disposed.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Registers a callback to be invoked asynchronously when the observable sequence is disposed.</summary>
        /// <param name="disposeAction">A function that returns a ValueTask representing the asynchronous operation to execute upon disposal of the
        /// observable sequence. Cannot be null.</param>
        /// <returns>An SignalAsync{T} that invokes the specified asynchronous callback when disposed.</returns>
        /// <remarks>The callback runs when the subscription is disposed, whether explicitly or through completion or
        /// error.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> OnDispose(Func<ValueTask> disposeAction) =>
            new OnDisposeSignal<T>(source, disposeAction);

        /// <summary>Registers an action to be invoked when the observable sequence is disposed.</summary>
        /// <param name="disposeAction">The action to execute when the subscription is disposed. Cannot be null.</param>
        /// <returns>An observable sequence that invokes the specified action upon disposal of the subscription.</returns>
        /// <remarks>The action runs synchronously during disposal; chained registrations run in the order they were
        /// added.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> OnDispose(Action disposeAction) =>
            new OnDisposeSignal<T>(source, disposeAction);
    }
}
