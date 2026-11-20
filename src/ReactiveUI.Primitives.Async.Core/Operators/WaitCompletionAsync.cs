// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous completion-await operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The observable sequence to wait for completion.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously waits for the observable sequence to complete without retrieving any values.</summary>
        /// <returns>A ValueTask that represents the asynchronous wait operation.</returns>
        /// <remarks>The source is subscribed once and its values are ignored; the returned task completes when the
        /// sequence completes and faults with the error when it fails.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask WaitCompletionAsync() =>
            CompletionTaskWitness<T>.RunAsync(source);

        /// <summary>Asynchronously waits for the observable sequence to complete without retrieving any values.</summary>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the wait operation.</param>
        /// <returns>A ValueTask that represents the asynchronous wait operation.</returns>
        /// <remarks>The source is subscribed once and its values are ignored; the returned task completes when the
        /// sequence completes and faults with the error when it fails.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask WaitCompletionAsync(CancellationToken cancellationToken) =>
            CompletionTaskWitness<T>.RunAsync(source, cancellationToken);
    }
}
