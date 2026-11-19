// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Error-handling operators that convert source errors into failure completion results for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The source asynchronous observable sequence to monitor for errors.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Creates a new observable sequence that converts any error encountered in the source sequence into a failure
        /// result, allowing the sequence to complete without propagating exceptions.
        /// </summary>
        /// <returns>An observable sequence that emits the same elements as the source, but represents errors as failure results
        /// instead of throwing exceptions.</returns>
        /// <remarks>The completion result carries the error, so an observer inspects that result to tell success from
        /// failure.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> OnErrorResumeAsFailure() =>
            new OnErrorResumeAsFailureSignal<T>(source);
    }
}
