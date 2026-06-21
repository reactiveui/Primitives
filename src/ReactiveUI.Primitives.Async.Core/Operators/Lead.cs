// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers utility methods that enable manipulation and composition of
/// asynchronous observables, such as prepending values to a sequence. These methods facilitate common operations when
/// building reactive, asynchronous workflows.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Prepend (value-prepending) operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Returns a new observable sequence that begins with the specified value, followed by the elements of the
        /// current sequence.
        /// </summary>
        /// <param name="value">The value to prepend to the beginning of the sequence.</param>
        /// <returns>An observable sequence with the specified value prepended to the original sequence.</returns>
        public IObservableAsync<T> Lead(T value) => new LeadSignal<T>(@this, [value]);

        /// <summary>
        /// Returns a new observable sequence that begins with the specified value, followed by the elements of the
        /// current sequence.
        /// </summary>
        /// <param name="value">The value to prepend to the beginning of the sequence.</param>
        /// <returns>An observable sequence with the specified value prepended to the original sequence.</returns>
        public IObservableAsync<T> Prepend(T value) => new LeadSignal<T>(@this, [value]);

        /// <summary>Returns a new observable sequence that emits the specified values before the emissions from the current sequence.</summary>
        /// <remarks>The values in the provided collection are emitted in order before any items from the
        /// original sequence. If the sequence is unsubscribed before completion, remaining values may not be
        /// emitted.</remarks>
        /// <param name="values">The collection of values to emit before the original sequence. Cannot be null.</param>
        /// <returns>An observable sequence that emits the specified values first, followed by the items from the current
        /// sequence.</returns>
        public IObservableAsync<T> Prepend(IEnumerable<T> values) => new LeadSignal<T>(@this, values);
    }
}
