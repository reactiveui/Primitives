// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>SwitchTo/Switch operators for an observable source sequence of inner observable sequences.</summary>
    /// <typeparam name="T">The type of the elements in the inner observable sequences.</typeparam>
    /// <param name="source">The source observable sequence of observable sequences.</param>
    extension<T>(IObservableAsync<IObservableAsync<T>> source)
    {
        /// <summary>
        /// Transforms an observable sequence of observable sequences into a single observable sequence that emits
        /// values from the most recent inner observable sequence.
        /// </summary>
        /// <returns>An observable sequence that emits items from the most recently emitted inner observable sequence. When a new
        /// inner sequence is emitted, the previous one is unsubscribed.</returns>
        /// <remarks>This operator is commonly used to switch to a new data stream whenever a new inner
        /// observable is produced, unsubscribing from the previous inner observable. Only items from the latest inner
        /// observable are emitted to subscribers.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> SwitchTo() => new SwitchToSignal<T>(source);

        /// <summary>
        /// Transforms an observable sequence of observable sequences into a single observable sequence that emits
        /// values from the most recent inner observable sequence.
        /// </summary>
        /// <returns>An observable sequence that emits items from the most recently emitted inner observable sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Switch() => new SwitchToSignal<T>(source);
    }
}
