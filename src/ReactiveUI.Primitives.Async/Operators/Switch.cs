// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
public static partial class ObservableAsync
{
    /// <summary>
    /// Transforms an observable sequence of observable sequences into a single observable sequence that emits
    /// values from the most recent inner observable sequence.
    /// </summary>
    /// <remarks>This operator is commonly used to switch to a new data stream whenever a new inner
    /// observable is produced, unsubscribing from the previous inner observable. Only items from the latest inner
    /// observable are emitted to subscribers.</remarks>
    /// <typeparam name="T">The type of the elements in the inner observable sequences.</typeparam>
    /// <param name="this">The source observable sequence of observable sequences.</param>
    /// <returns>An observable sequence that emits items from the most recently emitted inner observable sequence. When a new
    /// inner sequence is emitted, the previous one is unsubscribed.</returns>
    public static IObservableAsync<T> Switch<T>(this IObservableAsync<IObservableAsync<T>> @this) => new SwitchObservable<T>(@this);
}
