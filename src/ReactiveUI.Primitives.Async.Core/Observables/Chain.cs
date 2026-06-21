// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for composing asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Concatenates the supplied sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>An observable sequence that concatenates the sources.</returns>
    public static IObservableAsync<T> Chain<T>(params IObservableAsync<T>[] sources) =>
        new ChainEnumerableSignal<T>(sources);
}
