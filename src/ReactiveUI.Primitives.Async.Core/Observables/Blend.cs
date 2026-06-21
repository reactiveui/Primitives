// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for composing asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Merges the supplied sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that merges the sources.</returns>
    public static IObservableAsync<T> Blend<T>(params IObservableAsync<T>[] sources) =>
        new SignalAsyncExtensions.BlendEnumerableSignal<T>(sources);
}
