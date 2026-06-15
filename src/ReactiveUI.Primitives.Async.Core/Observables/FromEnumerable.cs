// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for converting enumerable sources into asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates a source from an enumerable sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The enumerable to convert.</param>
    /// <returns>An observable sequence emitting the enumerable's values.</returns>
    public static IObservableAsync<T> FromEnumerable<T>(IEnumerable<T> values) => values.ToAsyncSignal();

    /// <summary>Creates a source from an async enumerable sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The async enumerable to convert.</param>
    /// <returns>An observable sequence emitting the async enumerable's values.</returns>
    public static IObservableAsync<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values) => values.ToAsyncSignal();
}
