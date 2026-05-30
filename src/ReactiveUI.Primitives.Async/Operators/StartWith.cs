// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the StartWith extension method for asynchronous observable sequences.
/// </summary>
/// <remarks>StartWith mirrors the System.Reactive naming convention and prepends one or more values
/// to the beginning of an observable sequence before its own emissions.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Prepends the specified value to the beginning of the observable sequence.
    /// </summary>
    /// <remarks>This is equivalent to Prepend(T) and follows the System.Reactive
    /// naming convention.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="value">The value to prepend to the sequence.</param>
    /// <returns>An observable sequence that emits the specified value first, followed by the elements
    /// of the source sequence.</returns>
    public static IObservableAsync<T> StartWith<T>(this IObservableAsync<T> @this, T value) => @this.Prepend(value);

    /// <summary>
    /// Prepends the specified values to the beginning of the observable sequence.
    /// </summary>
    /// <remarks>This is equivalent to Prepend(IEnumerable{T}) and follows the System.Reactive
    /// naming convention. Values are emitted in the order they appear in the collection.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="values">The values to prepend to the sequence. Cannot be null.</param>
    /// <returns>An observable sequence that emits the specified values first, followed by the elements
    /// of the source sequence.</returns>
    public static IObservableAsync<T> StartWith<T>(this IObservableAsync<T> @this, IEnumerable<T> values) => @this.Prepend(values);

    /// <summary>
    /// Prepends the specified values to the beginning of the observable sequence.
    /// </summary>
    /// <remarks>This overload accepts a params array for convenience. Values are emitted in the
    /// order they appear in the array.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <param name="values">The values to prepend to the sequence.</param>
    /// <returns>An observable sequence that emits the specified values first, followed by the elements
    /// of the source sequence.</returns>
    public static IObservableAsync<T> StartWith<T>(this IObservableAsync<T> @this, params T[] values) => @this.Prepend(values);
}
