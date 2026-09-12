// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// An immutable value-type accumulator for a single-source aggregate sink: each <see cref="Add"/> folds a value
/// into the running state and returns the next accumulator, while <see cref="Result"/> yields the terminal value.
/// Implement it as a <see langword="readonly record struct"/> so
/// <see cref="AggregateWitness{T,TResult,TAggregator}"/> can fold without allocating per value.
/// </summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <typeparam name="TResult">The terminal result type.</typeparam>
/// <typeparam name="TSelf">The implementing accumulator type, returned by <see cref="Add"/>.</typeparam>
public interface IAggregator<in T, out TResult, TSelf>
    where TSelf : IAggregator<T, TResult, TSelf>
{
    /// <summary>Gets the terminal result computed from the values folded so far.</summary>
    TResult Result { get; }

    /// <summary>Folds <paramref name="value"/> into the running state and returns the next accumulator.</summary>
    /// <param name="value">The observed value.</param>
    /// <returns>The accumulator reflecting <paramref name="value"/>.</returns>
    TSelf Add(T value);
}
