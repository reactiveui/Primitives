// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Immutable accumulator that counts the values matching a predicate as an <see cref="int"/>.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Result = {Result}, Predicate = {_predicate}")]
public readonly record struct CountPredicateAggregator<T> : IAggregator<T, int, CountPredicateAggregator<T>>
{
    /// <summary>The predicate selecting which values to count.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="CountPredicateAggregator{T}"/> struct.</summary>
    /// <param name="predicate">The predicate selecting which values to count.</param>
    public CountPredicateAggregator(Func<T, bool> predicate)
        : this(predicate, 0)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CountPredicateAggregator{T}"/> struct.</summary>
    /// <param name="predicate">The predicate selecting which values to count.</param>
    /// <param name="result">The current accumulated count.</param>
    private CountPredicateAggregator(Func<T, bool> predicate, int result)
    {
        _predicate = predicate;
        Result = result;
    }

    /// <inheritdoc/>
    public int Result { get; }

    /// <inheritdoc/>
    public CountPredicateAggregator<T> Add(T value) =>
        _predicate(value) ? new(_predicate, checked(Result + 1)) : this;
}
