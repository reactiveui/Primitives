// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Immutable accumulator that counts the values matching a predicate as a <see cref="long"/>.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
public readonly record struct LongCountPredicateAggregator<T> : IAggregator<T, long, LongCountPredicateAggregator<T>>
{
    /// <summary>The predicate selecting which values to count.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="LongCountPredicateAggregator{T}"/> struct.</summary>
    /// <param name="predicate">The predicate selecting which values to count.</param>
    public LongCountPredicateAggregator(Func<T, bool> predicate)
        : this(predicate, 0L)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LongCountPredicateAggregator{T}"/> struct.</summary>
    /// <param name="predicate">The predicate selecting which values to count.</param>
    /// <param name="result">The current accumulated count.</param>
    private LongCountPredicateAggregator(Func<T, bool> predicate, long result)
    {
        _predicate = predicate;
        Result = result;
    }

    /// <inheritdoc/>
    public long Result { get; }

    /// <inheritdoc/>
    public LongCountPredicateAggregator<T> Add(T value) =>
        _predicate(value) ? new(_predicate, checked(Result + 1L)) : this;
}
