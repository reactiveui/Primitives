// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Immutable accumulator that counts distinct selected keys as a <see cref="long"/>.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <typeparam name="TKey">The key type.</typeparam>
public readonly record struct DistinctByLongCountAggregator<T, TKey> : IAggregator<T, long, DistinctByLongCountAggregator<T, TKey>>
{
    /// <summary>The selector that projects each value to its distinctness key.</summary>
    private readonly Func<T, TKey> _keySelector;

    /// <summary>The set of keys that have already been observed.</summary>
    private readonly HashSet<TKey> _seen;

    /// <summary>Initializes a new instance of the <see cref="DistinctByLongCountAggregator{T,TKey}"/> struct.</summary>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key comparer, or <see langword="null"/> for the default comparer.</param>
    public DistinctByLongCountAggregator(Func<T, TKey> keySelector, IEqualityComparer<TKey>? comparer)
        : this(keySelector, comparer is null ? [] : new(comparer), 0L)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DistinctByLongCountAggregator{T,TKey}"/> struct.</summary>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="seen">The set of keys that have already been observed.</param>
    /// <param name="result">The current accumulated count.</param>
    private DistinctByLongCountAggregator(Func<T, TKey> keySelector, HashSet<TKey> seen, long result)
    {
        _keySelector = keySelector;
        _seen = seen;
        Result = result;
    }

    /// <inheritdoc/>
    public long Result { get; }

    /// <inheritdoc/>
    public DistinctByLongCountAggregator<T, TKey> Add(T value) =>
        _seen.Add(_keySelector(value)) ? new(_keySelector, _seen, checked(Result + 1L)) : this;
}
