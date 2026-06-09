// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Immutable accumulator that counts every observed value as a <see cref="long"/>.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal readonly record struct LongCountAggregator<T> : IAggregator<T, long, LongCountAggregator<T>>
{
    /// <summary>Initializes a new instance of the <see cref="LongCountAggregator{T}"/> struct.</summary>
    /// <param name="result">The current accumulated count.</param>
    private LongCountAggregator(long result) => Result = result;

    /// <inheritdoc/>
    public long Result { get; }

    /// <inheritdoc/>
    public LongCountAggregator<T> Add(T value) => new(checked(Result + 1L));
}
