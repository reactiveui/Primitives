// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Immutable accumulator that counts every observed value as an <see cref="int"/>.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal readonly record struct CountAggregator<T> : IAggregator<T, int, CountAggregator<T>>
{
    private CountAggregator(int result) => Result = result;

    /// <inheritdoc/>
    public int Result { get; }

    /// <inheritdoc/>
    public CountAggregator<T> Add(T value) => new(checked(Result + 1));
}
