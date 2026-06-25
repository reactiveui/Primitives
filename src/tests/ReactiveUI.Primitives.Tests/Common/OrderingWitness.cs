// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// An observer that watches a monotonically increasing stream and flags the first delivery that breaks the
/// ordering contract: a value that is not strictly greater than the one before it. A repeat of the previous
/// value (a duplicate) or a smaller value (a reorder) both trip the flag.
/// </summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class OrderingWitness<T> : IObserver<T>
    where T : IComparable<T>
{
    /// <summary>The most recently observed value, or <see langword="default"/> before the first value.</summary>
    private T? _previous;

    /// <summary>Whether at least one value has been observed.</summary>
    private bool _hasPrevious;

    /// <summary>Gets the first delivery that broke ordering, or <see langword="null"/> when none did.</summary>
    public OutOfOrderDelivery? OutOfOrder { get; private set; }

    /// <summary>Records a completion callback.</summary>
    public void OnCompleted()
    {
    }

    /// <summary>Records an error callback.</summary>
    /// <param name="error">The error to record.</param>
    public void OnError(Exception error)
    {
    }

    /// <summary>Records a value callback and flags the first out-of-order or duplicate delivery.</summary>
    /// <param name="value">The value to record.</param>
    public void OnNext(T value)
    {
        if (_hasPrevious && OutOfOrder is null && value.CompareTo(_previous!) <= 0)
        {
            OutOfOrder = new(_previous!.ToString(), value!.ToString());
        }

        _previous = value;
        _hasPrevious = true;
    }

    /// <summary>The previous and offending values for the first delivery that broke ordering.</summary>
    /// <param name="Previous">The value observed immediately before the offending one.</param>
    /// <param name="Offending">The value that was not strictly greater than <paramref name="Previous"/>.</param>
    internal sealed record OutOfOrderDelivery(string? Previous, string? Offending);
}
