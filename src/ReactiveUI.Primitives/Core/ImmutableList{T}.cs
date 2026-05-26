// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Immutable array-backed list optimized for copy-on-write observer storage.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
internal sealed class ImmutableList<T>
{
    /// <summary>
    /// Gets the shared empty list.
    /// </summary>
    public static readonly ImmutableList<T> Empty = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="ImmutableList{T}"/> class.
    /// </summary>
    /// <param name="data">Items owned by the immutable list.</param>
    public ImmutableList(T[] data) => Items = data;

    /// <summary>
    /// Initializes a new instance of the <see cref="ImmutableList{T}"/> class.
    /// </summary>
    private ImmutableList() => Items = [];

    /// <summary>
    /// Gets the immutable list items.
    /// </summary>
    public T[] Items { get; }

    /// <summary>
    /// Returns a new list with the value appended.
    /// </summary>
    /// <param name="value">Value to append.</param>
    /// <returns>A new immutable list containing the added value.</returns>
    public ImmutableList<T> Add(T value)
    {
        var newData = new T[Items.Length + 1];
        Array.Copy(Items, newData, Items.Length);
        newData[Items.Length] = value;
        return new ImmutableList<T>(newData);
    }

    /// <summary>
    /// Returns a new list with the first matching value removed.
    /// </summary>
    /// <param name="value">Value to remove.</param>
    /// <returns>A new immutable list without the value, or the current list when the value is absent.</returns>
    public ImmutableList<T> Remove(T value)
    {
        var i = IndexOf(value);
        if (i < 0)
        {
            return this;
        }

        var length = Items.Length;
        if (length == 1)
        {
            return Empty;
        }

        var newData = new T[length - 1];

        Array.Copy(Items, 0, newData, 0, i);
        Array.Copy(Items, i + 1, newData, i, length - i - 1);

        return new ImmutableList<T>(newData);
    }

    /// <summary>
    /// Finds the first matching value.
    /// </summary>
    /// <param name="value">Value to find.</param>
    /// <returns>The value index, or -1 when the value is absent.</returns>
    public int IndexOf(T value)
    {
        for (var i = 0; i < Items.Length; ++i)
        {
            if (Equals(Items[i], value))
            {
                return i;
            }
        }

        return -1;
    }
}
