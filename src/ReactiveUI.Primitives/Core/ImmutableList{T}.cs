// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class ImmutableList<T>
{
    public static readonly ImmutableList<T> Empty = new();

    public ImmutableList(T[] data) => Items = data;

    private ImmutableList() => Items = new T[0];

    public T[] Items { get; }

    public ImmutableList<T> Add(T value)
    {
        var newData = new T[Items.Length + 1];
        Array.Copy(Items, newData, Items.Length);
        newData[Items.Length] = value;
        return new ImmutableList<T>(newData);
    }

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
