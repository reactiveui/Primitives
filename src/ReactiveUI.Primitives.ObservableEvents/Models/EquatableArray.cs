// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.ObservableEvents.Models;

/// <summary>An array that compares by value, so it can sit inside an incremental-pipeline model.</summary>
/// <typeparam name="T">The element type, which must itself compare by value.</typeparam>
/// <remarks>
/// The pipeline decides whether to re-run a downstream step by asking whether the model it produced equals the one
/// from the previous run. An array compares by reference, so a model carrying a bare array is never equal to its
/// predecessor and every step below it re-runs on every keystroke. Wrapping the array here is what makes the
/// per-target caching real. A readonly struct so the value sits inline in its owning record rather than adding a
/// heap object per collection.
/// </remarks>
internal readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>>
    where T : notnull, IEquatable<T>
{
    /// <summary>The seed of the deterministic hash-combine loop.</summary>
    private const int HashSeed = 17;

    /// <summary>The multiplier of the deterministic hash-combine loop.</summary>
    private const int HashMultiplier = 31;

    /// <summary>The wrapped elements, or null when default-constructed.</summary>
    private readonly T[]? _values;

    /// <summary>The hash computed once at construction, because the pipeline asks for it repeatedly.</summary>
    private readonly int _hashCode;

    /// <summary>Initializes a new instance of the <see cref="EquatableArray{T}"/> struct.</summary>
    /// <param name="values">The elements to wrap; ownership passes to this instance.</param>
    internal EquatableArray(T[] values)
    {
        _values = values;
        _hashCode = ComputeHashCode(values);
    }

    /// <summary>Gets an empty array.</summary>
    internal static EquatableArray<T> Empty => default;

    /// <summary>Gets a value indicating whether there are no elements.</summary>
    internal bool IsEmpty => _values is null || _values.Length == 0;

    /// <summary>Determines whether two arrays hold equal elements.</summary>
    /// <param name="left">The first array.</param>
    /// <param name="right">The second array.</param>
    /// <returns><see langword="true"/> when the arrays are element-wise equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(EquatableArray<T> left, EquatableArray<T> right) => left.Equals(right);

    /// <summary>Determines whether two arrays differ.</summary>
    /// <param name="left">The first array.</param>
    /// <param name="right">The second array.</param>
    /// <returns><see langword="true"/> when the arrays are not element-wise equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator !=(EquatableArray<T> left, EquatableArray<T> right) => !left.Equals(right);

    /// <inheritdoc/>
    /// <remarks>
    /// A defaulted instance and one wrapping a zero-length array are the same value here. Treating them as
    /// different would make an extraction that happened to build an empty array compare unequal to one that
    /// returned <see cref="Empty"/>, and silently cost the caching this type exists for.
    /// </remarks>
    public bool Equals(EquatableArray<T> other)
    {
        var values = _values;
        var otherValues = other._values;
        if (ReferenceEquals(values, otherValues))
        {
            return true;
        }

        var length = values?.Length ?? 0;
        if (length != (otherValues?.Length ?? 0))
        {
            return false;
        }

        for (var index = 0; index < length; index++)
        {
            if (!values![index].Equals(otherValues![index]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _hashCode;

    /// <summary>Gets the wrapped elements for iteration without allocating an enumerator.</summary>
    /// <returns>The backing array, or an empty array when default-constructed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal T[] AsArray() => _values ?? [];

    /// <summary>Computes the deterministic hash of the elements.</summary>
    /// <param name="values">The elements to hash.</param>
    /// <returns>The combined hash.</returns>
    /// <remarks>
    /// Empty hashes to zero, which is what a default-constructed instance keeps in its field without calling
    /// here - so the two forms of empty that <see cref="Equals(EquatableArray{T})"/> calls equal hash alike.
    /// </remarks>
    private static int ComputeHashCode(T[] values)
    {
        if (values.Length == 0)
        {
            return 0;
        }

        unchecked
        {
            var hash = HashSeed;
            for (var index = 0; index < values.Length; index++)
            {
                hash = (hash * HashMultiplier) + values[index].GetHashCode();
            }

            return hash;
        }
    }
}
