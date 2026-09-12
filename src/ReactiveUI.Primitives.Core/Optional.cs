// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives;

/// <summary>Represents a value that may be absent, treating null as absent.</summary>
/// <typeparam name="T">The type of the value that may be contained by the optional.</typeparam>
[System.Diagnostics.DebuggerDisplay("Optional: HasValue = {HasValue}, Value = {_value}")]
public readonly record struct Optional<T>
{
    /// <summary>The underlying value, or <see langword="default"/> when no value is present.</summary>
    private readonly T? _value;

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct with no value.</summary>
    public Optional() => (_value, HasValue) = (default, false);

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct.</summary>
    /// <param name="value">The value to be contained in the <see cref="Optional{T}"/> instance.</param>
    public Optional([AllowNull] T value) => (_value, HasValue) = value is null ? (default, false) : (value, true);

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct.</summary>
    /// <param name="value">The value to contain, treated as absent when <see langword="null"/>.</param>
    /// <param name="hasValue">A value indicating whether a value is present.</param>
    private Optional([AllowNull] T value, bool hasValue) =>
        (_value, HasValue) = hasValue && value is not null ? (value, true) : (default, false);

    /// <summary>Gets an empty instance of the <see cref="Optional{T}"/> type that contains no value.</summary>
    public static Optional<T> Empty => new();

    /// <summary>Gets an empty optional value.</summary>
    public static Optional<T> None => default;

    /// <summary>Gets a value indicating whether the current instance has a valid value assigned.</summary>
    public bool HasValue { get; }

    /// <summary>Gets the value contained in the optional object.</summary>
    /// <exception cref="InvalidOperationException"><see cref="HasValue"/> is <see langword="false"/>.</exception>
    [NotNull]
    public T? Value => HasValue
        ? _value!
        : throw new InvalidOperationException("Impossible retrieve a value for an empty optional");

    /// <summary>Creates an optional value containing a value.</summary>
    /// <param name="value">The contained value.</param>
    /// <returns>The optional value.</returns>
    public static Optional<T> Some([AllowNull] T value) => new(value, true);

    /// <summary>Implicit cast from the value to the optional.</summary>
    /// <param name="value">The value to wrap, treated as absent when <see langword="null"/>.</param>
    /// <returns>The optional value.</returns>
    public static implicit operator Optional<T>([AllowNull] T value) => ToOptional(value);

    /// <summary>Explicit cast from the optional to the value.</summary>
    /// <param name="value">The optional value.</param>
    /// <returns>The contained value.</returns>
    public static explicit operator T?(in Optional<T> value) => FromOptional(value);

    /// <summary>Creates an optional value, treating a <see langword="null"/> value as absent.</summary>
    /// <param name="value">The value to wrap.</param>
    /// <returns>The optional value.</returns>
    public static Optional<T> Create([AllowNull] T value) => new(value);

    /// <summary>Gets the value from the optional value.</summary>
    /// <param name="value">The optional value to unwrap.</param>
    /// <returns>The contained value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? FromOptional(in Optional<T> value) => value.Value;

    /// <summary>Gets the optional from a value.</summary>
    /// <param name="value">The value to get the optional for.</param>
    /// <returns>The optional.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Optional<T> ToOptional([AllowNull] T value) => Create(value);

    /// <inheritdoc />
    public override string? ToString() => _value is null || !HasValue ? "<None>" : _value.ToString();
}
