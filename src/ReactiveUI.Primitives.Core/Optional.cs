// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives;

/// <summary>Represents an optional value that may or may not be present.</summary>
/// <remarks>Use this struct to indicate the presence or absence of a value without resorting to null references.
/// When an instance has a value, the HasValue property is <see langword="true"/> and the Value property returns the
/// contained value. If no value is present, HasValue is <see langword="false"/> and accessing Value throws an
/// exception. This pattern is useful for APIs that need to distinguish between an explicit 'no value' state and a
/// default value.</remarks>
/// <typeparam name="T">The type of the value that may be contained by the optional.</typeparam>
[System.Diagnostics.DebuggerDisplay("HasValue = {HasValue}, Value = {_value}")]
public readonly record struct Optional<T>
{
    /// <summary>The underlying value, or <see langword="default"/> when no value is present.</summary>
    private readonly T? _value;

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct.</summary>
    /// <remarks>After using this constructor, the HasValue property is set to false, indicating that the
    /// <see cref="Optional{T}"/>  instance does not contain a value.</remarks>
    public Optional() => (_value, HasValue) = (default, false);

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct.</summary>
    /// <param name="value">The value to be contained in the <see cref="Optional{T}"/>  instance.</param>
    public Optional([AllowNull] T value) => (_value, HasValue) = value is null ? (default, false) : (value, true);

    /// <summary>Initializes a new instance of the <see cref="Optional{T}"/> struct.</summary>
    /// <param name="value">The value.</param>
    /// <param name="hasValue">A value indicating whether a value is present.</param>
    private Optional([AllowNull] T value, bool hasValue) => (_value, HasValue) = hasValue && value is not null ? (value, true) : (default, false);

    /// <summary>Gets an empty instance of the <see cref="Optional{T}"/> type that contains no value.</summary>
    /// <remarks>Use this property to represent the absence of a value in a type-safe manner. The returned
    /// instance has no value set and IsPresent is false.</remarks>
    public static Optional<T> Empty => new();

    /// <summary>Gets an empty optional value.</summary>
    public static Optional<T> None => default;

    /// <summary>Gets a value indicating whether the current instance has a valid value assigned.</summary>
    public bool HasValue { get; }

    /// <summary>Gets the value contained in the optional object.</summary>
    /// <remarks>Accessing this property when the optional object does not have a value will throw an
    /// exception. Use the HasValue property to determine whether a value is present before accessing this
    /// property.</remarks>
    [NotNull]
    public T? Value => HasValue
        ? _value!
        : throw new InvalidOperationException("Impossible retrieve a value for an empty optional");

    /// <summary>Creates an optional value containing a value.</summary>
    /// <param name="value">The contained value.</param>
    /// <returns>The optional value.</returns>
    public static Optional<T> Some([AllowNull] T value) => new(value, hasValue: true);

    /// <summary>Implicit cast from the value to the optional.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The optional value.</returns>
    public static implicit operator Optional<T>([AllowNull] T value) => ToOptional(value);

    /// <summary>Explicit cast from option to value.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The optional value.</returns>
    public static explicit operator T?(in Optional<T> value) => FromOptional(value);

    /// <summary>Creates the specified value.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The optional value.</returns>
    public static Optional<T> Create([AllowNull] T value) => new(value);

    /// <summary>Gets the value from the optional value.</summary>
    /// <param name="value">The optional value.</param>
    /// <returns>The value.</returns>
    public static T? FromOptional(in Optional<T> value) => value.Value;

    /// <summary>Gets the optional from a value.</summary>
    /// <param name="value">The value to get the optional for.</param>
    /// <returns>The optional.</returns>
    public static Optional<T> ToOptional([AllowNull] T value) => new(value);

    /// <inheritdoc />
    public override string? ToString()
    {
        if (_value is null)
        {
            return "<None>";
        }

        return !HasValue ? "<None>" : _value.ToString();
    }
}
