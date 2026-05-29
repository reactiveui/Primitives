// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents an optional value that may or may not be present.
/// </summary>
/// <remarks>Use this struct to indicate the presence or absence of a value without resorting to null references.
/// When an instance has a value, the HasValue property is <see langword="true"/> and the Value property returns the
/// contained value. If no value is present, HasValue is <see langword="false"/> and accessing Value throws an
/// exception. This pattern is useful for APIs that need to distinguish between an explicit 'no value' state and a
/// default value.</remarks>
/// <typeparam name="T">The type of the value that may be contained by the optional.</typeparam>
public readonly record struct Optional<T>
{
    /// <summary>
    /// The underlying value, or <see langword="default"/> when no value is present.
    /// </summary>
    private readonly T? _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="Optional{T}"/> struct.
    /// </summary>
    /// <remarks>After using this constructor, the HasValue property is set to false, indicating that the
    /// <see cref="Optional{T}"/>  instance does not contain a value.</remarks>
    public Optional() => (_value, HasValue) = (default, false);

    /// <summary>
    /// Initializes a new instance of the <see cref="Optional{T}"/> struct.
    /// </summary>
    /// <param name="value">The value to be contained in the <see cref="Optional{T}"/>  instance.</param>
    public Optional(T value) => (_value, HasValue) = (value, true);

    /// <summary>
    /// Gets an empty instance of the <see cref="Optional{T}"/> type that contains no value.
    /// </summary>
    /// <remarks>Use this property to represent the absence of a value in a type-safe manner. The returned
    /// instance has no value set and IsPresent is false.</remarks>
    public static Optional<T> Empty => new();

    /// <summary>
    /// Gets a value indicating whether the current instance has a valid value assigned.
    /// </summary>
    public bool HasValue { get; }

    /// <summary>
    /// Gets the value contained in the optional object.
    /// </summary>
    /// <remarks>Accessing this property when the optional object does not have a value will throw an
    /// exception. Use the HasValue property to determine whether a value is present before accessing this
    /// property.</remarks>
    public T? Value => HasValue
        ? _value
        : throw new InvalidOperationException("Impossible retrieve a value for an empty optional");
}
