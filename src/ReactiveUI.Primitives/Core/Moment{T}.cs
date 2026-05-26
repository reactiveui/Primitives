// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Represents a value captured at a scheduler timestamp.
/// </summary>
/// <typeparam name="T">The captured value type.</typeparam>
[Serializable]
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly partial struct Moment<T> : IEquatable<Moment<T>>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Moment{T}"/> struct.
    /// </summary>
    /// <param name="value">The captured value.</param>
    /// <param name="timestamp">The scheduler timestamp.</param>
    public Moment(T value, DateTimeOffset timestamp)
    {
        Value = value;
        Timestamp = timestamp;
    }

    /// <summary>
    /// Gets the captured value.
    /// </summary>
    public T Value { get; }

    /// <summary>
    /// Gets the scheduler timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; }

    /// <summary>
    /// Compares two timestamped values for equality.
    /// </summary>
    /// <param name="first">First value.</param>
    /// <param name="second">Second value.</param>
    /// <returns><c>true</c> when both values and timestamps are equal.</returns>
    public static bool operator ==(Moment<T> first, Moment<T> second) => first.Equals(second);

    /// <summary>
    /// Compares two timestamped values for inequality.
    /// </summary>
    /// <param name="first">First value.</param>
    /// <param name="second">Second value.</param>
    /// <returns><c>true</c> when either value or timestamp differs.</returns>
    public static bool operator !=(Moment<T> first, Moment<T> second) => !first.Equals(second);

    /// <inheritdoc/>
    public bool Equals(Moment<T> other) => Timestamp.Equals(other.Timestamp) && EqualityComparer<T>.Default.Equals(Value, other.Value);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Moment<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var valueHashCode = Value is null ? 1963 : EqualityComparer<T>.Default.GetHashCode(Value);
        return Timestamp.GetHashCode() ^ valueHashCode;
    }

    /// <inheritdoc/>
    public override string ToString() => string.Format(CultureInfo.CurrentCulture, "{0}@{1:o}", Value, Timestamp);
}
