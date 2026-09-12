// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Represents a value paired with a time interval, whose meaning is the producer's: how long the value took to
/// produce, the gap since the previous value, or its delivery time relative to a base.
/// </summary>
/// <typeparam name="T">The annotated value type.</typeparam>
[Serializable]
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly struct TimeInterval<T> : IEquatable<TimeInterval<T>>
{
    /// <summary>Initializes a new instance of the <see cref="TimeInterval{T}"/> struct.</summary>
    /// <param name="value">The value to annotate.</param>
    /// <param name="interval">The time interval associated with the value.</param>
    public TimeInterval(T value, TimeSpan interval)
    {
        Interval = interval;
        Value = value;
    }

    /// <summary>Gets the annotated value.</summary>
    public T Value { get; }

    /// <summary>Gets the time interval associated with the value.</summary>
    public TimeSpan Interval { get; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Compares two annotated values for equality.</summary>
    /// <param name="first">First value.</param>
    /// <param name="second">Second value.</param>
    /// <returns><c>true</c> when both values and intervals are equal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(TimeInterval<T> first, TimeInterval<T> second) =>
        first.Equals(second);

    /// <summary>Compares two annotated values for inequality.</summary>
    /// <param name="first">First value.</param>
    /// <param name="second">Second value.</param>
    /// <returns><c>true</c> when either value or interval differs.</returns>
    public static bool operator !=(TimeInterval<T> first, TimeInterval<T> second) =>
        !first.Equals(second);

    /// <inheritdoc/>
    public bool Equals(TimeInterval<T> other) =>
        other.Interval.Equals(Interval) && EqualityComparer<T>.Default.Equals(Value, other.Value);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is TimeInterval<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var valueHashCode = Value is null ? 1963 : EqualityComparer<T>.Default.GetHashCode(Value);

        return Interval.GetHashCode() ^ valueHashCode;
    }

    /// <inheritdoc/>
    public override string ToString() =>
#if NET8_0_OR_GREATER
        string.Format(CultureInfo.CurrentCulture, CoreCompositeFormats.TimeInterval, Value, Interval);
#else
        string.Format(CultureInfo.CurrentCulture, "{0}@{1}", Value, Interval);
#endif
}
