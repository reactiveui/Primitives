// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives;

/// <summary>A Reactive Void.</summary>
[Serializable]
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public readonly struct RxVoid : IEquatable<RxVoid>
{
    /// <summary>Gets the single <see cref="RxVoid"/> value.</summary>
    public static RxVoid Default => default;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Determines whether the two specified <see cref="RxVoid"/> values are not equal. Because <see cref="RxVoid"/> has a single value, this always returns <c>false</c>.</summary>
    /// <param name="first">The first <see cref="RxVoid"/> value to compare.</param>
    /// <param name="second">The second <see cref="RxVoid"/> value to compare.</param>
    /// <returns>Because <see cref="RxVoid"/> has a single value, this always returns <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator !=(RxVoid first, RxVoid second) => false;

    /// <summary>Determines whether the two specified <see cref="RxVoid"/> values are equal. Because <see cref="RxVoid"/> has a single value, this always returns <c>true</c>.</summary>
    /// <param name="first">The first <see cref="RxVoid"/> value to compare.</param>
    /// <param name="second">The second <see cref="RxVoid"/> value to compare.</param>
    /// <returns>Because <see cref="RxVoid"/> has a single value, this always returns <c>true</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(RxVoid first, RxVoid second) => true;

    /// <summary>Determines whether the specified <see cref="RxVoid"/> value is equal to the current <see cref="RxVoid"/>.</summary>
    /// <param name="other">An object to compare to the current <see cref="RxVoid"/> value.</param>
    /// <returns>Because <see cref="RxVoid"/> has a single value, this always returns <c>true</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(RxVoid other) => true;

    /// <summary>Determines whether the specified System.Object is equal to the current <see cref="RxVoid"/>.</summary>
    /// <param name="obj">The System.Object to compare with the current <see cref="RxVoid"/>.</param>
    /// <returns><c>true</c> if the specified System.Object is a <see cref="RxVoid"/> value; otherwise, <c>false</c>.</returns>
    public override bool Equals(object? obj) => obj is RxVoid;

    /// <summary>Returns the hash code for the current <see cref="RxVoid"/> value.</summary>
    /// <returns>A hash code for the current <see cref="RxVoid"/> value.</returns>
    public override int GetHashCode() => 0;

    /// <summary>Returns a string representation of the current <see cref="RxVoid"/> value.</summary>
    /// <returns>String representation of the current <see cref="RxVoid"/> value.</returns>
    public override string ToString() => "()";
}
