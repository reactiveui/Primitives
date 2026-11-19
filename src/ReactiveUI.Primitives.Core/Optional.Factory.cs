// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives;

/// <summary>Provides factory helpers for optional values.</summary>
public static class Optional
{
    /// <summary>Wraps the specified value in an optional container.</summary>
    /// <typeparam name="T">The type of the optional value.</typeparam>
    /// <param name="value">The value to wrap.</param>
    /// <returns>The optional value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static Optional<T> Some<T>([AllowNull] T value) => Optional<T>.Some(value);

    /// <summary>Attempts to retrieve the value contained in <paramref name="optional"/>.</summary>
    /// <typeparam name="T">The type of the value contained in the <see cref="Optional{T}"/>.</typeparam>
    /// <param name="optional">The <see cref="Optional{T}"/> instance from which to retrieve the value.</param>
    /// <param name="value">When this method returns, contains the value if <paramref name="optional"/> has a value; otherwise, the default value for <typeparamref name="T"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="optional"/> has a value; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryGetValue<T>(Optional<T> optional, [NotNullWhen(true)] out T? value)
    {
        var hasValue = optional.HasValue;
        value = hasValue ? optional.Value : default;
        return hasValue;
    }
}
