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
}
