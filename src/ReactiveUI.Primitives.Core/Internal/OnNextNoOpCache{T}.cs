// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Internal;

/// <summary>Holds cached no-op value callbacks by value type.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal static class OnNextNoOpCache<T>
{
    /// <summary>Gets the cached no-op value callback.</summary>
    public static readonly Action<T> Instance = static _ => { };
}
