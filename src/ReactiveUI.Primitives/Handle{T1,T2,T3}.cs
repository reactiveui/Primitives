// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Shared delegate handlers for three-argument callbacks.
/// </summary>
/// <typeparam name="T1">The first value type.</typeparam>
/// <typeparam name="T2">The second value type.</typeparam>
/// <typeparam name="T3">The third value type.</typeparam>
internal static class Handle<T1, T2, T3>
{
    /// <summary>
    /// Callback that ignores all values.
    /// </summary>
    public static readonly Action<T1, T2, T3> Ignore = (_, _, _) => { };

    /// <summary>
    /// Error callback that throws the supplied exception.
    /// </summary>
    public static readonly Action<Exception, T1, T2, T3> Throw = (ex, _, _, _) => ex.Throw();
}
