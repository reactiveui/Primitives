// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Shared delegate handlers for one-argument callbacks.</summary>
/// <typeparam name="T">The value type.</typeparam>
public static class Handle<T>
{
    /// <summary>Callback that ignores its value.</summary>
    public static readonly Action<T> Ignore = _ => { };

    /// <summary>Function that returns its input.</summary>
    public static readonly Func<T, T> Identity = t => t;

    /// <summary>Error callback that throws the supplied exception.</summary>
    public static readonly Action<Exception, T> Throw = (ex, _) => ex.Throw();
}
