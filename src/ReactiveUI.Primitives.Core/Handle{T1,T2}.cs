// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Shared delegate handlers for two-argument callbacks.</summary>
/// <typeparam name="T1">The first value type.</typeparam>
/// <typeparam name="T2">The second value type.</typeparam>
public static class Handle<T1, T2>
{
    /// <summary>Callback that ignores both values.</summary>
    public static readonly Action<T1, T2> Ignore = (_, _) => { };

    /// <summary>Error callback that throws the supplied exception.</summary>
    public static readonly Action<Exception, T1, T2> Throw = (ex, _, _) => ex.Throw();
}
