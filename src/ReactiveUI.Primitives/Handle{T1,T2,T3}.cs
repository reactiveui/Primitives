// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

internal static class Handle<T1, T2, T3>
{
#pragma warning disable SA1313 // Parameter names should begin with lower-case letter
    public static readonly Action<T1, T2, T3> Ignore = (_, __, ___) => { };
    public static readonly Action<Exception, T1, T2, T3> Throw = (ex, _, __, ___) => ex.Throw();
#pragma warning restore SA1313 // Parameter names should begin with lower-case letter
}
