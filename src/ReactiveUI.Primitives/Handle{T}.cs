// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

internal static class Handle<T>
{
    public static readonly Action<T> Ignore = (T _) => { };
    public static readonly Func<T, T> Identity = (T t) => t;
    public static readonly Action<Exception, T> Throw = (ex, _) => ex.Throw();
}
