// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

internal static class Handle
{
    public static readonly Action Nop = () => { };
    public static readonly Action<Exception> Throw = ex => ex.Throw();

    public static IObservable<TSource> CatchIgnore<TSource>(Exception ex) =>
        Signal.Empty<TSource>();
}
