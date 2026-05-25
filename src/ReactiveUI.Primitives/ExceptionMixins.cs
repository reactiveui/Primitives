// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

internal static class ExceptionMixins
{
    public static void Throw(this Exception exception)
    {
#if NET472 || NETSTANDARD2_0
        System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception).Throw();
#endif
        throw exception;
    }
}
