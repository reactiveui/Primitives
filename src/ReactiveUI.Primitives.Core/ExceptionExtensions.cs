// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Extension methods for rethrowing exceptions.</summary>
public static class ExceptionExtensions
{
    /// <summary>Throwing operators for an exception.</summary>
    /// <param name="exception">Exception to throw.</param>
    extension(Exception exception)
    {
        /// <summary>Throws the exception, keeping the stack trace from where it was first thrown.</summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Throw() =>
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception).Throw();
    }
}
