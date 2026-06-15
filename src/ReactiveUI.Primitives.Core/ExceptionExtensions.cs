// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Exception helper methods.</summary>
public static class ExceptionExtensions
{
    /// <summary>Throwing operators for an exception.</summary>
    /// <param name="exception">Exception to throw.</param>
    extension(Exception exception)
    {
        /// <summary>Throws the exception while preserving stack trace where required by the target framework.</summary>
        public void Throw()
        {
#if NET472 || NETSTANDARD2_0
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(exception).Throw();
#endif
            throw exception;
        }
    }
}
