// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Identifies runtime exceptions that should not be converted into observable error notifications.</summary>
internal static class FatalExceptionHelper
{
    /// <summary>Determines whether an exception represents a fatal runtime failure.</summary>
    /// <param name="error">The exception to classify.</param>
    /// <returns><see langword="true"/> when the exception should be allowed to propagate.</returns>
    internal static bool IsFatal(Exception error) =>
        error is
            StackOverflowException or
            AccessViolationException or
            AppDomainUnloadedException or
            BadImageFormatException or
            CannotUnloadAppDomainException or
            InvalidProgramException or
            System.Threading.ThreadAbortException or
            (OutOfMemoryException and not InsufficientMemoryException);
}
