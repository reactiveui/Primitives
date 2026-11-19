// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CoreFatalExceptionHelper"/>.</summary>
public sealed class CoreFatalExceptionHelperTests
{
    /// <summary>Verifies each fatal runtime exception type is classified as fatal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IsFatal_RuntimeFatalExceptions_ReturnsTrue()
    {
        Exception[] fatal =
        [
            CreateException(typeof(StackOverflowException)),
            CreateException(typeof(AccessViolationException)),
            new AppDomainUnloadedException(),
            new BadImageFormatException(),
            new CannotUnloadAppDomainException(),
            new InvalidProgramException(),
            CreateException(typeof(ThreadAbortException)),
            CreateException(typeof(OutOfMemoryException))
        ];

        foreach (var exception in fatal)
        {
            await Assert.That(CoreFatalExceptionHelper.IsFatal(exception)).IsTrue();
        }
    }

    /// <summary>Verifies recoverable exceptions, including the out-of-memory subtype, are not classified as fatal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IsFatal_RecoverableExceptions_ReturnsFalse()
    {
        await Assert.That(CoreFatalExceptionHelper.IsFatal(new InsufficientMemoryException())).IsFalse();
        await Assert.That(CoreFatalExceptionHelper.IsFatal(new InvalidOperationException())).IsFalse();
    }

    /// <summary>Creates an exception instance without invoking a platform-specific or runtime-reserved constructor.</summary>
    /// <param name="exceptionType">The exception type.</param>
    /// <returns>An exception instance.</returns>
    private static Exception CreateException(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
                                    | DynamicallyAccessedMemberTypes.NonPublicConstructors)]
        Type exceptionType) =>
        (Exception)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(exceptionType);
}
