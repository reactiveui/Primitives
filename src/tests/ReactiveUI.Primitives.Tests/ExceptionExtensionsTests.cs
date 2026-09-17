// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ExceptionExtensions"/>.</summary>
public sealed class ExceptionExtensionsTests
{
    /// <summary>Verifies rethrowing keeps the frame the exception was first thrown from.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Throw_KeepsTheOriginalThrowSite()
    {
        var captured = Capture();

        var rethrown = await Assert.That(captured.Throw).ThrowsExactly<InvalidOperationException>();

        await Assert.That(rethrown!.StackTrace).Contains(nameof(OriginalPlace));
    }

    /// <summary>Verifies rethrowing preserves the message and the instance.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Throw_RethrowsTheSameException()
    {
        var captured = Capture();

        var rethrown = await Assert.That(captured.Throw).ThrowsExactly<InvalidOperationException>();

        await Assert.That(rethrown).IsSameReferenceAs(captured);
    }

    /// <summary>Throws from a named frame so the test can look for it in the stack trace.</summary>
    /// <exception cref="InvalidOperationException">Always.</exception>
    private static void OriginalPlace() => throw new InvalidOperationException("original");

    /// <summary>Runs <see cref="OriginalPlace"/> and returns the exception it threw.</summary>
    /// <returns>The thrown exception.</returns>
    private static InvalidOperationException Capture()
    {
        InvalidOperationException? captured = null;
        try
        {
            OriginalPlace();
        }
        catch (InvalidOperationException e)
        {
            captured = e;
        }

        return captured!;
    }
}
