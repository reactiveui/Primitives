// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Result"/> success and failure contracts.</summary>
public class ResultTests
{
    /// <summary>Failure message used by result tests.</summary>
    private const string FailureMessage = "boom";

    /// <summary>Covers the success result contract.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResultSuccessReportsStatusAndString()
    {
        var result = Result.Success;

        await Assert.That(result.IsSuccess).IsTrue();
        await Assert.That(result.IsFailure).IsFalse();
        await Assert.That(result.Exception).IsNull();
        await Assert.That(result.ToString()).IsEqualTo("Success");
    }

    /// <summary>Covers the failure result contract.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResultFailureReportsExceptionStatusAndString()
    {
        InvalidOperationException error = new(FailureMessage);
        var constructed = new Result(error);
        var created = Result.Failure(error);

        await Assert.That(constructed.IsSuccess).IsFalse();
        await Assert.That(constructed.IsFailure).IsTrue();
        await Assert.That(constructed.Exception).IsSameReferenceAs(error);
        await Assert.That(constructed.ToString()).IsEqualTo("Failure{boom}");
        await Assert.That(created.Exception).IsSameReferenceAs(error);
    }

    /// <summary>Covers failure result argument validation.</summary>
    [Test]
    public void ResultRejectsNullException() =>
        Assert.Throws<ArgumentNullException>(() =>
        {
            Result invalid = new(null!);
            GC.KeepAlive(invalid);
        });
}
