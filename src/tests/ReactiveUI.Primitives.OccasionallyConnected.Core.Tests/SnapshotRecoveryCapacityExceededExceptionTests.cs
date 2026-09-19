// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SnapshotRecoveryCapacityExceededException"/>.</summary>
public sealed class SnapshotRecoveryCapacityExceededExceptionTests
{
    /// <summary>The observed capacity value used by exception tests.</summary>
    private const long Observed = 2;

    /// <summary>Verifies bounded capture capacity failures carry typed limit details.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorCarriesLimitDetails()
    {
        var exception = new SnapshotRecoveryCapacityExceededException("MaximumPendingOperations", 1, Observed);

        await Assert.That(exception.LimitName).IsEqualTo("MaximumPendingOperations");
        await Assert.That(exception.Maximum).IsEqualTo(1);
        await Assert.That(exception.Observed).IsEqualTo(Observed);
    }

    /// <summary>Verifies the default constructor keeps typed limit details empty.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultConstructorKeepsLimitDetailsEmpty()
    {
        var exception = new SnapshotRecoveryCapacityExceededException();

        await Assert.That(exception.LimitName).IsEqualTo(string.Empty);
        await Assert.That(exception.Maximum).IsEqualTo(0);
        await Assert.That(exception.Observed).IsEqualTo(0);
    }

    /// <summary>Verifies message constructors preserve messages and reject null messages.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageConstructorCarriesMessageAndRejectsNull()
    {
        const string Message = "bounded capture failed";
        var exception = new SnapshotRecoveryCapacityExceededException(Message);
        Action missing = static () => _ = new SnapshotRecoveryCapacityExceededException(null!);

        await Assert.That(exception.Message).IsEqualTo(Message);
        await Assert.That(missing).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies message and inner constructors preserve exception details and reject null messages.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MessageInnerConstructorCarriesDetailsAndRejectsNullMessage()
    {
        const string Message = "bounded capture failed";
        var inner = new InvalidOperationException("inner");
        var exception = new SnapshotRecoveryCapacityExceededException(Message, inner);
        Action missing = static () => _ = new SnapshotRecoveryCapacityExceededException(null!, new InvalidOperationException("inner"));

        await Assert.That(exception.Message).IsEqualTo(Message);
        await Assert.That(exception.InnerException).IsSameReferenceAs(inner);
        await Assert.That(missing).ThrowsExactly<ArgumentNullException>();
    }
}
