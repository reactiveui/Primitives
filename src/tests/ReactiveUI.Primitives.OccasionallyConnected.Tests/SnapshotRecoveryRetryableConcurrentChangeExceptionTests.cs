// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the retryable snapshot conflict's diagnostic information.</summary>
public sealed class SnapshotRecoveryRetryableConcurrentChangeExceptionTests
{
    /// <summary>Verifies callers can identify a retryable I/O conflict without supplying a message.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DefaultConflictExplainsThatAResnapshotIsRequired()
    {
        var exception = new SnapshotRecoveryRetryableConcurrentChangeException();

        await Assert.That(exception is IOException).IsTrue();
        await Assert.That(exception.Message).Contains("fresh capture");
    }

    /// <summary>Verifies a custom conflict retains its message and the original failure for diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConflictRetainsCustomMessageAndOriginalFailure()
    {
        const string message = "The source advanced during snapshot capture.";
        var cause = new IOException("The local cursor changed.");
        var exception = new SnapshotRecoveryRetryableConcurrentChangeException(message, cause);

        await Assert.That(exception.Message).IsEqualTo(message);
        await Assert.That(exception.InnerException).IsSameReferenceAs(cause);
        await Assert.That(new SnapshotRecoveryRetryableConcurrentChangeException(message).Message).IsEqualTo(message);
    }
}
