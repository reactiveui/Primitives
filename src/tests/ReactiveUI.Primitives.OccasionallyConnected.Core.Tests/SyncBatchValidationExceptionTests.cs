// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncBatchValidationException"/>.</summary>
public sealed class SyncBatchValidationExceptionTests
{
    /// <summary>The representative diagnostic message.</summary>
    private const string FailureMessage = "The peer omitted an operation.";

    /// <summary>Verifies constructors preserve diagnostic context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorsPreserveDiagnosticContext()
    {
        var defaultException = new SyncBatchValidationException();
        var messageException = new SyncBatchValidationException(FailureMessage);
        var cause = new InvalidOperationException(FailureMessage);
        var innerException = new SyncBatchValidationException(FailureMessage, cause);
        var reasonException = new SyncBatchValidationException(SyncBatchValidationError.OmittedOperationResult, FailureMessage);

        await Assert.That(defaultException.Error).IsEqualTo(SyncBatchValidationError.MalformedBatch);
        await Assert.That(messageException.Message).IsEqualTo(FailureMessage);
        await Assert.That(messageException.Error).IsEqualTo(SyncBatchValidationError.MalformedBatch);
        await Assert.That(innerException.Message).IsEqualTo(FailureMessage);
        await Assert.That(innerException.InnerException).IsSameReferenceAs(cause);
        await Assert.That(innerException.Error).IsEqualTo(SyncBatchValidationError.MalformedBatch);
        await Assert.That(reasonException.Message).IsEqualTo(FailureMessage);
        await Assert.That(reasonException.Error).IsEqualTo(SyncBatchValidationError.OmittedOperationResult);
    }
}
