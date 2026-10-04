// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests diagnostic cause preservation for <see cref="PreparedUploadSizeExceededException"/>.</summary>
public sealed class PreparedUploadSizeExceededExceptionTests
{
    /// <summary>Verifies a wrapped preparation failure retains the original cause in application diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WrappedPreparationFailurePreservesDiagnosticCause()
    {
        var cause = new InvalidOperationException("The encoded request exceeded its bounded buffer.");
        var failure = new PreparedUploadSizeExceededException("Preparing the upload failed.", cause);

        await Assert.That(failure.InnerException).IsSameReferenceAs(cause);
        await Assert.That(failure.ToString()).Contains(cause.Message);
        await Assert.That(failure.ToString()).Contains(failure.Message);
    }
}
