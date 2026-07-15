// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the <c>HandleCancellation</c> operators on an observable source, which await the source through a
/// cancellable task and either return its value or, on cancellation, run the cleanup action and return the
/// default.
/// </summary>
public sealed class ObservableCancellationHandlingTests
{
    /// <summary>The value emitted by the source observable.</summary>
    private const string SourceValue = "observed";

    /// <summary>Verifies awaiting the source through the token-only overload returns its value when not cancelled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HandleCancellationReturnsTheSourceValueWhenNotCancelled()
    {
        var result = await Signal.Return(SourceValue).HandleCancellation(CancellationToken.None);

        await Assert.That(result).IsEqualTo(SourceValue);
    }

    /// <summary>Verifies an already-cancelled token runs the cleanup action and yields the default value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HandleCancellationRunsTheCleanupActionAndReturnsDefaultWhenCancelled()
    {
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();
        var cleanupRan = false;

        var result = await Signal.Return(SourceValue).HandleCancellation(() => cleanupRan = true, cancellation.Token);

        await Assert.That(cleanupRan).IsTrue();
        await Assert.That(result).IsNull();
    }
}
