// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the <c>HandleCancellation</c> operators on an observable source.</summary>
public sealed class ObservableCancellationHandlingTests
{
    /// <summary>The value emitted by the source observable.</summary>
    private const string SourceValue = "observed";

    /// <summary>The bound a cancelled wait must finish inside; a hang shows up as a failure rather than a stall.</summary>
    private const int WaitBoundSeconds = 30;

    /// <summary>Verifies awaiting the source through the token-only overload returns its value when not cancelled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HandleCancellationReturnsTheSourceValueWhenNotCancelled()
    {
        var result = await Signal.Return(SourceValue).HandleCancellation(CancellationToken.None);

        await Assert.That(result).IsEqualTo(SourceValue);
    }

    /// <summary>Verifies a pre-cancelled token runs the cleanup action and yields the default value.</summary>
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

    /// <summary>Verifies cancelling part-way through the wait ends it rather than waiting on a source that never terminates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HandleCancellationEndsTheWaitWhenTheTokenCancelsMidWait()
    {
        using CancellationTokenSource cancellation = new();
        Signal<string> source = new();

        var pending = source.HandleCancellation(cancellation.Token);
        await cancellation.CancelAsync();

        // A hang surfaces as a TimeoutException here rather than stalling the run.
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(WaitBoundSeconds));

        await Assert.That(result).IsNull();
    }

    /// <summary>Verifies cancelling part-way through the wait still runs the cleanup action.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HandleCancellationRunsTheCleanupActionWhenTheTokenCancelsMidWait()
    {
        using CancellationTokenSource cancellation = new();
        Signal<string> source = new();
        var cleanupRan = false;

        var pending = source.HandleCancellation(() => cleanupRan = true, cancellation.Token);
        await cancellation.CancelAsync();

        // A hang surfaces as a TimeoutException here rather than stalling the run.
        var result = await pending.WaitAsync(TimeSpan.FromSeconds(WaitBoundSeconds));

        await Assert.That(result).IsNull();
        await Assert.That(cleanupRan).IsTrue();
    }
}
