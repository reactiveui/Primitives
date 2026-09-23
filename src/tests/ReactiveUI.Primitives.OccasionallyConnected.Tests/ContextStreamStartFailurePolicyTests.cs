// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ContextStreamStartFailurePolicy"/>.</summary>
public sealed class ContextStreamStartFailurePolicyTests
{
    /// <summary>Verifies captured generation cancellation is expected after that generation is canceled.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CanceledGenerationTokenOperationCancellationIsExpected()
    {
        using CancellationTokenSource source = new();
        await source.CancelAsync();
        var exception = new OperationCanceledException(source.Token);

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, source.Token);

        await Assert.That(result).IsTrue();
    }

    /// <summary>Verifies matching generation token cancellation is unexpected until the generation is canceled.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UncanceledGenerationTokenOperationCancellationIsUnexpected()
    {
        using CancellationTokenSource source = new();
        var exception = new OperationCanceledException(source.Token);

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, source.Token);

        await Assert.That(result).IsFalse();
    }

    /// <summary>Verifies task cancellation from the captured start generation is expected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CanceledGenerationTokenTaskCancellationIsExpected()
    {
        using CancellationTokenSource source = new();
        await source.CancelAsync();
        var exception = new TaskCanceledException("late stream start canceled", null, source.Token);

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, source.Token);

        await Assert.That(result).IsTrue();
    }

    /// <summary>Verifies cancellation without a captured generation token is treated as a dependency failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NoneTokenOperationCancellationIsUnexpectedWhenGenerationIsCanceled()
    {
        using CancellationTokenSource source = new();
        await source.CancelAsync();
        var exception = new OperationCanceledException(CancellationToken.None);

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, source.Token);

        await Assert.That(result).IsFalse();
    }

    /// <summary>Verifies cancellation with an unrelated token is treated as a dependency failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UnrelatedTokenOperationCancellationIsUnexpected()
    {
        using CancellationTokenSource generation = new();
        using CancellationTokenSource dependency = new();
        await generation.CancelAsync();
        await dependency.CancelAsync();
        var exception = new OperationCanceledException(dependency.Token);

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, generation.Token);

        await Assert.That(result).IsFalse();
    }

    /// <summary>Verifies ordinary failures are never treated as expected cancellation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OrdinaryFailureIsUnexpected()
    {
        using CancellationTokenSource source = new();
        await source.CancelAsync();
        var exception = new InvalidOperationException("late stream start failed");

        var result = ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, source.Token);

        await Assert.That(result).IsFalse();
    }
}
