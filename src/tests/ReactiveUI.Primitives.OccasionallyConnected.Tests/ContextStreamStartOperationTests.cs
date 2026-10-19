// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ContextStreamStartOperation"/>.</summary>
public sealed class ContextStreamStartOperationTests
{
    /// <summary>The first failure message used by preservation tests.</summary>
    private const string FirstFailureMessage = "first failure";

    /// <summary>The timeout used for owned start operation assertions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a canceled generation prevents the cold start delegate from running.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PreCanceledGenerationDoesNotInvokeStartDelegate()
    {
        using CancellationTokenSource generation = new();
        await generation.CancelAsync();
        var calls = 0;
        using var operation = new ContextStreamStartOperation(
            () =>
            {
                calls++;
                return Task.CompletedTask;
            },
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsNull();
        await Assert.That(calls).IsEqualTo(0);
    }

    /// <summary>Verifies generation cancellation from inside the start delegate is expected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CapturedGenerationCancellationAfterDelegateEntryIsExpected()
    {
        using CancellationTokenSource generation = new();
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var operation = new ContextStreamStartOperation(
            async () =>
            {
                entered.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, generation.Token);
            },
            generation.Token);

        operation.Start(TaskScheduler.Default);
        try
        {
            await entered.Task.WaitAsync(GuardTimeout);
            await generation.CancelAsync();
            var failure = await CaptureAsync(operation, null);

            await Assert.That(failure).IsNull();
        }
        finally
        {
            await generation.CancelAsync();
            _ = await CaptureAsync(operation, null).WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Verifies dependency cancellation without the generation token remains an unexpected failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NoneTokenOperationCancellationIsCapturedAsFailure()
    {
        using CancellationTokenSource generation = new();
        var dependencyFailure = new OperationCanceledException("dependency canceled", CancellationToken.None);
        using var operation = new ContextStreamStartOperation(
            () => Task.FromException(dependencyFailure),
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsNotNull();
        await Assert.That(failure is OperationCanceledException).IsTrue();
        if (failure is OperationCanceledException cancellation)
        {
            await Assert.That(cancellation.CancellationToken.CanBeCanceled).IsFalse();
        }
    }

    /// <summary>Verifies unrelated task cancellation remains an unexpected failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UnrelatedTaskCancellationIsCapturedAsFailure()
    {
        using CancellationTokenSource generation = new();
        using CancellationTokenSource dependency = new();
        await dependency.CancelAsync();
        using var operation = new ContextStreamStartOperation(
            () => Task.FromCanceled(dependency.Token),
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsNotNull();
        await Assert.That(failure is OperationCanceledException).IsTrue();
        if (failure is OperationCanceledException cancellation)
        {
            await Assert.That(cancellation.CancellationToken).IsEqualTo(dependency.Token);
        }
    }

    /// <summary>Verifies ordinary dependency failures keep their original exception identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OrdinaryFailureKeepsOriginalExceptionIdentity()
    {
        using CancellationTokenSource generation = new();
        var dependencyFailure = new InvalidOperationException("dependency failed");
        using var operation = new ContextStreamStartOperation(
            () => Task.FromException(dependencyFailure),
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsSameReferenceAs(dependencyFailure);
    }

    /// <summary>Verifies an existing first failure survives later expected generation cancellation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExistingFailureIsPreservedWhenGenerationCancellationIsExpected()
    {
        using CancellationTokenSource generation = new();
        await generation.CancelAsync();
        var firstFailure = new InvalidOperationException(FirstFailureMessage);
        using var operation = new ContextStreamStartOperation(
            static () => Task.FromException(new InvalidOperationException("should not run")),
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, firstFailure);

        await Assert.That(failure).IsSameReferenceAs(firstFailure);
    }

    /// <summary>Verifies an existing first failure survives a later ordinary failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExistingFailureIsPreservedWhenLaterOrdinaryFailureOccurs()
    {
        using CancellationTokenSource generation = new();
        var firstFailure = new InvalidOperationException(FirstFailureMessage);
        var laterFailure = new InvalidOperationException("later failure");
        using var operation = new ContextStreamStartOperation(
            () => Task.FromException(laterFailure),
            generation.Token);

        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, firstFailure);

        await Assert.That(failure).IsSameReferenceAs(firstFailure);
    }

    /// <summary>Verifies the start delegate remains cold until the operation starts.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartDelegateRunsOnlyAfterStart()
    {
        using CancellationTokenSource generation = new();
        var calls = 0;
        using var operation = new ContextStreamStartOperation(
            () =>
            {
                calls++;
                return Task.CompletedTask;
            },
            generation.Token);

        await Assert.That(calls).IsEqualTo(0);
        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsNull();
        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>Verifies disposal requested before start keeps the operation startable and observable.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeBeforeStartStillAllowsStartAndCapture()
    {
        using CancellationTokenSource generation = new();
        var calls = 0;
        var operation = new ContextStreamStartOperation(
            () =>
            {
                calls++;
                return Task.CompletedTask;
            },
            generation.Token);
        var started = false;
        try
        {
            RequestDispose(operation);
            operation.Start(TaskScheduler.Default);
            started = true;
            var failure = await CaptureAsync(operation, null);

            await Assert.That(failure).IsNull();
            await Assert.That(calls).IsEqualTo(1);
        }
        finally
        {
            if (started)
            {
                _ = await CaptureAsync(operation, null).WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies concurrent captures preserve their own first-failure state.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConcurrentCapturesPreserveIndependentFirstFailures()
    {
        CancellationTokenSource generation = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstFailure = new InvalidOperationException(FirstFailureMessage);
        var laterFailure = new InvalidOperationException("later failure");
        Task<Exception?>? observer = null;
        Task<Exception?>? drain = null;
        var operation = new ContextStreamStartOperation(
            async () =>
            {
                await release.Task.WaitAsync(GuardTimeout);
                throw laterFailure;
            },
            generation.Token);
        try
        {
            operation.Start(TaskScheduler.Default);
            observer = operation.CaptureFailureAsync(null).AsTask();
            drain = operation.CaptureFailureAsync(firstFailure).AsTask();
            release.SetResult();

            var observerFailure = await observer.WaitAsync(GuardTimeout);
            var drainFailure = await drain.WaitAsync(GuardTimeout);

            await Assert.That(observerFailure).IsSameReferenceAs(laterFailure);
            await Assert.That(drainFailure).IsSameReferenceAs(firstFailure);
        }
        finally
        {
            _ = release.TrySetResult();
            try
            {
                if (observer is not null)
                {
                    _ = await observer.WaitAsync(GuardTimeout);
                }
            }
            finally
            {
                try
                {
                    if (drain is not null)
                    {
                        _ = await drain.WaitAsync(GuardTimeout);
                    }
                }
                finally
                {
                    operation.Dispose();
                    generation.Dispose();
                }
            }
        }
    }

    /// <summary>Verifies repeated disposal after successful completion is harmless.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RepeatedDisposeAfterSuccessfulCompletionIsHarmless()
    {
        using CancellationTokenSource generation = new();
        var operation = new ContextStreamStartOperation(static () => Task.CompletedTask, generation.Token);
        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsNull();

        operation.Dispose();
        operation.Dispose();
    }

    /// <summary>Verifies disposal after failure preserves the original failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeAfterFailureKeepsOriginalFailure()
    {
        using CancellationTokenSource generation = new();
        var expected = new InvalidOperationException("operation failed");
        var operation = new ContextStreamStartOperation(() => Task.FromException(expected), generation.Token);
        operation.Start(TaskScheduler.Default);
        var failure = await CaptureAsync(operation, null);

        await Assert.That(failure).IsSameReferenceAs(expected);

        operation.Dispose();
        operation.Dispose();
    }

    /// <summary>Captures an operation failure with the test timeout.</summary>
    /// <param name="operation">The owned start operation.</param>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The captured first failure.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<Exception?> CaptureAsync(ContextStreamStartOperation operation, Exception? failure) =>
        operation.CaptureFailureAsync(failure).AsTask().WaitAsync(GuardTimeout);

    /// <summary>Requests disposal for the operation under test.</summary>
    /// <param name="operation">The operation to dispose.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RequestDispose(ContextStreamStartOperation operation) => operation.Dispose();
}
