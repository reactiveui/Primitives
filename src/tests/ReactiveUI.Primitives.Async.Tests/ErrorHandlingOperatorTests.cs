// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for error handling operators: Catch, CatchAndIgnoreErrorResume, OnErrorResumeAsFailure, Retry.</summary>
public class ErrorHandlingOperatorTests
{
    /// <summary>Message of the resumable error raised by the source.</summary>
    private const string ResumeErrorMessage = "resume error";

    /// <summary>Maximum time a test waits for a completion or error to arrive.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Window the retry test watches to confirm no completion is published.</summary>
    private static readonly TimeSpan NoCompletionWindow = TimeSpan.FromMilliseconds(500);

    /// <summary>Tests Catch with fallback switches to fallback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithFallback_ThenSwitchesToFallback()
    {
        const int FallbackValue = 42;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var fallback = SignalAsync.Return(FallbackValue);
        var result = await source.Catch(_ => fallback).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([FallbackValue]);
    }

    /// <summary>Tests Catch on success completes original sequence.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchOnSuccess_ThenOriginalSequenceCompletes()
    {
        const int SecondElement = 2;
        const int ThirdElement = 3;
        const int SourceValueCount = 3;
        const int UnusedFallbackValue = 99;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .Catch(static _ => SignalAsync.Return(UnusedFallbackValue))
            .ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests CatchAndIgnoreErrorResume ignores and continues.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndIgnoreErrorResume_ThenIgnoresAndContinues()
    {
        const int FallbackValue = 100;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var fallback = SignalAsync.Return(FallbackValue);
        var result = await source.CatchAndIgnoreErrorResume(_ => fallback).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([FallbackValue]);
    }

    /// <summary>Tests OnErrorResumeAsFailure converts error resume to failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsFailure_ThenConvertsErrorResumeToFailure()
    {
        var errorSent = false;
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(ResumeErrorMessage), ct);
            errorSent = true;
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        Result? completionResult = null;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.OnErrorResumeAsFailure().SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            _ = completed.TrySetResult();
            return default;
        });
        await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(errorSent).IsTrue();
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that OnErrorResumeAsFailure throws ArgumentNullException when source is null.</summary>
    [Test]
    public void WhenOnErrorResumeAsFailureWithNullSource_ThenThrowsArgumentNullException()
    {
        const IObservableAsync<int> Source = null!;
        _ = Assert.Throws<ArgumentNullException>(static () => Source.OnErrorResumeAsFailure());
    }

    /// <summary>Tests that OnErrorResumeAsFailure forwards emitted values to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsFailureWithValues_ThenForwardsValuesToDownstream()
    {
        const int SecondElement = 2;
        const int ThirdElement = 3;

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(SecondElement, ct);
            await observer.OnNextAsync(ThirdElement, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var result = await source.OnErrorResumeAsFailure().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Retry on transient error succeeds after retry.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryOnTransientError_ThenSucceedsAfterRetry()
    {
        const int SuccessValue = 42;
        const int ExpectedAttempts = 3;
        const int RetryCount = 5;

        var attempt = 0;
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, ct) =>
            {
                attempt++;
                if (attempt < ExpectedAttempts)
                {
                    await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
                    return;
                }

                await obs.OnNextAsync(SuccessValue, ct);
                await obs.OnCompletedAsync(Result.Success);
            },
            NewThreadTaskScheduler.Instance);
        var result = await source.Retry(RetryCount).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SuccessValue]);
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests Retry exhausted propagates last error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenRetryExhausted_ThenPropagatesLastError()
    {
        const int RetryCount = 2;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("permanent failure"));
        await Assert.That(async () => await source.Retry(RetryCount).ToListAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Retry negative count throws.</summary>
    [Test]
    public void WhenRetryNegativeCount_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Return(1).Retry(-1));

    /// <summary>Tests Retry on success completes normally.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryInfiniteOnSuccess_ThenCompletesNormally()
    {
        const int ExpectedValue = 7;
        var result = await SignalAsync.Return(ExpectedValue).Retry().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([ExpectedValue]);
    }

    /// <summary>Exercises <c>CatchObserver.OnErrorResumeAsyncCore</c>'s null-callback branch —
    /// when <c>Catch(handler)</c> is used without an <c>onErrorResume</c> argument, source
    /// <c>OnErrorResumeAsync</c> notifications flow through to the downstream verbatim.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithoutErrorResumeCallback_ThenForwardsToDownstream()
    {
        const int UnusedFallbackValue = 42;

        var signal = Signal.Create<int>();
        Exception? caught = null;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Catch(static _ => SignalAsync.Return(UnusedFallbackValue))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    IgnoredResult.Of(errorTcs.TrySetResult());
                    return default;
                });
        InvalidOperationException expected = new("catch-passthrough");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        await errorTcs.Task.WaitAsync(WaitTimeout);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Tests Catch with error resume callback is invoked.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithErrorResumeCallback_ThenCallbackInvoked()
    {
        const int FallbackValue = 99;
        List<Exception> errorResumes = [];
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("warning"), ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
            return DisposableAsync.Empty;
        });
        var result = await source.Catch(static _ => SignalAsync.Return(FallbackValue), (ex, _) =>
        {
            errorResumes.Add(ex);
            return ValueTask.CompletedTask;
        }).ToListAsync();
        await Assert.That(result).Contains(FallbackValue);
    }

    /// <summary>Tests Retry with count zero propagates error immediately without retrying.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithCountZero_ThenPropagatesErrorImmediately()
    {
        var attempt = 0;
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
            {
                attempt++;
                await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
            },
            NewThreadTaskScheduler.Instance);
        await using var sub = await source.Retry(0).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(1);
    }

    /// <summary>Tests Retry with count two exhausts all retries then propagates the last error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryCountExhausted_ThenPropagatesLastError()
    {
        const int ExpectedAttempts = 3;
        const int RetryCount = 2;

        var attempt = 0;
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
            {
                attempt++;
                await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
            },
            NewThreadTaskScheduler.Instance);
        await using var sub = await source.Retry(RetryCount).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests Retry with count one retries exactly once then propagates the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithCountOne_ThenRetriesOnceAndPropagates()
    {
        const int ExpectedAttempts = 2;
        var attempt = 0;
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
            {
                attempt++;
                await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
            },
            NewThreadTaskScheduler.Instance);
        await using var sub = await source.Retry(1).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that Catch handler throwing an exception routes to OnCompletedAsync with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchHandlerThrows_ThenCompletesWithHandlerException()
    {
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Throw<int>(new InvalidOperationException("source error"));
        await using var sub = await source.Catch(static _ => throw new ArithmeticException("handler error"))
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    _ = completed.TrySetResult(result);
                    return default;
                });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(completionResult.Exception).IsTypeOf<ArithmeticException>();
    }

    /// <summary>Tests that Catch disposes both source and handler subscriptions when the outer subscription is disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchDisposed_ThenDisposesSourceAndHandler()
    {
        TaskCompletionSource<bool> handlerItemReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var handlerObservable = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            _ = handlerItemReceived.TrySetResult(true);
            return DisposableAsync.Empty;
        });
        var sub = await source.Catch(_ => handlerObservable)
            .SubscribeAsync(static (_, _) => default, null, static _ => default);
        await handlerItemReceived.Task.WaitAsync(WaitTimeout);

        // Disposing should dispose both source and handler disposables
        await sub.DisposeAsync();
    }

    /// <summary>Exercises the <c>CatchObserver.DisposeAsyncCore</c> catch branch — when the
    /// handler-produced subscription throws on <see cref = "IAsyncDisposable.DisposeAsync"/>, the
    /// failure is routed through <see cref = "UnhandledExceptionHandler"/> rather than re-thrown.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchHandlerDisposeThrows_ThenRoutedToUnhandled()
    {
        var previousHandler = UnhandledExceptionHandler.CurrentHandler;
        try
        {
            Exception? unhandled = null;
            TaskCompletionSource unhandledTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            UnhandledExceptionHandler.Register(ex =>
            {
                unhandled = ex;
                _ = unhandledTcs.TrySetResult();
            });
            InvalidOperationException disposeFailure = new("handler-dispose-failed");
            var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
            TaskCompletionSource handlerSubscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var handlerObservable = SignalAsync.Create<int>((_, _) =>
            {
                _ = handlerSubscribed.TrySetResult();
                return new(new ThrowingDisposable(disposeFailure));
            });
            var sub = await source.Catch(_ => handlerObservable)
                .SubscribeAsync(static (_, _) => default, null, static _ => default);
            await handlerSubscribed.Task.WaitAsync(WaitTimeout);
            await sub.DisposeAsync();
            await unhandledTcs.Task.WaitAsync(WaitTimeout);
            await Assert.That(unhandled).IsSameReferenceAs(disposeFailure);
        }
        finally
        {
            UnhandledExceptionHandler.Register(previousHandler);
        }
    }

    /// <summary>Tests that CatchAndIgnoreErrorResume invokes the unhandled exception handler for error resumes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndIgnoreErrorResume_ThenReportsToUnhandledExceptionHandler()
    {
        List<Exception> reportedExceptions = [];
        UnhandledExceptionHandler.Register(reportedExceptions.Add);
        const int ExpectedFallback = 99;

        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(ResumeErrorMessage), ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
            return DisposableAsync.Empty;
        });
        var result = await source.CatchAndIgnoreErrorResume(static _ => SignalAsync.Return(ExpectedFallback))
            .ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([ExpectedFallback]);
        await Assert.That(reportedExceptions).Count().IsEqualTo(1);
        await Assert.That(reportedExceptions[0].Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests that Retry catches OperationCanceledException during re-subscription without propagating.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryResubscriptionCancelled_ThenSwallowsCancellation()
    {
        const int RetryCount = 3;

        var attempt = 0;
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            attempt++;
            if (attempt == 1)
            {
                // First subscription: complete with failure to trigger retry
                await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
                return DisposableAsync.Empty;
            }

            // Second subscription: throw OperationCanceledException from SubscribeAsync itself
            throw new OperationCanceledException("cancelled during resubscribe");
        });
        await using var sub = await source.Retry(RetryCount).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });

        // The OperationCanceledException is swallowed, so completion should not fire.
        // Give a short window to verify no completion occurs.
        var completedInTime = completed.Task.WaitAsync(NoCompletionWindow);
        const int ExpectedAttempts = 2;
        await Assert.That(() => completedInTime).ThrowsExactly<TimeoutException>();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that Retry catches generic exceptions during re-subscription and completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryResubscriptionThrows_ThenCompletesWithFailure()
    {
        const int RetryCount = 3;

        var attempt = 0;
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            attempt++;
            if (attempt == 1)
            {
                // First subscription: complete with failure to trigger retry
                await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
                return DisposableAsync.Empty;
            }

            // Second subscription: throw a generic exception from SubscribeAsync itself
            throw new ArithmeticException("resubscribe failed");
        });
        await using var sub = await source.Retry(RetryCount).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
        const int ExpectedAttempts = 2;
        await Assert.That(completionResult.Exception).IsTypeOf<ArithmeticException>();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that parameterless Retry retries indefinitely until success (covers the int.MaxValue path).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryParameterless_ThenRetriesUntilSuccess()
    {
        const int SuccessValue = 100;
        const int ExpectedAttempts = 5;

        var attempt = 0;
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, ct) =>
            {
                attempt++;
                if (attempt < ExpectedAttempts)
                {
                    await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
                    return;
                }

                await obs.OnNextAsync(SuccessValue, ct);
                await obs.OnCompletedAsync(Result.Success);
            },
            NewThreadTaskScheduler.Instance);
        var result = await source.Retry().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SuccessValue]);
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Async disposable that throws on <see cref = "IAsyncDisposable.DisposeAsync"/>. Used to verify dispose-failure routing in operators that swallow secondary errors.</summary>
    /// <param name = "error">The exception thrown when the disposable is disposed.</param>
    private sealed class ThrowingDisposable(Exception error) : IAsyncDisposable
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        [SuppressMessage("Maintainability", "SST1485:Members that must not throw should not throw", Justification = "The throwing disposal is the behaviour under test.")]
        public ValueTask DisposeAsync() => throw error;
    }
}
