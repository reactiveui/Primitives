// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for error handling operators: Catch, CatchAndIgnoreErrorResume, OnErrorResumeAsFailure, Retry.
/// </summary>
public class ErrorHandlingOperatorTests
{
    /// <summary>Tests Catch with fallback switches to fallback.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithFallback_ThenSwitchesToFallback()
    {
        const int FallbackValue = 42;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var fallback = SignalAsync.Return(FallbackValue);

        var result = await source.Catch(_ => fallback).ToListAsync();

        await Assert.That(result).IsEquivalentTo([FallbackValue]);
    }

    /// <summary>Tests Catch on success completes original sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchOnSuccess_ThenOriginalSequenceCompletes()
    {
        const int SecondElement = 2;
        const int ThirdElement = 3;
        var result = await SignalAsync.Range(1, 3)
            .Catch(_ => SignalAsync.Return(99))
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests CatchAndIgnoreErrorResume ignores and continues.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndIgnoreErrorResume_ThenIgnoresAndContinues()
    {
        const int FallbackValue = 100;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var fallback = SignalAsync.Return(FallbackValue);

        var result = await source.CatchAndIgnoreErrorResume(_ => fallback).ToListAsync();

        await Assert.That(result).IsEquivalentTo([FallbackValue]);
    }

    /// <summary>Tests OnErrorResumeAsFailure converts error resume to failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsFailure_ThenConvertsErrorResumeToFailure()
    {
        var errorSent = false;
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume error"), ct);
            errorSent = true;
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        Result? completionResult = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source
            .OnErrorResumeAsFailure()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    completed.TrySetResult();
                    return default;
                });

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(errorSent).IsTrue();
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that OnErrorResumeAsFailure throws ArgumentNullException when source is null.</summary>
    [Test]
    public void WhenOnErrorResumeAsFailureWithNullSource_ThenThrowsArgumentNullException()
    {
        const IObservableAsync<int> Source = null!;
        Assert.Throws<ArgumentNullException>(() => Source.OnErrorResumeAsFailure());
    }

    /// <summary>Tests that OnErrorResumeAsFailure forwards emitted values to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsFailureWithValues_ThenForwardsValuesToDownstream()
    {
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(2, ct);
            await observer.OnNextAsync(3, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        const int SecondElement = 2;
        const int ThirdElement = 3;
        var result = await source.OnErrorResumeAsFailure().ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Retry on transient error succeeds after retry.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryOnTransientError_ThenSucceedsAfterRetry()
    {
        const int SuccessValue = 42;
        const int ExpectedAttempts = 3;
        var attempt = 0;
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, ct) =>
        {
            attempt++;
            if (attempt < 3)
            {
                await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
                return;
            }

            await obs.OnNextAsync(SuccessValue, ct);
            await obs.OnCompletedAsync(Result.Success);
        },
            NewThreadTaskScheduler.Instance);

        var result = await source.Retry(5).ToListAsync();

        await Assert.That(result).IsEquivalentTo([SuccessValue]);
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests Retry exhausted propagates last error.</summary>
    [Test]
    public void WhenRetryExhausted_ThenPropagatesLastError()
    {
        const int RetryCount = 2;
        var source = SignalAsync.Throw<int>(new InvalidOperationException("permanent failure"));

        Assert.ThrowsAsync<InvalidOperationException>(async () => await source.Retry(RetryCount).ToListAsync());
    }

    /// <summary>Tests Retry negative count throws.</summary>
    [Test]
    public void WhenRetryNegativeCount_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SignalAsync.Return(1).Retry(-1));

    /// <summary>Tests Retry on success completes normally.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryInfiniteOnSuccess_ThenCompletesNormally()
    {
        const int ExpectedValue = 7;
        var result = await SignalAsync.Return(7).Retry().ToListAsync();

        await Assert.That(result).IsEquivalentTo([ExpectedValue]);
    }

    /// <summary>Exercises <c>CatchObserver.OnErrorResumeAsyncCore</c>'s null-callback branch —
    /// when <c>Catch(handler)</c> is used without an <c>onErrorResume</c> argument, source
    /// <c>OnErrorResumeAsync</c> notifications flow through to the downstream verbatim.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithoutErrorResumeCallback_ThenForwardsToDownstream()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Catch(static _ => SignalAsync.Return(42))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("catch-passthrough");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Tests Catch with error resume callback is invoked.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchWithErrorResumeCallback_ThenCallbackInvoked()
    {
        const int FallbackValue = 99;
        var errorResumes = new List<Exception>();
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("warning"), ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
            return DisposableAsync.Empty;
        });

        var result = await source.Catch(
            _ => SignalAsync.Return(99),
            (ex, _) =>
                {
                    try
                    {
                        errorResumes.Add(ex);
                        return ValueTask.CompletedTask;
                    }
                    catch (Exception exception)
                    {
                        return ValueTask.FromException(exception);
                    }
                }).ToListAsync();

        await Assert.That(result).Contains(FallbackValue);
    }

    /// <summary>Tests Retry with count zero propagates error immediately without retrying.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithCountZero_ThenPropagatesErrorImmediately()
    {
        var attempt = 0;
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
        {
            attempt++;
            await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
        },
            NewThreadTaskScheduler.Instance);

        await using var sub = await source
            .Retry(0)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(1);
    }

    /// <summary>Tests Retry with count two exhausts all retries then propagates the last error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryCountExhausted_ThenPropagatesLastError()
    {
        const int ExpectedAttempts = 3;
        var attempt = 0;
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
        {
            attempt++;
            await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
        },
            NewThreadTaskScheduler.Instance);

        await using var sub = await source
            .Retry(2)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests Retry with count one retries exactly once then propagates the error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithCountOne_ThenRetriesOnceAndPropagates()
    {
        const int ExpectedAttempts = 2;
        var attempt = 0;
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, _) =>
        {
            attempt++;
            await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
        },
            NewThreadTaskScheduler.Instance);

        await using var sub = await source
            .Retry(1)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that Catch handler throwing an exception routes to OnCompletedAsync with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchHandlerThrows_ThenCompletesWithHandlerException()
    {
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Throw<int>(new InvalidOperationException("source error"));

        await using var sub = await source
            .Catch<int>(_ => throw new ArithmeticException("handler error"))
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult.IsFailure).IsTrue();
        await Assert.That(completionResult.Exception).IsTypeOf<ArithmeticException>();
    }

    /// <summary>Tests that Catch disposes both source and handler subscriptions when the outer subscription is disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchDisposed_ThenDisposesSourceAndHandler()
    {
        var handlerItemReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
        var handlerObservable = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            handlerItemReceived.TrySetResult(true);
            return DisposableAsync.Empty;
        });

        var sub = await source
            .Catch(_ => handlerObservable)
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ => default);

        await handlerItemReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Disposing should dispose both source and handler disposables
        await sub.DisposeAsync();
    }

    /// <summary>Exercises the <c>CatchObserver.DisposeAsyncCore</c> catch branch — when the
    /// handler-produced subscription throws on <see cref="IAsyncDisposable.DisposeAsync"/>, the
    /// failure is routed through <see cref="UnhandledExceptionHandler"/> rather than re-thrown.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchHandlerDisposeThrows_ThenRoutedToUnhandled()
    {
        var previousHandler = UnhandledExceptionHandler.CurrentHandler;
        try
        {
            Exception? unhandled = null;
            var unhandledTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            UnhandledExceptionHandler.Register(ex =>
            {
                unhandled = ex;
                unhandledTcs.TrySetResult();
            });

            var disposeFailure = new InvalidOperationException("handler-dispose-failed");
            var source = SignalAsync.Throw<int>(new InvalidOperationException("fail"));
            var handlerSubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handlerObservable = SignalAsync.Create<int>((_, _) =>
            {
                handlerSubscribed.TrySetResult();
                return new ValueTask<IAsyncDisposable>(new ThrowingDisposable(disposeFailure));
            });

            var sub = await source.Catch(_ => handlerObservable)
                .SubscribeAsync(static (_, _) => default, null, static _ => default);

            await handlerSubscribed.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await sub.DisposeAsync();
            await unhandledTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await Assert.That(unhandled).IsSameReferenceAs(disposeFailure);
        }
        finally
        {
            UnhandledExceptionHandler.Register(previousHandler);
        }
    }

    /// <summary>Tests that CatchAndIgnoreErrorResume invokes the unhandled exception handler for error resumes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndIgnoreErrorResume_ThenReportsToUnhandledExceptionHandler()
    {
        var reportedExceptions = new List<Exception>();
        UnhandledExceptionHandler.Register(reportedExceptions.Add);

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume error"), ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
            return DisposableAsync.Empty;
        });

        const int ExpectedFallback = 99;
        var result = await source.CatchAndIgnoreErrorResume(_ => SignalAsync.Return(99)).ToListAsync();

        await Assert.That(result).IsEquivalentTo([ExpectedFallback]);
        await Assert.That(reportedExceptions).Count().IsEqualTo(1);
        await Assert.That(reportedExceptions[0].Message).IsEqualTo("resume error");
    }

    /// <summary>Tests that Retry catches OperationCanceledException during re-subscription without propagating.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryResubscriptionCancelled_ThenSwallowsCancellation()
    {
        var attempt = 0;
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

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

        await using var sub = await source
            .Retry(3)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        // The OperationCanceledException is swallowed, so completion should not fire.
        // Give a short window to verify no completion occurs.
        var completedInTime = completed.Task.WaitAsync(TimeSpan.FromMilliseconds(500));
        const int ExpectedAttempts = 2;
        await Assert.ThrowsAsync<TimeoutException>(async () => await completedInTime);
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that Retry catches generic exceptions during re-subscription and completes with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryResubscriptionThrows_ThenCompletesWithFailure()
    {
        var attempt = 0;
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

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

        await using var sub = await source
            .Retry(3)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult.IsFailure).IsTrue();
        const int ExpectedAttempts = 2;
        await Assert.That(completionResult.Exception).IsTypeOf<ArithmeticException>();
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Tests that parameterless Retry retries indefinitely until success (covers the int.MaxValue path).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryParameterless_ThenRetriesUntilSuccess()
    {
        var attempt = 0;
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, ct) =>
        {
            attempt++;
            if (attempt < 5)
            {
                await obs.OnCompletedAsync(Result.Failure(new InvalidOperationException($"attempt {attempt}")));
                return;
            }

            await obs.OnNextAsync(100, ct);
            await obs.OnCompletedAsync(Result.Success);
        },
            NewThreadTaskScheduler.Instance);

        const int SuccessValue = 100;
        const int ExpectedAttempts = 5;
        var result = await source.Retry().ToListAsync();

        await Assert.That(result).IsEquivalentTo([SuccessValue]);
        await Assert.That(attempt).IsEqualTo(ExpectedAttempts);
    }

    /// <summary>Async disposable that throws on <see cref="IAsyncDisposable.DisposeAsync"/>.
    /// Used to verify dispose-failure routing in operators that swallow secondary errors.</summary>
    private sealed class ThrowingDisposable(Exception error) : IAsyncDisposable
    {
        /// <inheritdoc/>
        public ValueTask DisposeAsync() => throw error;
    }
}
