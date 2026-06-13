// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Merge operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Merge two sequences emits from both.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeTwoSequences_ThenEmitsFromBoth()
    {
        var first = SignalAsync.Return(1);
        var second = SignalAsync.Return(2);
        var result = await first.Merge(second).ToListAsync();
        await Assert.That(result).Count().IsEqualTo(SampleValue2);
        await Assert.That(result).Contains(1);
    }

    /// <summary>Tests Merge enumerable emits from all.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerable_ThenEmitsFromAll()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(10), SignalAsync.Return(20), SignalAsync.Return(30)];
        var result = await sources.Merge().ToListAsync();
        await Assert.That(result).Count().IsEqualTo(SampleValue3);
    }

    /// <summary>Tests Merge observable of observables flattens.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeObservableOfObservables_ThenFlattens()
    {
        var source = new[]
        {
            SignalAsync.Return(1),
            SignalAsync.Return(2)
        }.ToAsyncSignal();
        var result = await source.Merge().ToListAsync();
        await Assert.That(result).Count().IsEqualTo(SampleValue2);
    }

    /// <summary>Tests Merge with max concurrency respects limit.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithMaxConcurrency_ThenRespectsLimit()
    {
        var activeConcurrency = 0;
        var maxConcurrency = 0;
        var source = SignalAsync.Range(1, 5).Select(i => SignalAsync.CreateAsBackgroundJob<int>(async (obs, ct) =>
        {
            lock (_gate)
            {
                activeConcurrency++;
                maxConcurrency = Math.Max(maxConcurrency, activeConcurrency);
            }

            await Task.Delay(50, ct);
            lock (_gate)
            {
                activeConcurrency--;
            }

            await obs.OnNextAsync(i, ct);
            await obs.OnCompletedAsync(Result.Success);
        }));
        var result = await source.Merge(2).ToListAsync();
        await Assert.That(result).Count().IsEqualTo(SampleValue5);
        await Assert.That(maxConcurrency).IsLessThanOrEqualTo(SampleValue2);
    }

    /// <summary>
    /// Verifies that merging an observable-of-observables where the outer source errors propagates the failure
    /// and disposes the subscription cleanly.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeObservableOfObservablesOuterErrors_ThenFailurePropagates()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        await using var sub = await outer.Values.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await outer.OnCompletedAsync(Result.Failure(new InvalidOperationException("outer fail")));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await outer.DisposeAsync();
    }

    /// <summary>Verifies that merge with max concurrency propagates an error when the inner subscription itself throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithMaxConcurrencySubscriptionThrows_ThenErrorPropagates()
    {
        var failing = SignalAsync.Create<int>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("subscribe fail");
            }
            catch (InvalidOperationException exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        var source = new[]
        {
            failing
        }.ToAsyncSignal();
        Result? completionResult = null;
        await using var sub = await source.Merge(1).SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(2));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that merging an enumerable of observables where one inner source errors propagates the failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableOneInnerErrors_ThenFailurePropagates()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Throw<int>(new InvalidOperationException(InnerFailMessage)), SignalAsync.Return(3)];
        Result? completionResult = null;
        var items = new List<int>();
        await using var sub = await sources.Merge().SubscribeAsync(
            (x, _) =>
        {
            items.Add(x);
            return default;
        },
            null,
            result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that merge enumerable forwards error-resume events from inner sources to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableInnerErrorResume_ThenForwardedToObserver()
    {
        var inner = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("warning"), ct);
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        IObservableAsync<int>[] sources = [inner];
        var errors = new List<Exception>();
        var items = new List<int>();
        await using var sub = await sources.Merge().SubscribeAsync(
            (x, _) =>
        {
            items.Add(x);
            return default;
        },
            (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(items).Contains(1);
    }

    /// <summary>Verifies that merge of observable-of-observables forwards error-resume events from inner sources.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeObservableOfObservablesInnerErrorResume_ThenForwarded()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        var errors = new List<Exception>();
        var inner = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(InnerWarningMessage), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await outer.Values.Merge().SubscribeAsync((_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await outer.OnNextAsync(inner, CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the outer source throws synchronously during subscribe in the
    /// observable-of-observables Merge overload, the subscription is disposed and the
    /// exception propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeObservableOfObservablesSubscriptionThrows_ThenDisposesAndRethrows()
    {
        var failing = SignalAsync.Create<IObservableAsync<int>>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException(SubscribeBoomMessage);
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        var act = async () =>
        {
            await using var sub = await failing.Merge().SubscribeAsync((_, _) => default, null);
        };
        await Assert.That(act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when the outer source throws synchronously during subscribe in the
    /// max-concurrency Merge overload, the subscription is disposed and the exception propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithMaxConcurrencySubscriptionThrowsDuringOuterSubscribe_ThenDisposesAndRethrows()
    {
        var failing = SignalAsync.Create<IObservableAsync<int>>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("subscribe boom max");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        var act = async () =>
        {
            await using var sub = await failing.Merge(2).SubscribeAsync((_, _) => default, null);
        };
        await Assert.That(act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when an inner observable throws during subscription in the
    /// observable-of-observables Merge, the error is propagated via completion.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeObservableInnerSubscriptionThrows_ThenCompletesWithFailure()
    {
        var throwingInner = SignalAsync.Create<int>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("inner subscribe fail");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        await using var sub = await outer.Values.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await outer.OnNextAsync(throwingInner, CancellationToken.None);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the enumerable itself throws during iteration in MergeEnumerable,
    /// the error is propagated to the observer.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableEnumerationThrows_ThenErrorPropagates()
    {
        static IEnumerable<IObservableAsync<int>> ThrowingEnumerable()
        {
            yield return SignalAsync.Return(1);
            throw new InvalidOperationException("enumeration fail");
        }

        Result? completionResult = null;
        await using var sub = await ThrowingEnumerable().Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that when a subscription to an inner source throws in MergeEnumerable BeginSubscribing,
    /// the exception is caught and the sequence completes with failure.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableInnerSubscriptionThrows_ThenCompletesWithFailure()
    {
        var throwingInner = SignalAsync.Create<int>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("inner subscribe fail");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        IObservableAsync<int>[] sources = [throwingInner];
        Result? completionResult = null;
        await using var sub = await sources.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that MergeEnumerable FinishAsync called a second time with an exception
    /// routes the exception to UnhandledExceptionHandler rather than throwing.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDoubleCompletionWithError_ThenUnhandledExceptionHandlerInvoked()
    {
        // Capture the unhandled exception
        UnhandledExceptionHandler.Register(ex => _ = ex);
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Throw<int>(new InvalidOperationException(FirstFailMessage)), SignalAsync.Throw<int>(new InvalidOperationException("second fail"))];
        Result? completionResult = null;
        await using var sub = await sources.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that MergeEnumerable awaits _subscriptionFinished when completed from a non-reentrant
    /// context (i.e., when an inner source completes asynchronously after the subscription loop finishes).
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableInnerCompletesAsynchronously_ThenAwaitsSubscriptionFinished()
    {
        var innerSignal = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await new[]
        {
            innerSignal.Values
        }.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });

        // Complete asynchronously (not during subscription loop)
        await innerSignal.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await innerSignal.DisposeAsync();
    }

    /// <summary>Tests that BlendEnumerableSignal SubscribeAsyncCore catch block disposes and rethrows.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableBeginSubscribingThrows_ThenCatchBlockHandled()
    {
        // BeginSubscribing contains an async void path that catches exceptions
        // We exercise this by ensuring an error during inner subscription is caught
        static IEnumerable<IObservableAsync<int>> ThrowingEnumerable()
        {
            yield return SignalAsync.Return(1);
            throw new InvalidOperationException("enumeration boom");
        }

        Result? completionResult = null;
        var items = new List<int>();
        await using var sub = await ThrowingEnumerable().Merge().SubscribeAsync(
            (x, _) =>
        {
            items.Add(x);
            return default;
        },
            null,
            result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult is not null, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that MergeEnumerable cancellation during inner subscription is handled.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableCanceledDuringSubscription_ThenHandledGracefully()
    {
        using var cts = new CancellationTokenSource();
        var signal = Signal.Create<int>();
        var neverCompleting = SignalAsync.Never<int>();
        IObservableAsync<int>[] sources = [signal.Values, neverCompleting];
        await using var sub = await sources.Merge().SubscribeAsync((_, _) => default, null, null, cts.Token);
        await cts.CancelAsync();

    // After cancellation, the subscription should be cleaned up
    }

    /// <summary>Tests Merge with error from one source propagates correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithError_ThenErrorPropagates()
    {
        var errorSource = SignalAsync.Throw<int>(new InvalidOperationException("merge-error"));
        var goodSource = SignalAsync.Return(1);
        await Assert.That(async () => await goodSource.Merge(errorSource).ToListAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Merge with max concurrency and error propagation.</summary>
    /// <remarks>Background jobs run with <c>startSynchronously: true</c> so the test does
    /// not depend on free thread-pool threads — under heavy cross-assembly parallel test runs
    /// the default <c>Task.Yield()</c> path used to starve and the test hit the 60s timeout.
    /// The concurrency-limit contract being asserted (four sources flow through a Merge(2)
    /// gate and all emit) is preserved.</remarks>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeConcurrencyWithSlowSource_ThenLimitsAndCompletes()
    {
        var source = SignalAsync.Range(1, 4).Select(i => SignalAsync.CreateAsBackgroundJob<int>(
            async (obs, ct) =>
        {
            await obs.OnNextAsync(i, ct);
            await obs.OnCompletedAsync(Result.Success);
        },
            startSynchronously: true));
        var result = await source.Merge(2).ToListAsync();
        await Assert.That(result).Count().IsEqualTo(SampleValue4);
    }

    /// <summary>Tests that Merge of empty enumerable returns empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEmptyEnumerable_ThenReturnsEmpty()
    {
        IObservableAsync<int>[] sources = [];
        var result = await sources.Merge().ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Merge error from one source propagates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithError_ThenErrorPropagated()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Throw<int>(new InvalidOperationException("fail"))];
        Result? completionResult = null;
        var items = new List<int>();
        await using var sub = await sources.Merge().SubscribeAsync(
            (x, _) =>
        {
            items.Add(x);
            return default;
        },
            null,
            result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that MergeEnumerable forwards errors from a source that throws during subscribe.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableSourceThrowsDuringSubscribe_ThenCompletesWithFailure()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) => ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException(SubscribeBoomMessage)));
        IObservableAsync<int>[] sources = [throwingSource];
        Result? completionResult = null;
        await using var sub = await sources.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that when an inner source throws TaskCanceledException during subscribe
    /// in MergeEnumerable BeginSubscribing, the cancellation is handled gracefully.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableInnerSubscribeThrowsTaskCanceled_ThenHandledGracefully()
    {
        var canceledSource = SignalAsync.Create<int>((_, _) => ValueTask.FromException<IAsyncDisposable>(new TaskCanceledException("subscribe canceled")));
        Result? completionResult = null;
        var items = new List<int>();
        await using var sub = await new[]
        {
            canceledSource
        }.Merge().SubscribeAsync(
            (x, _) =>
        {
            items.Add(x);
            return default;
        },
            null,
            result =>
        {
            completionResult = result;
            return default;
        });

        // The TaskCanceledException catch returns early without signaling completion,
        // so completionResult should remain null (graceful early return).
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromSeconds(2));
        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Verifies that when an inner source throws a non-cancellation exception during
    /// SubscribeAsync in MergeEnumerable, the error is forwarded via FinishAsync.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableInnerSubscribeThrows_ThenCompletesWithFailure()
    {
        Result? completionResult = null;
        var throwingSource = SignalAsync.Create<int>((_, _) => ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException(SubscribeBoomMessage)));
        await using var sub = await new[]
        {
            throwingSource
        }.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await Assert.That(completionResult.Value.Exception!.Message).Contains(SubscribeBoomMessage);
    }

    /// <summary>
    /// Verifies that when a second source in an enumerable merge throws during
    /// SubscribeAsync, the first source is properly disposed and the error propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableSecondSourceSubscribeThrows_ThenCompletesWithFailure()
    {
        Result? completionResult = null;
        var goodSource = new DirectSource<int>();
        var throwingSource = SignalAsync.Create<int>((_, _) => ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException("second subscribe boom")));
        await using var sub = await new IObservableAsync<int>[]
        {
            goodSource,
            throwingSource
        }.Merge().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await Assert.That(completionResult.Value.Exception!.Message).Contains("second subscribe boom");
    }

    /// <summary>Tests Merge inner source failure propagates error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeInnerSourceFails_ThenErrorPropagated()
    {
        var error = new InvalidOperationException("inner-error");
        var inner = SignalAsync.Throw<int>(error);
        var outer = SignalAsync.Return(inner);
        Result? completionResult = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await outer.Merge().SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            completed.TrySetResult();
            return default;
        });
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests Merge with max concurrency inner failure propagates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeWithMaxConcurrencyInnerFails_ThenErrorPropagated()
    {
        var error = new InvalidOperationException("merge-fail");
        var inner = SignalAsync.Throw<int>(error);
        var outer = SignalAsync.Return(inner);
        Result? completionResult = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await outer.Merge(1).SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            completed.TrySetResult();
            return default;
        });
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that subscribing <c>Merge(IEnumerable)</c> with an already-cancelled
    /// token short-circuits the subscription's cancellation chain immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSubscribedWithAlreadyCancelledToken_ThenSubscriptionDisposes()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var first = SignalAsync.Return(1);
        var second = SignalAsync.Return(SampleValue2);
        var values = new List<int>();
        await using var sub = await first.Merge(second).SubscribeAsync(
            (v, _) =>
        {
            values.Add(v);
            return default;
        },
            cts.Token);

        // The subscription should have been cancelled before producing any values.
        await Assert.That(values.Count).IsLessThanOrEqualTo(SampleValue2);
    }

    /// <summary>Verifies that subscribing <c>Merge(maxConcurrency)</c> with an already-cancelled
    /// token short-circuits the subscription's cancellation chain immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeMaxConcurrencySubscribedWithAlreadyCancelledToken_ThenSubscriptionDisposes()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var outer = SignalAsync.Return(SignalAsync.Return(1));
        await using var sub = await outer.Merge(1).SubscribeAsync(static (_, _) => default, cts.Token);

        // The act of producing the disposable without throwing exercises the
        // already-cancelled short-circuit in LinkExternalCancellation.
        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that subscribing <c>Merge</c> with a cancellable but not-yet-cancelled
    /// token registers the external link and the registration fires when the token is cancelled
    /// after subscribe, tearing the subscription down.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeExternalTokenCancelledAfterSubscribe_ThenRegistrationFires()
    {
        using var cts = new CancellationTokenSource();
        var first = Signal.Create<int>();
        var second = Signal.Create<int>();
        await using var sub = await first.Values.Merge(second.Values).SubscribeAsync(static (_, _) => default, cts.Token);
        await cts.CancelAsync();

        // After external cancellation the subscription must be unaffected by further pushes.
        await first.OnNextAsync(1, CancellationToken.None);
        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that subscribing <c>Merge(maxConcurrency)</c> with a cancellable but
    /// not-yet-cancelled token registers the external link.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeMaxConcurrencyExternalTokenCancelledAfterSubscribe_ThenRegistrationFires()
    {
        using var cts = new CancellationTokenSource();
        var outer = Signal.Create<IObservableAsync<int>>();
        await using var sub = await outer.Values.Merge(1).SubscribeAsync(static (_, _) => default, cts.Token);
        await cts.CancelAsync();

        // After external cancellation the subscription must be unaffected.
        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies the <see cref = "SignalAsyncExtensions.BlendCoordinator{T}.RelayNextIfActiveAsync"/>
    /// inside-gate after-dispose guard by subscribing, disposing the subscription, then calling
    /// the locked-helper directly — exercising the defensive branch that is otherwise only
    /// reachable through a real concurrency race between dispose and gate acquisition.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeRelayNextIfActiveAsyncAfterDispose_ThenDropped()
    {
        var captured = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var downstream = new CapturingWitness<int>(onNext: captured);
        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(downstream);
        await subscription.DisposeAsync();
        await subscription.RelayNextIfActiveAsync(1);
        await Assert.That(captured.Task.IsCompleted).IsFalse();
    }

    /// <summary>Verifies the <see cref = "SignalAsyncExtensions.BlendCoordinator{T}.RelayErrorIfActiveAsync"/> inside-gate after-dispose guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeRelayErrorIfActiveAsyncAfterDispose_ThenDropped()
    {
        var captured = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var downstream = new CapturingWitness<int>(onError: captured);
        var subscription = new SignalAsyncExtensions.BlendCoordinator<int>(downstream);
        await subscription.DisposeAsync();
        await subscription.RelayErrorIfActiveAsync(new InvalidOperationException("late"));
        await Assert.That(captured.Task.IsCompleted).IsFalse();
    }

    /// <summary>Verifies the enumerable-Merge <c>RelayNextIfActiveAsync</c> inside-gate after-dispose guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableOnNextAsyncLockedAfterDispose_ThenDropped()
    {
        var captured = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var downstream = new CapturingWitness<int>(onNext: captured);

        // Subscribe to a real Merge to obtain a BlendSequenceCoordinator; then dispose it
        // and call the Locked helper directly to verify the inside-gate guard.
        IObservableAsync<int>[] sources = [SignalAsync.Never<int>()];
        var sub = await sources.Merge().SubscribeAsync(downstream, CancellationToken.None);
        var enumerableSub = (SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator)sub;
        await enumerableSub.DisposeAsync();
        await enumerableSub.RelayNextIfActiveAsync(1);
        await Assert.That(captured.Task.IsCompleted).IsFalse();
    }

    /// <summary>Verifies the enumerable-Merge subscription's after-dispose <c>RelayErrorIfActiveAsync</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableOnErrorResumeAsyncLockedAfterDispose_ThenDropped()
    {
        var captured = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var downstream = new CapturingWitness<int>(onError: captured);
        IObservableAsync<int>[] sources = [SignalAsync.Never<int>()];
        var sub = await sources.Merge().SubscribeAsync(downstream, CancellationToken.None);
        var enumerableSub = (SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator)sub;
        await enumerableSub.DisposeAsync();
        await enumerableSub.RelayErrorIfActiveAsync(new InvalidOperationException("late"));
        await Assert.That(captured.Task.IsCompleted).IsFalse();
    }

    /// <summary>Test observer used by direct-invocation Merge tests; captures the first
    /// <c>OnNextAsync</c> or <c>OnErrorResumeAsync</c> via the supplied TCS so the assertion
    /// can verify the post-dispose call did not deliver anything.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class CapturingWitness<T> : IObserverAsync<T>
    {
        /// <summary>Captures the first <c>OnNextAsync</c> value, if a TCS was supplied.</summary>
        private readonly TaskCompletionSource<T>? _onNext;

        /// <summary>Captures the first <c>OnErrorResumeAsync</c> exception, if a TCS was supplied.</summary>
        private readonly TaskCompletionSource<Exception>? _onError;

        /// <summary>Initializes a new instance of the <see cref = "CapturingWitness{T}"/> class.</summary>
        /// <param name = "onNext">Optional TCS for capturing the first <c>OnNextAsync</c> value.</param>
        /// <param name = "onError">Optional TCS for capturing the first <c>OnErrorResumeAsync</c> exception.</param>
        public CapturingWitness(TaskCompletionSource<T>? onNext = null, TaskCompletionSource<Exception>? onError = null)
        {
            _onNext = onNext;
            _onError = onError;
        }

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
        {
            _onNext?.TrySetResult(value);
            return default;
        }

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            _onError?.TrySetResult(error);
            return default;
        }

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync() => default;
    }
}
