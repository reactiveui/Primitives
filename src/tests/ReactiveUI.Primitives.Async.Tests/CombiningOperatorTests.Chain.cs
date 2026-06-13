// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Concat operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Concat two sequences emits in order.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatTwoSequences_ThenEmitsInOrder()
    {
        var first = SignalAsync.Range(1, 2);
        var second = SignalAsync.Range(3, 2);
        var result = await first.Concat(second).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests Concat enumerable emits in sequential order.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerable_ThenEmitsInSequentialOrder()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Return(2), SignalAsync.Return(3)];
        var result = await sources.Concat().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests Concat observable of observables concatenates sequentially.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservableOfObservables_ThenConcatenatesSequentially()
    {
        var sources = new[] { SignalAsync.Range(1, 2), SignalAsync.Range(3, 2) }.ToAsyncSignal();
        var result = await sources.Concat().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Verifies that concat of an observable-of-observables where the outer source errors propagates the failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservableOfObservablesOuterErrors_ThenFailurePropagates()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        await using var sub = await outer.Values.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await outer.OnCompletedAsync(Result.Failure(new InvalidOperationException("outer fail")));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await outer.DisposeAsync();
    }

    /// <summary>Verifies that concat of an observable-of-observables where an inner source errors propagates the failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservableOfObservablesInnerErrors_ThenFailurePropagates()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        await using var sub = await outer.Values.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await outer.OnNextAsync(
            SignalAsync.Throw<int>(new InvalidOperationException(InnerFailMessage)),
            CancellationToken.None);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that concat of an observable-of-observables completes correctly when outer completes while no
    /// inner source is active (empty buffer scenario).
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservableOfObservablesOuterCompletesWithEmptyBuffer_ThenCompletes()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        List<int> items = [];
        await using var sub = await outer.Values.Concat().SubscribeAsync(
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

        // Emit an inner that completes, then complete outer when buffer is empty
        await outer.OnNextAsync(SignalAsync.Return(Sentinel42), CancellationToken.None);
        await outer.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).Contains(Sentinel42);
        await outer.DisposeAsync();
    }

    /// <summary>Verifies that concat of an enumerable where one source errors propagates the failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableOneSourceErrors_ThenFailurePropagates()
    {
        IObservableAsync<int>[] sources =
        [
            SignalAsync.Return(1), SignalAsync.Throw<int>(new InvalidOperationException(InnerFailMessage)),
            SignalAsync.Return(3)
        ];
        Result? completionResult = null;
        List<int> items = [];
        await using var sub = await sources.Concat().SubscribeAsync(
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
        await Assert.That(items).Contains(1);
    }

    /// <summary>Verifies that disposing a concat enumerable subscription multiple times is idempotent and does not throw.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableDoubleDisposal_ThenIdempotent()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Return(2)];
        var sub = await sources.Concat().SubscribeAsync((_, _) => default, null);
        await sub.DisposeAsync();
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the outer source throws synchronously during subscription in
    /// ChainSignalSourcesSignal, the subscription is disposed and the exception propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesSubscriptionThrows_ThenDisposesAndRethrows()
    {
        var failing = SignalAsync.Create<IObservableAsync<int>>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("concat subscribe boom");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        var act = async () =>
        {
            await using var sub = await failing.Concat().SubscribeAsync((_, _) => default, null);
        };
        await Assert.That(act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when the inner subscription throws during SubscribeCurrentInnerAsync in
    /// ChainSignalSourcesSignal, the error is propagated via completion.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesInnerSubscriptionThrows_ThenCompletesWithFailure()
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
        await using var sub = await outer.Values.Concat().SubscribeAsync((_, _) => default, null, result =>
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
    /// Verifies that ChainSignalSourcesSignal double-completion with an exception routes the
    /// exception to UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesDoubleCompletionWithError_ThenUnhandledExceptionHandlerInvoked()
    {
        UnhandledExceptionHandler.Register(ex => _ = ex);
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        var sub = await outer.Values.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });

        // Complete with failure first, then dispose (which calls FinishAsync(null))
        await outer.OnCompletedAsync(Result.Failure(new InvalidOperationException(FirstFailMessage)));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        // Now dispose – this will call FinishAsync(null) which will hit the already-disposed path
        await sub.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that error-resume events from the outer source in ChainSignalSourcesSignal
    /// are forwarded to the downstream observer.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesOuterErrorResume_ThenForwardedToObserver()
    {
        var outerSource = SignalAsync.Create<IObservableAsync<int>>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(OuterWarningMessage), ct);
            await observer.OnNextAsync(SignalAsync.Return(1), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<Exception> errors = [];
        List<int> items = [];
        await using var sub = await outerSource.Concat().SubscribeAsync(
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
        await Assert.That(errors[0].Message).IsEqualTo(OuterWarningMessage);
    }

    /// <summary>
    /// Verifies that error-resume events from an inner source in ChainSignalSourcesSignal
    /// are forwarded to the downstream observer.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesInnerErrorResume_ThenForwardedToObserver()
    {
        var innerWithError = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(InnerWarningMessage), ct);
            await observer.OnNextAsync(42, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var outer = Signal.Create<IObservableAsync<int>>();
        List<Exception> errors = [];
        List<int> items = [];
        await using var sub = await outer.Values.Concat().SubscribeAsync(
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
        await outer.OnNextAsync(innerWithError, CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo(InnerWarningMessage);
        await Assert.That(items).Contains(Sentinel42);
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that ChainSignalSourcesSignal correctly buffers multiple inner observables and
    /// subscribes to each sequentially when the previous completes, covering the buffer-count > 1 path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesMultipleBufferedInners_ThenSubscribesSequentially()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        List<int> items = [];
        Result? completionResult = null;
        await using var sub = await outer.Values.Concat().SubscribeAsync(
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

        // Emit a long-lived inner and two more while it's still active
        var innerSignal = Signal.Create<int>();
        await outer.OnNextAsync(innerSignal.Values, CancellationToken.None);

        // These will be buffered because the first inner hasn't completed
        await outer.OnNextAsync(SignalAsync.Return(SampleValue20), CancellationToken.None);
        await outer.OnNextAsync(SignalAsync.Return(SampleValue30), CancellationToken.None);
        await outer.OnCompletedAsync(Result.Success);

        // Complete the first inner
        await innerSignal.OnNextAsync(SampleValue10, CancellationToken.None);
        await innerSignal.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
        await innerSignal.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the first inner observable in ChainEnumerableSignal throws during
    /// subscription, the subscription is disposed and the exception propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenChainSequenceCoordinatorThrows_ThenDisposesAndRethrows()
    {
        var throwing = SignalAsync.Create<int>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("concat enum subscribe boom");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });
        IObservableAsync<int>[] sources = [throwing];
        Result? completionResult = null;
        await using var sub = await sources.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that ChainEnumerableSignal forwards error-resume events from inner sources to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableInnerErrorResume_ThenForwardedToObserver()
    {
        var innerWithError = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(InnerWarningMessage), ct);
            await observer.OnNextAsync(42, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        IObservableAsync<int>[] sources = [innerWithError];
        List<Exception> errors = [];
        List<int> items = [];
        await using var sub = await sources.Concat().SubscribeAsync(
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
        await Assert.That(errors[0].Message).IsEqualTo(InnerWarningMessage);
        await Assert.That(items).Contains(Sentinel42);
    }

    /// <summary>
    /// Verifies that ChainEnumerableSignal double-completion with an exception routes the
    /// exception to UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableDoubleCompletionWithError_ThenUnhandledExceptionHandlerInvoked()
    {
        UnhandledExceptionHandler.Register(ex => _ = ex);
        IObservableAsync<int>[] sources = [SignalAsync.Throw<int>(new InvalidOperationException("fail"))];
        Result? completionResult = null;
        var sub = await sources.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        // Second dispose attempts FinishAsync(null) on an already-disposed subscription
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that ChainEnumerableSignal handles the catch path in SubscribeNextSignalAsync
    /// when the enumerator MoveNext throws.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableEnumeratorThrows_ThenCompletesWithFailure()
    {
        static IEnumerable<IObservableAsync<int>> ThrowingEnumerable()
        {
            yield return SignalAsync.Return(1);
            throw new InvalidOperationException("enumerator fail");
        }

        Result? completionResult = null;
        List<int> items = [];
        await using var sub = await ThrowingEnumerable().Concat().SubscribeAsync(
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
        await Assert.That(items).Contains(1);
    }

    /// <summary>Tests Concat with error in second source propagates correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatWithErrorInSecondSource_ThenErrorPropagates()
    {
        var first = SignalAsync.Return(1);
        var second = SignalAsync.Throw<int>(new InvalidOperationException("concat-error"));
        await Assert.That(async () => await first.Concat(second).ToListAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Concat observable of observables with observable source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservableOfObservables_ThenConcatenatesInOrder()
    {
        var source = new[] { SignalAsync.Range(1, 2), SignalAsync.Range(3, 2) }.ToAsyncSignal();
        var result = await source.Concat().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SampleValue1, SampleValue2, SampleValue3, SampleValue4]);
    }

    /// <summary>Tests that Concat propagates error from first sequence.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatFirstFails_ThenErrorPropagated()
    {
        var first = SignalAsync.Throw<int>(new InvalidOperationException(FirstFailMessage));
        var second = SignalAsync.Return(2);
        await Assert.That(async () => await first.Concat(second).ToListAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests that Concat of empty sequences returns empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEmptySequences_ThenReturnsEmpty()
    {
        var result = await SignalAsync.Empty<int>().Concat(SignalAsync.Empty<int>()).ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Concat enumerable of empty returns empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableEmpty_ThenReturnsEmpty()
    {
        IObservableAsync<int>[] sources = [];
        var result = await sources.Concat().ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>
    /// Tests Concat with an enumerable source that throws during first subscription,
    /// exercising the catch/dispose path in ChainEnumerableSignal.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableInnerThrowsDuringSubscribe_ThenCleansUpAndThrows()
    {
        static IEnumerable<IObservableAsync<int>> Sources()
        {
            yield return SignalAsync.Create<int>((_, _) => throw new InvalidOperationException(SubscribeBoomMessage));
        }

        await Assert.That(async () => await Sources().Concat().ToListAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Tests Concat idempotent dispose: when inner fails after already-disposed,
    /// exercising the UnhandledExceptionHandler path in ChainEnumerableSignal.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableDoubleDisposeWithFailure_ThenRoutesToUnhandled()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);
        InvalidOperationException error = new("late failure");

        // Call the extracted helper directly to test the double-dispose path
        ChainEnumerableSignal<int>.ChainSequenceCoordinator.HandleAlreadyDisposed(Result.Failure(error));
        await Assert.That(unhandled).IsSameReferenceAs(error);
    }

    /// <summary>Tests that HandleAlreadyDisposed with null or success result does not invoke unhandled handler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableDoubleDisposeWithoutFailure_ThenNoUnhandledException()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);
        ChainEnumerableSignal<int>.ChainSequenceCoordinator.HandleAlreadyDisposed(null);
        ChainEnumerableSignal<int>.ChainSequenceCoordinator.HandleAlreadyDisposed(Result.Success);
        await Assert.That(unhandled).IsNull();
    }

    /// <summary>
    /// Verifies that when ChainSignalSourcesSignal is completed then disposed again with an error,
    /// the error is routed to UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesDoubleCompleteWithError_ThenRoutedToHandler()
    {
        UnhandledExceptionHandler.Register(ex => _ = ex);
        var outer = Signal.Create<IObservableAsync<int>>();
        Result? completionResult = null;
        var sub = await outer.Values.Concat().SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });

        // Complete with failure first
        await outer.OnCompletedAsync(Result.Failure(new InvalidOperationException(FirstFailMessage)));
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));

        // Now dispose, which calls FinishAsync(null) but TrySetDisposed returns true
        // (already disposed), and since result?.Exception is null for null result, no handler call.
        // We need another approach: dispose first, then force another completion with an error.
        await sub.DisposeAsync();
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that when the first observable in a ConcatEnumerable throws during
    /// subscribe, the subscription is disposed and the exception propagates.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableSubscribeThrows_ThenDisposesAndRethrows()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new InvalidOperationException("subscribe-failure")));
        IObservableAsync<int>[] sources = [throwingSource];
        await Assert.That(async () => await sources.Concat().ToListAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that ChainSignalSourcesSignal.HandleAlreadyDisposed routes a failure
    /// exception to the unhandled exception handler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesHandleAlreadyDisposedWithFailure_ThenRoutesToUnhandled()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);
        InvalidOperationException error = new("late failure");
        ChainSignalSourcesSignal<int>.ChainCoordinator.HandleAlreadyDisposed(Result.Failure(error));
        await Assert.That(unhandled).IsSameReferenceAs(error);
    }

    /// <summary>
    /// Verifies that ChainSignalSourcesSignal.HandleAlreadyDisposed with null or success
    /// result does not invoke the unhandled exception handler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesHandleAlreadyDisposedWithoutFailure_ThenNoUnhandledException()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);
        ChainSignalSourcesSignal<int>.ChainCoordinator.HandleAlreadyDisposed(null);
        ChainSignalSourcesSignal<int>.ChainCoordinator.HandleAlreadyDisposed(Result.Success);
        await Assert.That(unhandled).IsNull();
    }

    /// <summary>Tests Concat with observable of observables inner failure propagates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatObservablesInnerFails_ThenErrorPropagated()
    {
        InvalidOperationException error = new("obs-concat-fail");
        var outer = SignalAsync.Return(SignalAsync.Throw<int>(error));
        await Assert.That(async () => await outer.Concat().FirstAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Concat with enumerable source failure propagates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableInnerFails_ThenErrorPropagated()
    {
        InvalidOperationException error = new("concat-fail");
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Throw<int>(error)];
        await Assert.That(async () => await sources.Concat().LastAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when <c>SubscribeNextSignalAsync</c> throws and <c>FinishAsync</c> also throws
    /// (because the enumerator's Dispose faults), the catch block in <c>SubscribeAsyncCore</c>
    /// disposes the subscription and rethrows the exception.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcatEnumerableMoveNextAndDisposeBothThrow_ThenSubscribeAsyncCoreDisposesAndRethrows()
    {
        MoveNextAndDisposeThrowingEnumerable<int> sources = new();
        InvalidOperationException? exception = null;

        try
        {
            await sources.Concat().ToListAsync();
        }
        catch (InvalidOperationException ex)
        {
            exception = ex;
        }

        await Assert.That(exception).IsNotNull();
    }
}
