// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>TakeUntil operator tests — disposal, cancellation, forwarding errors, and integration scenarios.</summary>
public partial class TakeUntilOperatorTests
{
    /// <summary>Tests TakeUntil(predicate) DisposeAsyncCore when subscription is not null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalDisposed_ThenSubscriptionIsDisposed()
    {
        var source = Signal.Create<int>();
        List<int> items = [];
        var sub = await source.Values.TakeUntil(x => x > 100).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();

        // After dispose, emitting should not reach observer
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests TakeUntil(CancellationToken) where the token is cancelled
    /// and the observer's CompleteFromCancellation catch path is exercised.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalCanceled_ThenCompletionForwarded()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await source.OnNextAsync(1, CancellationToken.None);
        await cts.CancelAsync();
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult is not null, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Tests TakeUntil(CompletionSignalDelegate) where the delegate's disposable
    /// throws during cleanup, exercising the inner catch block.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateDisposableThrows_ThenCaughtGracefully()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(stop =>
        {
            _ = Task.Run(async () =>
            {
                await Task.Yield();
                stop(Result.Success);
            });
            return DisposableAsync.Create(() => throw new InvalidOperationException("dispose fail"));
        }).SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await source.OnNextAsync(1, CancellationToken.None);
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult is not null, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that TakeUntil stops on observable signal.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservable_ThenStopsOnSignal()
    {
        var source = Signal.Create<int>();
        var stopper = Signal.Create<string>();
        List<int> items = [];
        await using var sub = await source.Values.TakeUntil(stopper.Values).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await stopper.OnNextAsync("stop", CancellationToken.None);
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(100));
        await source.OnNextAsync(ThirdItem, CancellationToken.None);
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(50));
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(ThirdItem);
    }

    /// <summary>Tests that TakeUntil stops on task completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignal_ThenStopsOnTaskCompletion()
    {
        TaskCompletionSource tcs = new();
        var source = Signal.Create<int>();
        List<int> items = [];
        await using var sub = await source.Values.TakeUntil(tcs.Task).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        tcs.SetResult();
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(100));
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(50));
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SecondItem);
    }

    /// <summary>Tests that TakeUntil stops on cancellation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignal_ThenStopsOnCancellation()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        List<int> items = [];
        await using var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await cts.CancelAsync();
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(100));
        await Assert.That(items).Contains(1);
    }

    /// <summary>Tests that TakeUntil with predicate stops when predicate returns true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignal_ThenStopsWhenPredicateTrue()
    {
        var result = await SignalAsync.Range(1, 10).TakeUntil(x => x > 3).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem]);
    }

    /// <summary>Tests that TakeUntil with async predicate stops when predicate returns true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicate_ThenStopsWhenPredicateTrue()
    {
        var result = await SignalAsync.Range(1, 10).TakeUntil(async (x, _) =>
        {
            await Task.Yield();
            return x > 2;
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem]);
    }

    /// <summary>Tests that TakeUntil throws on null predicate.</summary>
    [Test]
    public void WhenTakeUntilNullPredicate_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() => SignalAsync.Return(1).TakeUntil((Func<int, bool>)null!));

    /// <summary>Tests that TakeUntil throws on null async predicate.</summary>
    [Test]
    public void WhenTakeUntilNullAsyncPredicate_ThenThrowsArgumentNull() => Assert.Throws<ArgumentNullException>(() =>
        SignalAsync.Return(1).TakeUntil((Func<int, CancellationToken, ValueTask<bool>>)null!));

    /// <summary>Verifies that TakeUntil(CompletionSignalDelegate) forwards error resume when SourceFailsWhenOtherFails is false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateOtherFailsAndOptionFalse_ThenErrorResumeForwarded()
    {
        var source = Signal.Create<int>();
        List<Exception> errors = [];
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(
            notifyStop =>
            {
                // Fire stop with failure after a brief moment
                _ = Task.Run(() => notifyStop(Result.Failure(new InvalidOperationException("delegate fail"))));
                return DisposableAsync.Empty;
            },
            new TakeUntilOptions { SourceFailsWhenOtherFails = false }).SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                lock (_gate)
                {
                    errors.Add(ex);
                }

                return default;
            },
            result =>
            {
                completionResult = result;
                return default;
            });
        await AsyncTestHelpers.WaitForConditionAsync(() => errors.Count >= 1, TimeSpan.FromSeconds(5));
        await Assert.That(errors).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that TakeUntil(CompletionSignalDelegate) forwards failure when SourceFailsWhenOtherFails is true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateOtherFailsAndOptionTrue_ThenCompletesWithFailure()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(
            notifyStop =>
            {
                _ = Task.Run(() => notifyStop(Result.Failure(new InvalidOperationException("delegate fail"))));
                return DisposableAsync.Empty;
            },
            new TakeUntilOptions { SourceFailsWhenOtherFails = true }).SubscribeAsync((_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that TakeUntil(Task) forwards error resume when task fails and SourceFailsWhenOtherFails is false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalAwaitStopThenCompleteFailsOptionFalse_ThenErrorResumeForwarded()
    {
        var source = Signal.Create<int>();
        List<Exception> errors = [];
        var failingTask = Task.FromException(new InvalidOperationException("task fail"));
        await using var sub = await source.Values
            .TakeUntil(failingTask, new TakeUntilOptions { SourceFailsWhenOtherFails = false }).SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });
        await AsyncTestHelpers.WaitForConditionAsync(() => errors.Count >= 1, TimeSpan.FromSeconds(5));
        await Assert.That(errors).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that TakeUntil(Task) forwards failure when task fails and SourceFailsWhenOtherFails is true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalAwaitStopThenCompleteFailsOptionTrue_ThenCompletesWithFailure()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;
        var failingTask = Task.FromException(new InvalidOperationException("task fail"));
        await using var sub = await source.Values
            .TakeUntil(failingTask, new TakeUntilOptions { SourceFailsWhenOtherFails = true }).SubscribeAsync(
            (_, _) => default,
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

    /// <summary>Verifies that TakeUntil(Task) SourceWitness forwards error resume and completion through parent.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalSourceEmitsErrorResume_ThenErrorIsForwarded()
    {
        List<Exception> errors = [];
        var neverTask = new TaskCompletionSource().Task;
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        Result? completionResult = null;
        await using var sub = await source.TakeUntil(neverTask).SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                lock (_gate)
                {
                    errors.Add(ex);
                }

                return default;
            },
            result =>
            {
                completionResult = result;
                return default;
            });
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(errors).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(completionResult).IsNotNull();
    }

    /// <summary>Verifies that TakeUntil(CompletionSignalDelegate) disposes during wait.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateDisposedDuringWait_ThenCancelsCleanly()
    {
        var source = Signal.Create<int>();
        var sub = await source.Values
            .TakeUntil(_ => DisposableAsync.Empty, new TakeUntilOptions { SourceFailsWhenOtherFails = false })
            .SubscribeAsync((_, _) => default, null);
        await source.OnNextAsync(1, CancellationToken.None);

        // Dispose while AwaitStopThenComplete is still waiting for the signal
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the stop signal fires with an error and SourceFailsWhenOtherFails is false,
    /// the error is forwarded as an error-resume and the disposable's exception is swallowed.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateSignalFailsAndOptionFalse_ThenSendsErrorResume()
    {
        var source = Signal.Create<int>();
        List<Exception> errors = [];
        Action<Result>? storedNotifyStop = null;
        CompletionSignalDelegate signalDelegate = notifyStop =>
        {
            storedNotifyStop = notifyStop;
            return DisposableAsync.Create(() =>
            {
                // This dispose throws to exercise the catch block
                throw new InvalidOperationException("dispose boom");
            });
        };
        await using var sub = await source.Values.TakeUntil(signalDelegate).SubscribeAsync((_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await source.OnNextAsync(1, CancellationToken.None);

        // Fire the stop signal with a failure
        storedNotifyStop!(Result.Failure(new InvalidOperationException("signal error")));
        await AsyncTestHelpers.WaitForConditionAsync(() => errors.Count >= 1, TimeSpan.FromSeconds(5));
        await Assert.That(errors).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>
    /// Verifies that when the stop signal fires with an error and SourceFailsWhenOtherFails is true,
    /// the error is forwarded as completion failure and the disposable's exception is swallowed.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateSignalFailsAndOptionTrue_ThenCompletesWithFailure()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;
        Action<Result>? storedNotifyStop = null;
        CompletionSignalDelegate signalDelegate = notifyStop =>
        {
            storedNotifyStop = notifyStop;
            return DisposableAsync.Create(() => throw new InvalidOperationException("dispose boom"));
        };
        await using var sub = await source.Values
            .TakeUntil(signalDelegate, new TakeUntilOptions { SourceFailsWhenOtherFails = true }).SubscribeAsync(
            (_, _) => default,
            null,
            result =>
                {
                    completionResult = result;
                    return default;
                });
        storedNotifyStop!(Result.Failure(new InvalidOperationException("signal error")));
        await AsyncTestHelpers.WaitForConditionAsync(() => completionResult.HasValue, TimeSpan.FromSeconds(5));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that when the delegate stop signal fires and forwarding completion throws,
    /// the outer catch block swallows the exception.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateForwardingThrows_ThenOuterCatchSwallows()
    {
        Action<Result>? storedNotifyStop = null;
        CompletionSignalDelegate signalDelegate = notifyStop =>
        {
            storedNotifyStop = notifyStop;
            return DisposableAsync.Empty;
        };

        // Use a source that never completes, with an observer that throws on completion
        var source = Signal.Create<int>();
        var sub = await source.Values.TakeUntil(signalDelegate).SubscribeAsync(
            (_, _) => default,
            null,
            _ => throw new InvalidOperationException("observer completion throws"));

        // Fire the stop signal with success; OnCompletedAsync will throw because observer throws
        storedNotifyStop!(Result.Success);

        // The outer catch block should swallow the exception; no crash
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(200));
        await sub.DisposeAsync();
    }

    /// <summary>Verifies that when TakeUntil(Task) forwarding completion throws, the outer catch block swallows the exception.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalForwardingThrows_ThenOuterCatchSwallows()
    {
        TaskCompletionSource tcs = new();
        var source = Signal.Create<int>();
        var sub = await source.Values.TakeUntil(tcs.Task).SubscribeAsync(
            (_, _) => default,
            null,
            _ => throw new InvalidOperationException("observer completion throws"));

        // Complete the task; OnCompletedAsync will throw because observer throws
        tcs.SetResult();

        // The outer catch block should swallow the exception; no crash
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(200));
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when TakeUntil(CancellationToken) forwards completion and the observer throws,
    /// the outer catch block in CompleteFromCancellation swallows the exception.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalForwardingThrows_ThenOuterCatchSwallows()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(
            (_, _) => default,
            null,
            _ => throw new InvalidOperationException("observer completion throws"));

        // Cancel the token; CompleteFromCancellation will call OnCompletedAsync which will throw
        await cts.CancelAsync();

        // The outer catch block should swallow the exception; no crash
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(200));
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when TakeUntil(CompletionSignalDelegate) with SourceFailsWhenOtherFails=false
    /// signals an error, OnErrorResumeAsync is called. If that also throws, the outer catch swallows it.
    /// Covers the outermost catch in AwaitStopThenComplete for CompletionSignalDelegate.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateErrorResumeThrows_ThenOuterCatchSwallows()
    {
        Action<Result>? storedNotifyStop = null;
        var source = Signal.Create<int>();
        CompletionSignalDelegate stopSignal = notifyStop =>
        {
            storedNotifyStop = notifyStop;
            return DisposableAsync.Create(() => default);
        };
        var sub = await source.Values.TakeUntil(stopSignal, new TakeUntilOptions { SourceFailsWhenOtherFails = false })
            .SubscribeAsync(
            (_, _) => default,
            (_, _) => throw new InvalidOperationException("error resume throws"),
            _ => throw new InvalidOperationException("completion throws"));

        // Signal a failure; SourceFailsWhenOtherFails=false so OnErrorResumeAsync is called, which throws
        storedNotifyStop!(Result.Failure(new InvalidOperationException("stop error")));

        // The outer catch block should swallow the exception
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(200));
        await sub.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when TakeUntil(Task) with SourceFailsWhenOtherFails=false
    /// the task faults and OnErrorResumeAsync throws, the outer catch swallows it.
    /// Covers the outermost catch in AwaitStopThenComplete for Task.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalErrorResumeThrows_ThenOuterCatchSwallows()
    {
        TaskCompletionSource tcs = new();
        var source = Signal.Create<int>();
        var sub = await source.Values.TakeUntil(tcs.Task, new TakeUntilOptions { SourceFailsWhenOtherFails = false })
            .SubscribeAsync(
            (_, _) => default,
            (_, _) => throw new InvalidOperationException("error resume throws"),
            _ => throw new InvalidOperationException("completion throws"));

        // Fault the task; SourceFailsWhenOtherFails=false so OnErrorResumeAsync is called, which throws
        tcs.SetException(new InvalidOperationException("task error"));

        // The outer catch block should swallow the exception
        await AsyncTestHelpers.WaitForConditionAsync(() => true, TimeSpan.FromMilliseconds(200));
        await sub.DisposeAsync();
    }

    /// <summary>Tests TakeUntil with sync predicate stops when predicate is true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilSyncPredicate_ThenStopsWhenTrue()
    {
        var result = await SignalAsync.Range(1, 10).TakeUntil(x => x >= 3).ToListAsync();
        await Assert.That(result).Contains(1);
        await Assert.That(result).Contains(SecondItem);
        await Assert.That(result).DoesNotContain(FourthItem);
    }

    /// <summary>Tests TakeUntil with async predicate stops when predicate is true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicate_ThenStopsWhenTrue()
    {
        var result = await SignalAsync.Range(1, 10).TakeUntil((x, _) => new(x >= 3)).ToListAsync();
        await Assert.That(result).Contains(1);
        await Assert.That(result).Contains(SecondItem);
        await Assert.That(result).DoesNotContain(FourthItem);
    }

    /// <summary>Tests TakeUntil with CancellationToken completes when token fires.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignal_ThenCompletesOnCancel()
    {
        using CancellationTokenSource cts = new();
        DirectSource<int> source = new();
        List<int> items = [];
        TaskCompletionSource completed = new();
        await using var sub = await source.TakeUntil(cts.Token).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await source.EmitNext(1);
        await cts.CancelAsync();
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(items).Contains(1);
    }

    /// <summary>Tests TakeUntil with Task completes when task finishes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalCompletes_ThenSourceCompletes()
    {
        TaskCompletionSource tcs = new();
        DirectSource<int> source = new();
        List<int> items = [];
        TaskCompletionSource completed = new();
        await using var sub = await source.TakeUntil(tcs.Task).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await source.EmitNext(1);
        tcs.SetResult();
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(items).Contains(1);
    }
}
