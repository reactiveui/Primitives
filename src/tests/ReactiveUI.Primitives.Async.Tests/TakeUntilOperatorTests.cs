// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Deep coverage tests for all TakeUntil operator overloads:
/// TakeUntil(observable), TakeUntil(Task), TakeUntil(CancellationToken),
/// TakeUntil(predicate), TakeUntil(asyncPredicate), TakeUntil(CompletionSignalDelegate).
/// </summary>
public partial class TakeUntilOperatorTests
{
    /// <summary>String literal "warning" used by multiple tests.</summary>
    private const string WarningMessage = "warning";

    /// <summary>Second item (2).</summary>
    private const int SecondItem = 2;

    /// <summary>Third item (3).</summary>
    private const int ThirdItem = 3;

    /// <summary>Fourth item (4).</summary>
    private const int FourthItem = 4;

    /// <summary>Fifth item (5).</summary>
    private const int FifthItem = 5;

    /// <summary>A predicate threshold no element of the test sources ever reaches.</summary>
    private const int UnreachableThreshold = 10;

    /// <summary>Maximum time a test waits for a completion signal to arrive.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);
#if NET9_0_OR_GREATER

    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>Tests that TakeUntil(observable) throws on null source.</summary>
    [Test]
    public void WhenTakeUntilObservableNullSource_ThenThrowsArgumentNull()
    {
        const IObservableAsync<int> Source = null!;
        _ = Assert.Throws<ArgumentNullException>(static () => Source.TakeUntil(SignalAsync.Never<string>()));
    }

    /// <summary>Tests that TakeUntil(observable) throws on null other.</summary>
    [Test]
    public void WhenTakeUntilObservableNullOther_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(
            static () => SignalAsync.Return(1).TakeUntil((IObservableAsync<string>)null!));

    /// <summary>Tests that source completing normally passes through to subscriber.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableSourceCompletes_ThenCompletionPassesThrough()
    {
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .TakeUntil(SignalAsync.Never<string>())
            .ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem]);
    }

    /// <summary>Tests that other error with SourceFailsWhenOtherFails=true completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableOtherFailsAndOptionTrue_ThenCompletesWithFailure()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        Result? completionResult = null;
        await using var sub = await source.Values
            .TakeUntil(other.Values, new TakeUntilOptions { SourceFailsWhenOtherFails = true }).SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });
        await source.OnNextAsync(1, CancellationToken.None);
        await other.OnCompletedAsync(Result.Failure(new InvalidOperationException("other failed")));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that other error with SourceFailsWhenOtherFails=false (default) completes with success.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableOtherFailsAndOptionFalse_ThenCompletesWithSuccess()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(other.Values).SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                return default;
            });
        await source.OnNextAsync(1, CancellationToken.None);
        await other.OnCompletedAsync(Result.Failure(new InvalidOperationException("other failed")));
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that other success completion does not trigger source completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableOtherCompletesSuccess_ThenSourceContinues()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        List<int> items = [];
        await using var sub = await source.Values.TakeUntil(other.Values).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            static _ => default);
        await source.OnNextAsync(1, CancellationToken.None);
        await other.OnCompletedAsync(Result.Success);

        // Other completed with success � according to StopSignalObserver.OnCompletedAsyncCore, success returns default (no-op)
        // Source should still be active
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SecondItem);
    }

    /// <summary>Tests that error resume from other is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableOtherErrorResume_ThenForwardedToSubscriber()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        List<Exception> errors = [];
        await using var sub = await source.Values.TakeUntil(other.Values).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await other.OnErrorResumeAsync(new InvalidOperationException(WarningMessage), CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo(WarningMessage);
    }

    /// <summary>Tests that error resume from source is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableSourceErrorResume_ThenForwardedToSubscriber()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        List<Exception> errors = [];
        await using var sub = await source.Values.TakeUntil(other.Values).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await source.OnErrorResumeAsync(new InvalidOperationException("src warning"), CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo("src warning");
    }

    /// <summary>Tests that disposal stops emissions from source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilObservableDisposed_ThenStopsEmissions()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        List<int> items = [];
        var sub = await source.Values.TakeUntil(other.Values).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SecondItem);
    }

    /// <summary>Tests that TakeUntil(Task) with null source throws.</summary>
    [Test]
    public void WhenTaskStopSignalNullSource_ThenThrowsArgumentNull()
    {
        const IObservableAsync<int> Source = null!;
        _ = Assert.Throws<ArgumentNullException>(static () => Source.TakeUntil(Task.CompletedTask));
    }

    /// <summary>Tests that task failure with SourceFailsWhenOtherFails=true completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalFailsAndOptionTrue_ThenCompletesWithFailure()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Signal.Create<int>();
        await using var sub = await source.Values
            .TakeUntil(tcs.Task, new TakeUntilOptions { SourceFailsWhenOtherFails = true }).SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    _ = completed.TrySetResult(result);
                    return default;
                });
        await source.OnNextAsync(1, CancellationToken.None);
        tcs.SetException(new InvalidOperationException("task failed"));
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsFailure).IsTrue();
    }

    /// <summary>Tests that task failure with default options sends error resume instead of failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalFailsAndOptionFalse_ThenSendsErrorResume()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Signal.Create<int>();
        List<Exception> errors = [];
        await using var sub = await source.Values.TakeUntil(tcs.Task).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await source.OnNextAsync(1, CancellationToken.None);
        tcs.SetException(new InvalidOperationException("task failed"));

        // Wait for the error to be relayed rather than assuming the task's continuation ran inline.
        var resumed = await AsyncTestHelpers.WaitForConditionAsync(() => errors.Count == 1, WaitTimeout);
        await Assert.That(resumed).IsTrue();
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that an already-completed task completes the sequence immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAlreadyCompletedTask_ThenCompletesImmediately()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(Task.CompletedTask).SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                return default;
            });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests disposal of TakeUntil(Task) stops emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalDisposed_ThenStopsEmissions()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Signal.Create<int>();
        List<int> items = [];
        var sub = await source.Values.TakeUntil(tcs.Task).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SecondItem);
    }

    /// <summary>Tests that source error resume is forwarded through TakeUntil(Task).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalSourceErrorResume_ThenForwarded()
    {
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Signal.Create<int>();
        List<Exception> errors = [];
        await using var sub = await source.Values.TakeUntil(tcs.Task).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await source.OnErrorResumeAsync(new InvalidOperationException(WarningMessage), CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that already-canceled token completes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAlreadyCanceledToken_ThenCompletesImmediately()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        var source = Signal.Create<int>();
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = completed.TrySetResult(result);
            return default;
        });
        var completionResult = await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(completionResult.IsSuccess).IsTrue();
    }

    /// <summary>Tests that source error resume is forwarded through TakeUntil(CancellationToken).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalSourceErrorResume_ThenForwarded()
    {
        using CancellationTokenSource cts = new();
        await using var source = Signal.Create<int>();
        List<Exception> errors = [];
        await using var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await source.OnErrorResumeAsync(new InvalidOperationException(WarningMessage), CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that source completion is forwarded through TakeUntil(CancellationToken).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalSourceCompletes_ThenCompletionForwarded()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        Result? completionResult = null;
        await using var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await source.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that disposal of TakeUntil(CancellationToken) stops emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalDisposed_ThenStopsEmissions()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        List<int> items = [];
        var sub = await source.Values.TakeUntil(cts.Token).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        await source.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SecondItem);
    }

    /// <summary>Tests that predicate never returning true emits all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalNeverTrue_ThenEmitsAllElements()
    {
        const int SourceValueCount = 5;

        var result = await SignalAsync.Range(1, SourceValueCount).TakeUntil(static _ => false).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem, FourthItem, FifthItem]);
    }

    /// <summary>Tests that predicate returning true on first element emits nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalTrueOnFirst_ThenEmitsNothing()
    {
        const int SourceValueCount = 5;

        var result = await SignalAsync.Range(1, SourceValueCount).TakeUntil(static _ => true).ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that source error resume is forwarded through TakeUntil(predicate).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalSourceErrorResume_ThenForwarded()
    {
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException(WarningMessage), ct);
            await observer.OnNextAsync(SecondItem, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<Exception> errors = [];
        List<int> items = [];
        await using var sub = await source.TakeUntil(static x => x > UnreachableThreshold).SubscribeAsync(
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
        await Assert.That(items).IsCollectionEqualTo([1, SecondItem]);
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that source completion with failure is forwarded through TakeUntil(predicate).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalSourceFails_ThenFailureForwarded()
    {
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("source failed")));
            return DisposableAsync.Empty;
        });
        Result? completionResult = null;
        await using var sub = await source.TakeUntil(static x => x > UnreachableThreshold)
            .SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that async predicate never returning true emits all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateNeverTrue_ThenEmitsAllElements()
    {
        const int SourceValueCount = 5;

        var result = await SignalAsync.Range(1, SourceValueCount).TakeUntil(static async (_, _) =>
        {
            await Task.Yield();
            return false;
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem, FourthItem, FifthItem]);
    }

    /// <summary>Tests that async predicate returning true on first element emits nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateTrueOnFirst_ThenEmitsNothing()
    {
        const int SourceValueCount = 5;

        var result = await SignalAsync.Range(1, SourceValueCount).TakeUntil(static async (_, _) =>
        {
            await Task.Yield();
            return true;
        }).ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that source error resume is forwarded through TakeUntil(asyncPredicate).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateSourceErrorResume_ThenForwarded()
    {
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException(WarningMessage), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<Exception> errors = [];
        await using var sub = await source.TakeUntil(static async (_, _) =>
        {
            await Task.Yield();
            return false;
        }).SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors.Add(ex);
            return default;
        });
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that source failure is forwarded through TakeUntil(asyncPredicate).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateSourceFails_ThenFailureForwarded()
    {
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
            return DisposableAsync.Empty;
        });
        Result? completionResult = null;
        await using var sub = await source.TakeUntil(static async (_, _) =>
        {
            await Task.Yield();
            return false;
        }).SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            return default;
        });
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests that TakeUntil with CompletionSignalDelegate throws on null.</summary>
    [Test]
    public void WhenTakeUntilCompletionDelegateNull_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(
            static () => SignalAsync.Return(1).TakeUntil((CompletionSignalDelegate)null!));

    /// <summary>Tests that CompletionSignalDelegate success signal completes the sequence.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateSuccess_ThenCompletesSequence()
    {
        var source = Signal.Create<int>();
        Action<Result>? notifyStop = null;
        List<int> items = [];
        Result? completionResult = null;
        CompletionSignalDelegate stopSignal = notify =>
        {
            notifyStop = notify;
            return DisposableAsync.Empty;
        };
        await using var sub = await source.Values.TakeUntil(stopSignal).SubscribeAsync(
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
        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(SecondItem, CancellationToken.None);
        notifyStop!(Result.Success);
        await Assert.That(items).Contains(1);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies the two-argument <c>TakeUntil(other, cancellationToken)</c> overload.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilOtherWithCancellationToken_ThenCompletesOnCancellation()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        var other = Signal.Create<int>();
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Values.TakeUntil(other.Values, cts.Token).SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await cts.CancelAsync();
        await completed.Task.WaitAsync(WaitTimeout);
    }

    /// <summary>Verifies the two-argument <c>TakeUntil(task, cancellationToken)</c> overload.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalWithCancellationToken_ThenCompletesOnCancellation()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        TaskCompletionSource taskTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Values.TakeUntil(taskTcs.Task, cts.Token).SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await cts.CancelAsync();
        await completed.Task.WaitAsync(WaitTimeout);
    }

    /// <summary>Verifies the predicate overload with a cancellable token reaches the CT-linked branch.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalWithCancellationToken_ThenCompletesOnCancellation()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Values.TakeUntil(static _ => false, cts.Token).SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await cts.CancelAsync();
        await completed.Task.WaitAsync(WaitTimeout);
    }

    /// <summary>Verifies the async-predicate overload with a cancellable token reaches the CT-linked branch.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateWithCancellationToken_ThenCompletesOnCancellation()
    {
        using CancellationTokenSource cts = new();
        var source = Signal.Create<int>();
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Values.TakeUntil(static (_, _) => new(false), cts.Token).SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(completed.TrySetResult());
                return default;
            });
        await cts.CancelAsync();
        await completed.Task.WaitAsync(WaitTimeout);
    }
}
