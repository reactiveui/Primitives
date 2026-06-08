// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>TakeUntil operator tests — CompletionSignalDelegate overload and option behavior.</summary>
public partial class TakeUntilOperatorTests
{
    /// <summary>String literal "subscribe failed" used by multiple tests.</summary>
    private const string SubscribeFailedMessage = "subscribe failed";

    /// <summary>Tests that CompletionSignalDelegate failure signal with SourceFailsWhenOtherFails=true completes with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateFailsAndOptionTrue_ThenCompletesWithFailure()
    {
        var source = Signal.Create<int>();
        Action<Result>? notifyStop = null;
        Result? completionResult = null;

        await using var sub = await source.Values
            .TakeUntil((CompletionSignalDelegate)StopSignal, new TakeUntilOptions { SourceFailsWhenOtherFails = true })
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await source.OnNextAsync(1, CancellationToken.None);

        notifyStop!(Result.Failure(new InvalidOperationException("stop failed")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        IAsyncDisposable StopSignal(Action<Result> notify)
        {
            notifyStop = notify;
            return DisposableAsync.Empty;
        }
    }

    /// <summary>Tests that CompletionSignalDelegate failure signal with default options sends error resume.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateFailsAndOptionFalse_ThenSendsErrorResume()
    {
        var source = Signal.Create<int>();
        Action<Result>? notifyStop = null;
        var errors = new List<Exception>();

        CompletionSignalDelegate stopSignal = notify =>
        {
            notifyStop = notify;
            return DisposableAsync.Empty;
        };

        await using var sub = await source.Values
            .TakeUntil(stopSignal)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await source.OnNextAsync(1, CancellationToken.None);

        notifyStop!(Result.Failure(new InvalidOperationException("stop failed")));

        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that source error resume is forwarded through TakeUntil(CompletionSignalDelegate).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateSourceErrorResume_ThenForwarded()
    {
        var source = Signal.Create<int>();
        var errors = new List<Exception>();

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        await using var sub = await source.Values
            .TakeUntil(stopSignal)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await source.OnErrorResumeAsync(new InvalidOperationException("warning"), CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests that source completion is forwarded through TakeUntil(CompletionSignalDelegate).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateSourceCompletes_ThenCompletionForwarded()
    {
        var source = Signal.Create<int>();
        Result? completionResult = null;

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        await using var sub = await source.Values
            .TakeUntil(stopSignal)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnCompletedAsync(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests disposal of TakeUntil(CompletionSignalDelegate) stops emissions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateDisposed_ThenStopsEmissions()
    {
        var source = Signal.Create<int>();
        var items = new List<int>();

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        var sub = await source.Values
            .TakeUntil(stopSignal)
            .SubscribeAsync(
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

    /// <summary>Tests TakeUntilOptions default has SourceFailsWhenOtherFails false.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilOptionsDefault_ThenSourceFailsWhenOtherFailsIsFalse()
    {
        var options = TakeUntilOptions.Default;

        await Assert.That(options.SourceFailsWhenOtherFails).IsFalse();
    }

    /// <summary>Tests TakeUntilOptions with SourceFailsWhenOtherFails set to true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilOptionsSourceFailsWhenOtherFailsTrue_ThenPropertyIsTrue()
    {
        var options = new TakeUntilOptions { SourceFailsWhenOtherFails = true };

        await Assert.That(options.SourceFailsWhenOtherFails).IsTrue();
    }

    /// <summary>
    /// Verifies that TakeUntil with a predicate disposes the subscription and rethrows when the source throws during subscribe.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(x => x > FifthItem).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with a predicate that becomes true mid-stream stops emitting further elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPredicateStopSignalBecomesTrueMidStream_ThenStopsEmitting()
    {
        var result = await SignalAsync.Range(1, 10)
            .TakeUntil(x => x > 3)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem]);
    }

    /// <summary>
    /// Verifies that TakeUntil with a cancellation token disposes the subscription and rethrows when the source throws during subscribe.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignalSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        using var cts = new CancellationTokenSource();
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(cts.Token).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with a cancellation token stops emission when the token is canceled during active subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilTokenCanceledDuringEmission_ThenEmissionStops()
    {
        using var cts = new CancellationTokenSource();
        var source = Signal.Create<int>();
        var items = new List<int>();
        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await source.Values
            .TakeUntil(cts.Token)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completed.TrySetResult(result);
                    return default;
                });

        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(SecondItem, CancellationToken.None);

        await cts.CancelAsync();

        var completionResult = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SecondItem);
        await Assert.That(completionResult.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that TakeUntil with a CompletionSignalDelegate disposes the subscription and rethrows when the source throws during subscribe.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(stopSignal).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with a Task disposes the subscription and rethrows when the source throws during subscribe.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        var tcs = new TaskCompletionSource();
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(tcs.Task).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with a Task that completes mid-stream stops further emissions and completes with success.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalCompletesMidStream_ThenStopsEmissions()
    {
        var tcs = new TaskCompletionSource();
        var source = Signal.Create<int>();
        var items = new List<int>();
        Result? completionResult = null;

        await using var sub = await source.Values
            .TakeUntil(tcs.Task)
            .SubscribeAsync(
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

        tcs.SetResult();

        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SecondItem);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that TakeUntil with another observable disposes the subscription and rethrows when the source throws during subscribe.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncObservableSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(SignalAsync.Never<string>())
                .SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with another observable that emits an item causes source to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilOtherObservableEmitsItem_ThenSourceCompletes()
    {
        var source = Signal.Create<int>();
        var other = Signal.Create<string>();
        var items = new List<int>();
        Result? completionResult = null;

        await using var sub = await source.Values
            .TakeUntil(other.Values)
            .SubscribeAsync(
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
        await other.OnNextAsync("stop", CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([1, SecondItem]);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that TakeUntil with an async predicate disposes the subscription and rethrows when the source throws during subscribe.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateSourceThrowsOnSubscribe_ThenDisposesAndRethrows()
    {
        var throwingSource = SignalAsync.Create<int>((_, _) =>
            throw new InvalidOperationException(SubscribeFailedMessage));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await throwingSource.TakeUntil(async (x, _) =>
            {
                await Task.Yield();
                return x > FifthItem;
            }).SubscribeAsync((_, _) => default, null));
    }

    /// <summary>Verifies that TakeUntil with an async predicate that becomes true mid-stream stops emitting further elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilAsyncPredicateBecomesTrueMidStream_ThenStopsEmitting()
    {
        var result = await SignalAsync.Range(1, 10)
            .TakeUntil(async (x, _) =>
            {
                await Task.Yield();
                return x > 3;
            })
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondItem, ThirdItem]);
    }

    /// <summary>Tests TakeUntil with Task overload stops emitting when task completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignal_ThenStopsWhenTaskCompletes()
    {
        var tcs = new TaskCompletionSource<bool>();
        var signal = Signal.Create<int>();
        var items = new List<int>();

        await using var sub = await signal.Values
            .TakeUntil(tcs.Task)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await signal.OnNextAsync(1, CancellationToken.None);

        tcs.SetResult(true);

        await signal.OnNextAsync(SecondItem, CancellationToken.None);

        await Assert.That(items).Contains(1);
    }

    /// <summary>Tests TakeUntil with CancellationToken stops emitting when token is canceled.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCancellationStopSignal_ThenStopsWhenCanceled()
    {
        using var cts = new CancellationTokenSource();
        var signal = Signal.Create<int>();
        var items = new List<int>();

        await using var sub = await signal.Values
            .TakeUntil(cts.Token)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await signal.OnNextAsync(1, CancellationToken.None);

        await cts.CancelAsync();

        await Assert.That(items).Contains(1);
    }

    /// <summary>Tests TakeUntil with CompletionSignalDelegate stops emitting when delegate signal completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegate_ThenStopsWhenSignalCompletes()
    {
        var signal = Signal.Create<int>();
        var items = new List<int>();

        await using var sub = await signal.Values
            .TakeUntil((CompletionSignalDelegate)CompletionSignal)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await signal.OnNextAsync(1, CancellationToken.None);

        await Assert.That(items).Contains(1);

        IAsyncDisposable CompletionSignal(Action<Result> notifyStop)
        {
            const int Delay = 100;
            _ = Task.Run(async () =>
            {
                await Task.Delay(Delay);
                notifyStop(Result.Success);
            });
            return DisposableAsync.Empty;
        }
    }

    /// <summary>
    /// Tests TakeUntil(CompletionSignalDelegate) where the stop signal fails and option is false,
    /// exercising the error resume path in AwaitStopThenComplete.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateFailsAndOptionFalse_ThenErrorResumeForwarded()
    {
        var source = Signal.Create<int>();
        Exception? errorResumed = null;
        var items = new List<int>();

        await using var sub = await source.Values
            .TakeUntil(
                stop =>
                {
                    // Signal failure after a brief delay
                    _ = Task.Run(async () =>
                    {
                        await Task.Yield();
                        stop(Result.Failure(new InvalidOperationException("signal fail")));
                    });
                    return DisposableAsync.Empty;
                },
                new TakeUntilOptions { SourceFailsWhenOtherFails = false })
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                (ex, _) =>
                {
                    errorResumed = ex;
                    return default;
                });

        await source.OnNextAsync(1, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => errorResumed is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(errorResumed).IsNotNull();
        await Assert.That(errorResumed!.Message).IsEqualTo("signal fail");
    }

    /// <summary>
    /// Tests TakeUntil(Task) where the task fails and option is false,
    /// exercising the error resume path in AwaitStopThenComplete.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskStopSignalFailsAndOptionFalse_ThenErrorResumeForwardedViaAwaitStopThenComplete()
    {
        var source = Signal.Create<int>();
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? errorResumed = null;

        await using var sub = await source.Values
            .TakeUntil(
            tcs.Task,
            new TakeUntilOptions { SourceFailsWhenOtherFails = false }).SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errorResumed = ex;
                    return default;
                });

        tcs.SetException(new InvalidOperationException("task fail"));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => errorResumed is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(errorResumed).IsNotNull();
        await Assert.That(errorResumed!.Message).IsEqualTo("task fail");
    }

    /// <summary>Exercises the <c>TakeUntil(CompletionSignalDelegate, CancellationToken)</c>
    /// overload — the no-options shortcut that forwards to the full overload with null options.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateWithCancellationTokenOverload_ThenForwardsValues()
    {
        var source = Signal.Create<int>();
        var values = new List<int>();

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        await using var sub = await source.Values
            .TakeUntil(stopSignal, CancellationToken.None)
            .SubscribeAsync(
                (x, _) =>
                {
                    values.Add(x);
                    return default;
                });

        const int Sentinel = 17;
        await source.OnNextAsync(Sentinel, CancellationToken.None);

        await Assert.That(values).IsCollectionEqualTo([Sentinel]);
    }

    /// <summary>Exercises the <c>cancellationToken.CanBeCanceled ? ... : ...</c> branch of the
    /// full <c>TakeUntil</c> overload — supplying a cancellable token routes the result through
    /// <c>inner.TakeUntil(cancellationToken)</c>, while <see cref="CancellationToken.None"/>
    /// returns the inner observable unwrapped (already covered by other tests).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilCompletionDelegateWithCancellableToken_ThenLinkedToTokenCancellation()
    {
        using var cts = new CancellationTokenSource();
        var source = Signal.Create<int>();
        var values = new List<int>();
        Result? completionResult = null;

        CompletionSignalDelegate stopSignal = _ => DisposableAsync.Empty;

        await using var sub = await source.Values
            .TakeUntil(stopSignal, options: null, cts.Token)
            .SubscribeAsync(
                (x, _) =>
                {
                    values.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        const int Sentinel = 31;
        await source.OnNextAsync(Sentinel, CancellationToken.None);
        await cts.CancelAsync();

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        await Assert.That(values).IsCollectionEqualTo([Sentinel]);
        await Assert.That(completionResult).IsNotNull();
    }
}
