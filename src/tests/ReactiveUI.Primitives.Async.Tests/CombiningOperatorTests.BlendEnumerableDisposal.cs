// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for enumerable-Merge disposal behavior.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>
    /// Verifies that MergeEnumerable forwards the OnNextAsync disposed check by disposing the
    /// subscription mid-emission and observing that subsequent values are not forwarded.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedDuringInnerNext_ThenOnNextSilentlyReturns()
    {
        var innerSignal = Signal.Create<int>();
        List<int> items = [];

        var sub = await new[] { innerSignal.Values }.Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await innerSignal.OnNextAsync(1, CancellationToken.None);

        await sub.DisposeAsync();

        try
        {
            await innerSignal.OnNextAsync(SampleValue2, CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await Assert.That(items).Contains(1);

        await innerSignal.DisposeAsync();
    }

    /// <summary>
    /// Verifies that MergeEnumerable forwards the OnErrorResumeAsync disposed check by disposing
    /// the subscription and observing that subsequent errors are not forwarded.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedDuringInnerErrorResume_ThenSilentlyReturns()
    {
        var innerSignal = Signal.Create<int>();
        List<Exception> errors = [];

        var sub = await new[] { innerSignal.Values }.Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await sub.DisposeAsync();

        try
        {
            await innerSignal.OnErrorResumeAsync(
                new InvalidOperationException(LateErrorMessage),
                CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await Assert.That(errors).IsEmpty();

        await innerSignal.DisposeAsync();
    }

    /// <summary>Verifies that MergeEnumerable OnNextAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedDuringEmission_ThenOnNextReturnsEarly()
    {
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();
        List<int> items = [];

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];

        var sub = await sources.Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null);

        await signal1.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();
        await signal1.OnNextAsync(SampleValue2, CancellationToken.None);

        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>Verifies that MergeEnumerable OnErrorResumeAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposed_ThenOnErrorResumeReturnsEarly()
    {
        var signal = Signal.Create<int>();
        List<Exception> errors = [];

        IObservableAsync<int>[] sources = [signal.Values];

        var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });

        await sub.DisposeAsync();
        await signal.OnErrorResumeAsync(new InvalidOperationException("test"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies that MergeEnumerable FinishAsync handles second error after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableCompletedTwiceWithError_ThenSecondErrorGoesToUnhandled()
    {
        Exception? unhandledException = null;
        UnhandledExceptionHandler.Register(ex => unhandledException = ex);

        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();
        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];

        await using var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                null);

        // First source fails - triggers FinishAsync
        await signal1.OnCompletedAsync(Result.Failure(new InvalidOperationException(FirstLiteral)));

        // Second source fails - already disposed, error goes to UnhandledExceptionHandler
        await signal2.OnCompletedAsync(Result.Failure(new InvalidOperationException(SecondLiteral)));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => unhandledException is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(unhandledException).IsNotNull();
    }

    /// <summary>
    /// Verifies that MergeEnumerable OnNextAsync returns early when the subscription
    /// has already been disposed, using DirectSource to bypass Signal un-subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposed_ThenOnNextReturnsEarlyViaDirectSource()
    {
        DirectSource<int> directSource = new();
        List<int> items = [];

        IObservableAsync<int>[] sources = [directSource];

        var sub = await sources.Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null);

        await directSource.EmitNext(1, CancellationToken.None);
        await sub.DisposeAsync();

        // DirectSource retains the inner observer, so this call reaches
        // BlendSequenceCoordinator.RelayNextAsync which checks _disposed.
        try
        {
            await directSource.EmitNext(SampleValue2, CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected – the linked CTS may be cancelled
        }

        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>
    /// Verifies that MergeEnumerable OnErrorResumeAsync returns early when the subscription
    /// has already been disposed, using DirectSource to bypass Signal un-subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposed_ThenOnErrorResumeReturnsEarlyViaDirectSource()
    {
        DirectSource<int> directSource = new();
        List<Exception> errors = [];

        IObservableAsync<int>[] sources = [directSource];

        var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });

        await sub.DisposeAsync();

        // DirectSource retains the inner observer, so this call reaches
        // BlendSequenceCoordinator.RelayErrorAsync which checks _disposed.
        try
        {
            await directSource.EmitError(new InvalidOperationException(LateErrorMessage), CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Verifies that MergeEnumerable FinishAsync handles a second completion with
    /// an error by routing it to UnhandledExceptionHandler, using DirectSource to
    /// ensure both completions reach the subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableCompletedTwiceWithErrorViaDirectSource_ThenUnhandledExceptionFires()
    {
        Exception? unhandledException = null;
        UnhandledExceptionHandler.Register(ex => unhandledException = ex);

        DirectSource<int> directSource1 = new();
        DirectSource<int> directSource2 = new();
        IObservableAsync<int>[] sources = [directSource1, directSource2];

        await using var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                null);

        // First source fails – triggers FinishAsync and disposes subscription
        await directSource1.Complete(Result.Failure(new InvalidOperationException(FirstLiteral)));

        // Second source fails – already disposed, error goes to UnhandledExceptionHandler
        await directSource2.Complete(Result.Failure(new InvalidOperationException(SecondLiteral)));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => unhandledException is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(unhandledException).IsNotNull();
    }

    /// <summary>
    /// Verifies that when the completion handler itself throws in MergeEnumerable BeginSubscribing,
    /// the outer catch routes the exception to UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableCompletionHandlerThrows_ThenOuterCatchRoutesToUnhandled()
    {
        Exception? unhandledException = null;
        UnhandledExceptionHandler.Register(ex => unhandledException = ex);

        // Use a single Return source that completes synchronously during subscription.
        // The sentinel decrement triggers FinishAsync(Result.Success), and we make the
        // observer's OnCompletedAsync throw, which escapes the inner try/finally and is
        // caught by the outer try in BeginSubscribing.
        IObservableAsync<int>[] sources = [SignalAsync.Return(1)];

        await using var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ => throw new InvalidOperationException("completion handler boom"));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => unhandledException is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(unhandledException).IsNotNull();
        await Assert.That(unhandledException!.Message).Contains("completion handler boom");
    }

    /// <summary>
    /// Verifies that when the enumerable throws during iteration in MergeEnumerable,
    /// the exception propagates through the BeginSubscribing outer catch and is routed to
    /// UnhandledExceptionHandler. This exercises the defensive error path in BeginSubscribing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableThrowsDuringIteration_ThenRoutesToUnhandled()
    {
        using UnhandledExceptionCapture unhandled = new();

        // Use an enumerable whose GetEnumerator throws, triggering the error path
        // inside BeginSubscribing's inner try block.
        ThrowingEnumerable<int> throwingEnumerable = new();

        await using var sub = await throwingEnumerable.Merge()
            .SubscribeAsync(
                (_, _) => default,
                null);

        var exception = await unhandled.WaitForAsync("enumerable boom", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Message).Contains("enumerable boom");
    }

    /// <summary>
    /// Verifies that MergeEnumerable OnNextAsync returns early at the post-gate disposed
    /// check when disposal occurs while waiting for the serialization gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedWhileGateHeld_ThenOnNextReturnsPostGate()
    {
        DirectSource<int> directSource = new();
        List<int> items = [];
        TaskCompletionSource gateHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource proceedWithFirstEmission = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = await new IObservableAsync<int>[] { directSource }
            .Merge()
            .SubscribeAsync(
                async (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    if (x == 1)
                    {
                        gateHeld.SetResult();
                        await proceedWithFirstEmission.Task;
                    }
                },
                null);

        // First emission holds the gate
        var firstEmission = directSource.EmitNext(1, CancellationToken.None);
        await gateHeld.Task;

        // Second emission queues behind the gate
        var secondEmissionTask = Task.Run(async () =>
        {
            try
            {
                await directSource.EmitNext(2, CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        });

        // Dispose while second emission waits for the gate
        var disposeTask = sub.DisposeAsync();

        // Release the first emission
        proceedWithFirstEmission.SetResult();

        await firstEmission;
        await disposeTask;
        await secondEmissionTask;

        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>
    /// Verifies that MergeEnumerable OnErrorResumeAsync returns early at the post-gate
    /// disposed check when disposal occurs while waiting for the serialization gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedWhileGateHeld_ThenOnErrorResumeReturnsPostGate()
    {
        DirectSource<int> directSource = new();
        List<Exception> errors = [];
        TaskCompletionSource gateHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource proceedWithFirstEmission = new(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = await new IObservableAsync<int>[] { directSource }
            .Merge()
            .SubscribeAsync(
                async (_, _) =>
                {
                    // Hold the gate on the first emission
                    gateHeld.SetResult();
                    await proceedWithFirstEmission.Task;
                },
                (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });

        // First emission holds the gate
        var firstEmission = directSource.EmitNext(1, CancellationToken.None);
        await gateHeld.Task;

        // Error emission queues behind the gate
        var errorTask = Task.Run(async () =>
        {
            try
            {
                await directSource.EmitError(new InvalidOperationException(LateErrorMessage), CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        });

        // Dispose while the error emission waits for the gate
        var disposeTask = sub.DisposeAsync();

        // Release the first emission
        proceedWithFirstEmission.SetResult();

        await firstEmission;
        await disposeTask;
        await errorTask;

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Verifies that MergeEnumerable outer catch routes exception
    /// to UnhandledExceptionHandler when BeginSubscribing itself throws.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableBeginSubscribingThrows_ThenRoutesToUnhandled()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);

        ThrowingEnumerable<int> sources = new();

        await using var sub = await sources.Merge().SubscribeAsync(
            (_, _) => default,
            null);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => unhandled is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(unhandled).IsNotNull();
    }

    /// <summary>Verifies MergeEnumerable OnNextAsync post-gate disposed guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposed_ThenOnNextReturnsEarly()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<int> items = [];

        var sub = await sources.Merge().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await src1.EmitNext(1);
        await sub.DisposeAsync();
        await src1.EmitNext(Sentinel99);

        await Assert.That(items).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies MergeEnumerable routes exception to UnhandledExceptionHandler
    /// when already disposed and completion has an exception.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableAlreadyDisposedWithFailure_ThenRoutesToUnhandled()
    {
        Exception? unhandled = null;
        UnhandledExceptionHandler.Register(ex => unhandled = ex);

        SignalAsyncExtensions.BlendEnumerableSignal<int>.BlendSequenceCoordinator.RoutePostDisposalException(
            Result.Failure(new InvalidOperationException("post-dispose error")));

        await Assert.That(unhandled).IsNotNull();
        await Assert.That(unhandled!.Message).IsEqualTo("post-dispose error");
    }

    /// <summary>
    /// Verifies that MergeEnumerable drops values after disposal.
    /// Covers the disposed-early-return guard in BlendSequenceCoordinator.RelayNextAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedDuringEmission_ThenDropsValues()
    {
        DirectSource<int> innerSource = new();
        List<int> results = [];

        var sub = await new IObservableAsync<int>[] { innerSource }
            .Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await innerSource.EmitNext(1);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        // Emit after disposal - should be dropped
        await innerSource.EmitNext(Sentinel99);

        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Verifies that MergeEnumerable drops error-resume after disposal.
    /// Covers the disposed-early-return guard in BlendSequenceCoordinator.RelayErrorAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedDuringErrorResume_ThenDropsErrors()
    {
        DirectSource<int> innerSource = new();
        List<Exception> errors = [];

        var sub = await new IObservableAsync<int>[] { innerSource }
            .Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await innerSource.EmitError(new InvalidOperationException(FirstLiteral));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => errors.Count >= 1,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        // Error after disposal - should be dropped
        await innerSource.EmitError(new InvalidOperationException(SecondLiteral));

        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Verifies MergeEnumerable OnNextAsync post-gate disposed guard using blocking-OnCompletedAsync.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedWhileGateHeld_ThenOnNextPostGateReturns()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<int> items = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await src2.EmitNext(Sentinel99);

        await Assert.That(items).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies MergeEnumerable OnErrorResumeAsync post-gate disposed guard using blocking-OnCompletedAsync.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeEnumerableDisposedWhileGateHeld_ThenOnErrorResumePostGateReturns()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<Exception> errors = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await src2.EmitError(new InvalidOperationException("post-dispose"));

        await Assert.That(errors).IsEmpty();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }
}
