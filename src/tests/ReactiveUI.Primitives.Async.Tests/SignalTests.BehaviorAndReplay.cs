// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>BehaviorSignal and ReplayLatest tests for <see cref="SignalTests"/>.</summary>
[System.Diagnostics.DebuggerDisplay("WaitTimeout = {WaitTimeout}")]
public partial class SignalTests
{
    /// <summary>Tests behavior Signal with start value emits latest first to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalWithStartValue_ThenNewSubscriberReceivesLatestFirst()
    {
        const int StartValue = 42;
        var signal = Signal.CreateBehavior(StartValue);
        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests concurrent behavior Signal emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalConcurrent_ThenNewSubscriberReceivesLatest()
    {
        BehaviorSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        const int StartValue = 100;
        var signal = Signal.CreateBehavior(StartValue, options);
        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests replay latest Signal replays last value to late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSignal_ThenLateSubscriberGetsLatestValue()
    {
        const int FirstValue = 10;
        const int LatestValue = 20;
        var signal = Signal.CreateReplayLatest<int>();

        await signal.OnNextAsync(FirstValue, CancellationToken.None);
        await signal.OnNextAsync(LatestValue, CancellationToken.None);

        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(LatestValue);
    }

    /// <summary>Tests concurrent replay latest Signal replays latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSignalConcurrent_ThenLateSubscriberGetsLatest()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 5;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests behavior Signal stateless emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalStateless_ThenEmitsStartValueToNewSubscriber()
    {
        BehaviorSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateBehavior("initial", options);
        List<string> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo("initial");
    }

    /// <summary>Tests replay latest stateless emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestStateless_ThenEmitsLatestToNewSubscriber()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 7;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests concurrent stateless replay latest emits latest.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLatest_ThenEmitsLatest()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 77;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests that OnNextAsync on a serial stateless replay-last Signal replays the value to a late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnNext_ThenLateSubscriberReceivesReplayedValue()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int FirstValue = 42;
        const int SecondValue = 99;

        await signal.OnNextAsync(FirstValue, CancellationToken.None);

        List<int> items = [];
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await signal.OnNextAsync(SecondValue, CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless replay-last Signal delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnErrorResume_ThenObserverReceivesError()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                IgnoredResult.Of(errorTcs.TrySetResult(ex));
                return default;
            });

        InvalidOperationException expected = new("stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateless replay-last Signal delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastOnErrorResume_ThenObserverReceivesError()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                IgnoredResult.Of(errorTcs.TrySetResult(ex));
                return default;
            });

        InvalidOperationException expected = new("concurrent-stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a serial stateless replay-last Signal delivers completion and resets state.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnCompleted_ThenObserverReceivesCompletionAndStateResets()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Result> resultTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                _ = resultTcs.TrySetResult(result);
                return default;
            });

        const int PushedValue = 10;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();

        // After completion, a new subscriber should NOT receive a replayed value since state was reset.
        List<int> lateItems = [];
        await using var lateSub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lateItems.Add(x);
                return default;
            },
            null);

        await Assert.That(lateItems).Count().IsEqualTo(0);
    }

    /// <summary>Tests that OnCompletedAsync on a concurrent stateless replay-last Signal delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastOnCompleted_ThenObserverReceivesCompletion()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Result> resultTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                _ = resultTcs.TrySetResult(result);
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that DisposeAsync on a serial stateless replay-last Signal completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastDispose_ThenCompletesSuccessfully()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();
    }

    /// <summary>Tests that DisposeAsync on a concurrent stateless replay-last Signal completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastDispose_ThenCompletesSuccessfully()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.CreateReplayLatest<int>(options);

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();
    }

    /// <summary>Tests concurrent stateless behavior emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessBehavior_ThenEmitsStartValue()
    {
        BehaviorSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        const int StartValue = 55;
        var signal = Signal.CreateBehavior(StartValue, options);
        List<int> items = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                IgnoredResult.Of(firstReceived.TrySetResult());
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(WaitTimeout);

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests that OnNextAsync on a replay-latest Signal is ignored after the Signal has completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnNextAfterCompleted_ThenValueIsIgnored()
    {
        var signal = Signal.CreateReplayLatest<int>();
        List<int> items = [];
        TaskCompletionSource completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                IgnoredResult.Of(completionTcs.TrySetResult());
                return default;
            });

        const int PostCompletionValue = 2;

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await signal.OnNextAsync(PostCompletionValue, CancellationToken.None);

        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a replay-latest Signal delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResume_ThenObserverReceivesError()
    {
        var signal = Signal.CreateReplayLatest<int>();
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                IgnoredResult.Of(errorTcs.TrySetResult(ex));
                return default;
            });

        InvalidOperationException expected = new("replay-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a replay-latest Signal is ignored after completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResumeAfterCompleted_ThenErrorIsIgnored()
    {
        var signal = Signal.CreateReplayLatest<int>();
        List<Exception> errors = [];
        TaskCompletionSource completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            _ =>
            {
                IgnoredResult.Of(completionTcs.TrySetResult());
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await signal.OnErrorResumeAsync(new InvalidOperationException("ignored"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Tests that OnCompletedAsync on a replay-latest Signal delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnCompleted_ThenObserverReceivesCompletion()
    {
        var signal = Signal.CreateReplayLatest<int>();
        TaskCompletionSource<Result> completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                _ = completionTcs.TrySetResult(result);
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);

        var completionResult = await completionTcs.Task;
        await Assert.That(completionResult.IsSuccess).IsTrue();
    }

    /// <summary>Tests that calling OnCompletedAsync twice on a replay-latest Signal ignores the second call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnCompletedCalledTwice_ThenSecondCallIsIgnored()
    {
        var signal = Signal.CreateReplayLatest<int>();
        var completionCount = 0;
        TaskCompletionSource completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(Interlocked.Increment(ref completionCount));
                IgnoredResult.Of(completionTcs.TrySetResult());
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await signal.OnCompletedAsync(Result.Failure(new InvalidOperationException("second")));

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Tests that DisposeAsync on a replay-latest Signal completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestDisposeAsync_ThenCompletesSuccessfully()
    {
        const int PushedValue = 42;
        var signal = Signal.CreateReplayLatest<int>();
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        await signal.DisposeAsync();
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateful replay-latest Signal delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestOnErrorResume_ThenObserverReceivesError()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                IgnoredResult.Of(errorTcs.TrySetResult(ex));
                return default;
            });

        InvalidOperationException expected = new("concurrent-stateful-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a concurrent stateful replay-latest Signal delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestOnCompleted_ThenObserverReceivesCompletion()
    {
        ReplayLatestSignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.CreateReplayLatest<int>(options);
        TaskCompletionSource<Result> resultTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                _ = resultTcs.TrySetResult(result);
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that subscribing to an already-completed replay-latest Signal immediately delivers completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeToCompletedReplayLatest_ThenObserverReceivesImmediateCompletion()
    {
        var signal = Signal.CreateReplayLatest<int>();
        TaskCompletionSource firstCompletionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var firstSub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                IgnoredResult.Of(firstCompletionTcs.TrySetResult());
                return default;
            });

        const int PushedValue = 99;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await firstCompletionTcs.Task;

        Result? lateResult = null;
        TaskCompletionSource lateTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var lateSub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                lateResult = result;
                _ = lateTcs.TrySetResult();
                return default;
            });

        await lateTcs.Task;

        await Assert.That(lateResult).IsNotNull();
        await Assert.That(lateResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that the replay-latest Signal's <c>OnNextAsync</c> with a caller-supplied
    /// cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnNextWithCustomToken_ThenForwardsValue()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false });
        TaskCompletionSource<int> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync((v, _) =>
        {
            IgnoredResult.Of(tcs.TrySetResult(v));
            return default;
        });

        using CancellationTokenSource cts = new();
        const int LinkedCtsValue = 11;
        await signal.OnNextAsync(LinkedCtsValue, cts.Token);

        var received = await tcs.Task.WaitAsync(WaitTimeout);
        await Assert.That(received).IsEqualTo(LinkedCtsValue);
    }

    /// <summary>Verifies that the replay-latest Signal's <c>OnErrorResumeAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResumeWithCustomToken_ThenForwardsError()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false });
        TaskCompletionSource<Exception> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                IgnoredResult.Of(tcs.TrySetResult(ex));
                return default;
            });

        InvalidOperationException expected = new("linked-cts");
        using CancellationTokenSource cts = new();
        await signal.OnErrorResumeAsync(expected, cts.Token);

        var received = await tcs.Task.WaitAsync(WaitTimeout);
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that the stateless replay-latest Signal's <c>OnNextAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestOnNextWithCustomToken_ThenForwardsValue()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Serial, IsStateless = true });
        TaskCompletionSource<int> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync((value, _) =>
        {
            IgnoredResult.Of(tcs.TrySetResult(value));
            return default;
        });

        using CancellationTokenSource cts = new();
        const int LinkedCtsValue = 17;
        await signal.OnNextAsync(LinkedCtsValue, cts.Token);

        var received = await tcs.Task.WaitAsync(WaitTimeout);
        await Assert.That(received).IsEqualTo(LinkedCtsValue);
    }

    /// <summary>Verifies that the stateless replay-latest Signal's <c>OnErrorResumeAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestOnErrorResumeWithCustomToken_ThenForwardsError()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Serial, IsStateless = true });
        TaskCompletionSource<Exception> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (error, _) =>
            {
                IgnoredResult.Of(tcs.TrySetResult(error));
                return default;
            });

        InvalidOperationException expected = new("stateless-linked-cts");
        using CancellationTokenSource cts = new();
        await signal.OnErrorResumeAsync(expected, cts.Token);

        var received = await tcs.Task.WaitAsync(WaitTimeout);
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that replay-latest subscription with a caller-supplied cancellation token
    /// disposes the linked token source after subscribing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubscribeWithCustomToken_ThenSubscriptionCompletes()
    {
        var signal = Signal.CreateReplayLatest<int>();
        using CancellationTokenSource cts = new();

        var sub = await signal.Values.SubscribeAsync(static (_, _) => default, cts.Token);
        await sub.DisposeAsync();

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that stateless replay-latest subscription with a caller-supplied cancellation
    /// token disposes the linked token source after subscribing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestSubscribeWithCustomToken_ThenSubscriptionCompletes()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Serial, IsStateless = true });
        using CancellationTokenSource cts = new();

        var sub = await signal.Values.SubscribeAsync(static (_, _) => default, cts.Token);
        await sub.DisposeAsync();

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that disposing a replay-latest subscription after its owning Signal has
    /// already been disposed is a no-op and remains idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubscriptionDisposedAfterSignalDisposed_ThenDisposeIsIdempotent()
    {
        var signal = Signal.CreateReplayLatest<int>();
        var sub = await signal.Values.SubscribeAsync(static (_, _) => default);

        await signal.DisposeAsync();
        await sub.DisposeAsync();
        await sub.DisposeAsync();

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that disposing a stateless replay-latest subscription after its owning
    /// Signal has already been disposed is a no-op and remains idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestSubscriptionDisposedAfterSignalDisposed_ThenDisposeIsIdempotent()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Serial, IsStateless = true });
        var sub = await signal.Values.SubscribeAsync(static (_, _) => default);

        await signal.DisposeAsync();
        await sub.DisposeAsync();
        await sub.DisposeAsync();

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Exercises the <c>_isDisposed</c> idempotency guard on <c>BaseReplayLatestSignalAsync.DisposeAsync</c> — a second dispose is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSignalDisposedTwice_ThenIdempotent()
    {
        var signal = Signal.CreateReplayLatest<int>();

        await signal.DisposeAsync();
        await signal.DisposeAsync();

        await Assert.That(signal).IsNotNull();
    }

    /// <summary>Exercises the <c>_isDisposed</c> idempotency guard on <c>BaseStatelessReplayLatestSignalAsync.DisposeAsync</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastSignalDisposedTwice_ThenIdempotent()
    {
        var signal = Signal.CreateReplayLatest<int>(new() { PublishingOption = PublishingOption.Serial, IsStateless = true });

        await signal.DisposeAsync();
        await signal.DisposeAsync();

        await Assert.That(signal).IsNotNull();
    }
}
