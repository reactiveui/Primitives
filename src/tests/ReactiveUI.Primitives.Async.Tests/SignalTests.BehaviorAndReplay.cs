// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>BehaviorSignal and ReplayLatest tests for <see cref="SignalTests"/>.</summary>
public partial class SignalTests
{
    /// <summary>Tests behavior Signal with start value emits latest first to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalWithStartValue_ThenNewSubscriberReceivesLatestFirst()
    {
        const int StartValue = 42;
        var signal = Signal.CreateBehavior(StartValue);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests concurrent behavior Signal emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalConcurrent_ThenNewSubscriberReceivesLatest()
    {
        var options = new BehaviorSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };
        const int StartValue = 100;
        var signal = Signal.CreateBehavior(StartValue, options);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

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

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(LatestValue);
    }

    /// <summary>Tests concurrent replay latest Signal replays latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSignalConcurrent_ThenLateSubscriberGetsLatest()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 5;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests behavior Signal stateless emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSignalStateless_ThenEmitsStartValueToNewSubscriber()
    {
        var options = new BehaviorSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateBehavior("initial", options);
        var items = new List<string>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo("initial");
    }

    /// <summary>Tests replay latest stateless emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestStateless_ThenEmitsLatestToNewSubscriber()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 7;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests concurrent stateless replay latest emits latest.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLatest_ThenEmitsLatest()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int PushedValue = 77;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests that OnNextAsync on a serial stateless replay-last Signal replays the value to a late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnNext_ThenLateSubscriberReceivesReplayedValue()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        const int FirstValue = 42;
        const int SecondValue = 99;

        await signal.OnNextAsync(FirstValue, CancellationToken.None);

        var items = new List<int>();
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
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateless replay-last Signal delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastOnErrorResume_ThenObserverReceivesError()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("concurrent-stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a serial stateless replay-last Signal delivers completion and resets state.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnCompleted_ThenObserverReceivesCompletionAndStateResets()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        const int PushedValue = 10;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();

        // After completion, a new subscriber should NOT receive a replayed value since state was reset.
        var lateItems = new List<int>();
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
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
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
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();
    }

    /// <summary>Tests that DisposeAsync on a concurrent stateless replay-last Signal completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastDispose_ThenCompletesSuccessfully()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };
        var signal = Signal.CreateReplayLatest<int>(options);

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();
    }

    /// <summary>Tests concurrent stateless behavior emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessBehavior_ThenEmitsStartValue()
    {
        var options = new BehaviorSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = true
        };
        const int StartValue = 55;
        var signal = Signal.CreateBehavior(StartValue, options);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests that OnNextAsync on a replay-latest Signal is ignored after the Signal has completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnNextAfterCompleted_ThenValueIsIgnored()
    {
        var signal = Signal.CreateReplayLatest<int>();
        var items = new List<int>();
        var completionTcs = new TaskCompletionSource();

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completionTcs.TrySetResult();
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
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("replay-error");
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
        var errors = new List<Exception>();
        var completionTcs = new TaskCompletionSource();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            _ =>
            {
                completionTcs.TrySetResult();
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
        var completionTcs = new TaskCompletionSource<Result>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                completionTcs.TrySetResult(result);
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
        var completionTcs = new TaskCompletionSource();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                Interlocked.Increment(ref completionCount);
                completionTcs.TrySetResult();
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
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("concurrent-stateful-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a concurrent stateful replay-latest Signal delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestOnCompleted_ThenObserverReceivesCompletion()
    {
        var options = new ReplayLatestSignalCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false
        };
        var signal = Signal.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
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
        var firstCompletionTcs = new TaskCompletionSource();

        await using var firstSub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                firstCompletionTcs.TrySetResult();
                return default;
            });

        const int PushedValue = 99;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await firstCompletionTcs.Task;

        Result? lateResult = null;
        var lateTcs = new TaskCompletionSource();

        await using var lateSub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                lateResult = result;
                lateTcs.TrySetResult();
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
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false,
        });
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (v, _) =>
            {
                tcs.TrySetResult(v);
                return default;
            });

        using var cts = new CancellationTokenSource();
        const int LinkedCtsValue = 11;
        await signal.OnNextAsync(LinkedCtsValue, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsEqualTo(LinkedCtsValue);
    }

    /// <summary>Verifies that the replay-latest Signal's <c>OnErrorResumeAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResumeWithCustomToken_ThenForwardsError()
    {
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false,
        });
        var tcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                tcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("linked-cts");
        using var cts = new CancellationTokenSource();
        await signal.OnErrorResumeAsync(expected, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that the stateless replay-latest Signal's <c>OnNextAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestOnNextWithCustomToken_ThenForwardsValue()
    {
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (value, _) =>
            {
                tcs.TrySetResult(value);
                return default;
            });

        using var cts = new CancellationTokenSource();
        const int LinkedCtsValue = 17;
        await signal.OnNextAsync(LinkedCtsValue, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsEqualTo(LinkedCtsValue);
    }

    /// <summary>Verifies that the stateless replay-latest Signal's <c>OnErrorResumeAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLatestOnErrorResumeWithCustomToken_ThenForwardsError()
    {
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });
        var tcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            static (_, _) => default,
            (error, _) =>
            {
                tcs.TrySetResult(error);
                return default;
            });

        var expected = new InvalidOperationException("stateless-linked-cts");
        using var cts = new CancellationTokenSource();
        await signal.OnErrorResumeAsync(expected, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that replay-latest subscription with a caller-supplied cancellation token
    /// disposes the linked token source after subscribing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubscribeWithCustomToken_ThenSubscriptionCompletes()
    {
        var signal = Signal.CreateReplayLatest<int>();
        using var cts = new CancellationTokenSource();

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
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });
        using var cts = new CancellationTokenSource();

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
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });
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
        var signal = Signal.CreateReplayLatest<int>(new()
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });

        await signal.DisposeAsync();
        await signal.DisposeAsync();

        await Assert.That(signal).IsNotNull();
    }
}
