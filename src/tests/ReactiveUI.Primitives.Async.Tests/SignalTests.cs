// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for SignalAsync factory, all Signal variants, and SignalExtensions.</summary>
public partial class SignalTests
{
#if NET9_0_OR_GREATER
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>Tests serial Signal pushes values to all observers in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialSignalPushValues_ThenAllObserversReceiveInOrder()
    {
        var signal = Signal.Create<int>();
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;
        const int ThirdValue = 3;

        await signal.OnNextAsync(FirstValue, CancellationToken.None);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await signal.OnNextAsync(ThirdValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([FirstValue, SecondValue, ThirdValue]);
    }

    /// <summary>Tests concurrent Signal pushes values to all observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSignalPushValues_ThenAllObserversReceive()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.Create<int>(options);
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 10;
        const int SecondValue = 20;
        const int ExpectedCount = 2;

        await signal.OnNextAsync(FirstValue, CancellationToken.None);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests serial stateless Signal pushes values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSignalPushValues_ThenObserversReceive()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.Create<string>(options);
        List<string> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        await signal.OnNextAsync("a", CancellationToken.None);
        await signal.OnNextAsync("b", CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo(["a", "b"]);
    }

    /// <summary>Tests concurrent stateless Signal pushes values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSignalPushValues_ThenObserversReceive()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.Create<int>(options);
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int PushedValue = 5;

        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([PushedValue]);
    }

    /// <summary>Tests Signal OnErrorResume delivers error to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSignalOnErrorResume_ThenObserverReceivesError()
    {
        var signal = Signal.Create<int>();
        List<Exception> errors = [];
        TaskCompletionSource errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                errorReceived.TrySetResult();
                return default;
            });

        await signal.OnErrorResumeAsync(new InvalidOperationException("test"), CancellationToken.None);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        const int ExpectedErrorCount = 1;
        await Assert.That(errors).Count().IsEqualTo(ExpectedErrorCount);
        await Assert.That(errors[0].Message).IsEqualTo("test");
    }

    /// <summary>Tests Signal OnCompleted delivers completion to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSignalOnCompleted_ThenObserverReceivesCompletion()
    {
        var signal = Signal.Create<int>();
        Result? completionResult = null;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                completed.TrySetResult();
                return default;
            });

        await signal.OnCompletedAsync(Result.Success);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests Signal OnCompleted with failure delivers failure to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSignalOnCompletedWithFailure_ThenObserverReceivesFailure()
    {
        var signal = Signal.Create<int>();
        Result? completionResult = null;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                completed.TrySetResult();
                return default;
            });

        await signal.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests multiple observers all receive values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMultipleObservers_ThenAllReceiveValues()
    {
        var signal = Signal.Create<int>();
        List<int> items1 = [];
        List<int> items2 = [];
        TaskCompletionSource completed1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed2 = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub1 = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items1.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed1.TrySetResult();
                return default;
            });

        await using var sub2 = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items2.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed2.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;

        await signal.OnNextAsync(FirstValue, CancellationToken.None);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await Task.WhenAll(completed1.Task, completed2.Task).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items1).IsCollectionEqualTo([FirstValue, SecondValue]);
        await Assert.That(items2).IsCollectionEqualTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests AsObserverAsync forwards to Signal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsync_ThenForwardsToSignal()
    {
        var signal = Signal.Create<int>();
        var observer = signal.AsObserverAsync();
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;

        await observer.OnNextAsync(FirstValue, CancellationToken.None);
        await observer.OnNextAsync(SecondValue, CancellationToken.None);
        await observer.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests default SignalCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultSignalCreationOptions_ThenSerialAndStateful()
    {
        var options = SignalCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }

    /// <summary>Tests default BehaviorSignalCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultBehaviorSignalCreationOptions_ThenSerialAndStateful()
    {
        var options = BehaviorSignalCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }

    /// <summary>Tests default ReplayLatestSignalCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultReplayLatestSignalCreationOptions_ThenSerialAndStateful()
    {
        var options = ReplayLatestSignalCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }
}
