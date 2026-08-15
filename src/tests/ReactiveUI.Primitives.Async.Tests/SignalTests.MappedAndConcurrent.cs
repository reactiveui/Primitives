// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Mapped Signal, mixins, concurrent multi-observer, and edge-case tests for <see cref = "SignalTests"/>.</summary>
public partial class SignalTests
{
    /// <summary>Tests MapValues transforms observable.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMapValues_ThenTransformsObservable()
    {
        const int Multiplier = 10;
        const int FirstInput = 1;
        const int SecondInput = 2;
        const int FirstMapped = 10;
        const int SecondMapped = 20;
        var signal = Signal.Create<int>();
        var mapped = signal.MapValues(static values => values.Select(static x => x * Multiplier));
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await mapped.Values.SubscribeAsync(
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
        await mapped.OnNextAsync(FirstInput, CancellationToken.None);
        await mapped.OnNextAsync(SecondInput, CancellationToken.None);
        await mapped.OnCompletedAsync(Result.Success);
        await completed.Task.WaitAsync(WaitTimeout);
        await Assert.That(items).IsCollectionEqualTo([FirstMapped, SecondMapped]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless Signal delivers the error to the observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSignalOnErrorResume_ThenObserverReceivesError()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.Create<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("serial-stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateless Signal delivers the error to the observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSignalOnErrorResume_ThenObserverReceivesError()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.Create<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("concurrent-stateless-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that DisposeAsync on a serial stateless Signal clears observers and completes without error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSignalDispose_ThenCompletesAndClearsObservers()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.Create<int>(options);
        List<int> items = [];
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        const int PostDisposeValue = 2;
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();

        // After dispose, observers are cleared so no further values should be delivered.
        await signal.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests that DisposeAsync on a concurrent stateless Signal clears observers and completes without error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSignalDispose_ThenCompletesAndClearsObservers()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var signal = Signal.Create<int>(options);
        List<int> items = [];
        await using var sub = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null);
        const int PostDisposeValue = 2;
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.DisposeAsync();

        // After dispose, observers are cleared so no further values should be delivered.
        await signal.OnNextAsync(PostDisposeValue, CancellationToken.None);
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateful Signal delivers the error to observers.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSignalOnErrorResume_ThenObserverReceivesError()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false };
        var signal = Signal.Create<int>(options);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("concurrent-stateful");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync is ignored after the Signal has already completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsyncCalledAfterCompletion_ThenIsIgnored()
    {
        var signal = Signal.Create<int>();
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
        await signal.OnErrorResumeAsync(new InvalidOperationException("should be ignored"), CancellationToken.None);
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Tests that OnCompletedAsync is ignored on the second call after the Signal has already completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsyncCalledTwice_ThenSecondCallIsIgnored()
    {
        var signal = Signal.Create<int>();
        var completionCount = 0;
        TaskCompletionSource completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, null, _ =>
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

    /// <summary>Tests that subscribing to an already-completed Signal immediately delivers the completion result.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribingToAlreadyCompletedSignal_ThenObserverReceivesCompletionImmediately()
    {
        var signal = Signal.Create<int>();
        TaskCompletionSource firstCompletionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var firstSub = await signal.Values.SubscribeAsync(static (_, _) => default, null, _ =>
        {
            IgnoredResult.Of(firstCompletionTcs.TrySetResult());
            return default;
        });
        await signal.OnCompletedAsync(Result.Success);
        await firstCompletionTcs.Task;
        Result? lateResult = null;
        TaskCompletionSource lateTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var lateSub = await signal.Values.SubscribeAsync(static (_, _) => default, null, result =>
        {
            lateResult = result;
            _ = lateTcs.TrySetResult();
            return default;
        });
        await lateTcs.Task;
        await Assert.That(lateResult).IsNotNull();
        await Assert.That(lateResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that OnNextAsync is ignored after the Signal has already completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAsyncCalledAfterCompletion_ThenIsIgnored()
    {
        var signal = Signal.Create<int>();
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

    /// <summary>Tests that OnErrorResumeAsync forwards the error to observers when the Signal has not completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsyncCalledBeforeCompletion_ThenErrorIsForwarded()
    {
        var signal = Signal.Create<int>();
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("forwarded");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync forwards the result to observers and clears the observer list.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsyncCalled_ThenResultIsForwardedToObservers()
    {
        var signal = Signal.Create<int>();
        TaskCompletionSource<Result> resultTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = resultTcs.TrySetResult(result);
            return default;
        });
        var failure = Result.Failure(new InvalidOperationException("done"));
        await signal.OnCompletedAsync(failure);
        var received = await resultTcs.Task;
        await Assert.That(received.IsFailure).IsTrue();
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless Signal delivers the error to multiple observers sequentially.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSignalOnErrorResumeWithMultipleObservers_ThenAllObserversReceiveError()
    {
        SignalCreationOptions options = new() { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var signal = Signal.Create<int>(options);
        List<Exception> errors1 = [];
        List<Exception> errors2 = [];
        await using var sub1 = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors1.Add(ex);
            return default;
        });
        await using var sub2 = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            errors2.Add(ex);
            return default;
        });
        InvalidOperationException expected = new("multi-observer-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(errors1).Count().IsEqualTo(1);
        await Assert.That(errors1[0]).IsEqualTo(expected);
        await Assert.That(errors2).Count().IsEqualTo(1);
        await Assert.That(errors2[0]).IsEqualTo(expected);
    }

    /// <summary>Tests that SignalAsync.Create throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenCreateWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Create<int>(null!));

    /// <summary>Tests that Signal.CreateBehavior throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenCreateBehaviorWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.CreateBehavior(0, null!));

    /// <summary>Tests that SignalAsync.CreateReplayLatest throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenCreateReplayLatestWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.CreateReplayLatest<int>(null!));

    /// <summary>Tests that Mappedsignal.SubscribeAsync subscribes an observer through the mapped values observable.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSignalSubscribeAsync_ThenObserverReceivesMappedValues()
    {
        const int InputValue = 10;
        const int Increment = 1;
        const int MappedValue = 11;
        var signal = Signal.Create<int>();
        var mapped = signal.MapValues(static values => values.Select(static x => x + Increment));
        var collector = Signal.Create<int>();
        List<int> items = [];
        await using var collectorSub = await collector.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);
        var observer = collector.AsObserverAsync();
        await using var sub = await mapped.SubscribeAsync(observer, CancellationToken.None);
        await mapped.OnNextAsync(InputValue, CancellationToken.None);
        await mapped.OnCompletedAsync(Result.Success);
        await Assert.That(items).IsCollectionEqualTo([MappedValue]);
    }

    /// <summary>Tests that Mappedsignal.OnErrorResumeAsync forwards the error to the original Signal.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSignalOnErrorResumeAsync_ThenErrorIsForwardedToOriginal()
    {
        var signal = Signal.Create<int>();
        var mapped = signal.MapValues(static values => values);
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("mapped-error");
        await mapped.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that Mappedsignal.DisposeAsync disposes the original Signal.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSignalDisposeAsync_ThenOriginalSignalIsDisposed()
    {
        var signal = Signal.Create<int>();
        var mapped = signal.MapValues(static values => values);

        // DisposeAsync on the mapped Signal should dispose the underlying Signal.
        await mapped.DisposeAsync();

        // Verify double-dispose is safe.
        await mapped.DisposeAsync();
    }

    /// <summary>Tests that AsObserverAsync forwards OnErrorResumeAsync to the underlying Signal.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsyncOnErrorResume_ThenErrorIsForwardedToSignal()
    {
        var signal = Signal.Create<int>();
        var observer = signal.AsObserverAsync();
        TaskCompletionSource<Exception> errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            IgnoredResult.Of(errorTcs.TrySetResult(ex));
            return default;
        });
        InvalidOperationException expected = new("observer-error");
        await observer.OnErrorResumeAsync(expected, CancellationToken.None);
        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that AsObserverAsync forwards OnCompletedAsync to the underlying Signal.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsyncOnCompleted_ThenCompletionIsForwardedToSignal()
    {
        var signal = Signal.Create<int>();
        var observer = signal.AsObserverAsync();
        TaskCompletionSource<Result> resultTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.SubscribeAsync(static (_, _) => default, null, result =>
        {
            _ = resultTcs.TrySetResult(result);
            return default;
        });
        await observer.OnCompletedAsync(Result.Success);
        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that ForwardOnErrorResumeConcurrently with an empty observer list completes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnErrorResumeConcurrentlyWithEmptyObservers_ThenCompletesImmediately()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        var task = Concurrent.ForwardOnErrorResumeConcurrently(
            emptyObservers,
            new InvalidOperationException("unused"),
            CancellationToken.None);
        await Assert.That(task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Tests that ForwardOnCompletedConcurrently with an empty observer list completes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnCompletedConcurrentlyWithEmptyObservers_ThenCompletesImmediately()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        var task = Concurrent.ForwardOnCompletedConcurrently(emptyObservers, Result.Success);
        await Assert.That(task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Tests that concurrent Signal forwards OnNext to multiple observers concurrently.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSignalWithMultipleObservers_ThenAllReceiveOnNext()
    {
        var signal = Signal.Create<int>(new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false });
        List<int> items1 = [];
        List<int> items2 = [];
        await using var sub1 = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items1.Add(x);
                }

                return default;
            },
            null);
        await using var sub2 = await signal.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items2.Add(x);
                }

                return default;
            },
            null);
        const int PushedValue = 42;
        await signal.OnNextAsync(PushedValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(items1).IsCollectionEqualTo([PushedValue]);
        await Assert.That(items2).IsCollectionEqualTo([PushedValue]);
    }

    /// <summary>Tests that concurrent Signal forwards OnErrorResume to multiple observers concurrently.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSignalWithMultipleObservers_ThenAllReceiveOnErrorResume()
    {
        var signal = Signal.Create<int>(new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false });
        List<Exception> errors1 = [];
        List<Exception> errors2 = [];
        InvalidOperationException error = new("test");
        await using var sub1 = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            lock (_gate)
            {
                errors1.Add(ex);
            }

            return default;
        });
        await using var sub2 = await signal.Values.SubscribeAsync(static (_, _) => default, (ex, _) =>
        {
            lock (_gate)
            {
                errors2.Add(ex);
            }

            return default;
        });
        await signal.OnErrorResumeAsync(error, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(errors1).Count().IsEqualTo(1);
        await Assert.That(errors2).Count().IsEqualTo(1);
    }

    /// <summary>Tests that concurrent Signal forwards OnCompleted to multiple observers concurrently.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSignalWithMultipleObservers_ThenAllReceiveOnCompleted()
    {
        var signal = Signal.Create<int>(new() { PublishingOption = PublishingOption.Concurrent, IsStateless = false });
        TaskCompletionSource completed1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed2 = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub1 = await signal.Values.SubscribeAsync(static (_, _) => default, null, _ =>
        {
            IgnoredResult.Of(completed1.TrySetResult());
            return default;
        });
        await using var sub2 = await signal.Values.SubscribeAsync(static (_, _) => default, null, _ =>
        {
            IgnoredResult.Of(completed2.TrySetResult());
            return default;
        });
        await signal.OnCompletedAsync(Result.Success);
        await completed1.Task.WaitAsync(WaitTimeout);
        await completed2.Task.WaitAsync(WaitTimeout);
    }
}
