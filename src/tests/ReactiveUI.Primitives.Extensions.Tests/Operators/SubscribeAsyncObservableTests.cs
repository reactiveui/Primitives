// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the <c>SubscribeSynchronous</c> / <c>SubscribeAsync</c>
/// overloads backed by <c>SubscribeAsyncObservable&lt;T&gt;</c> — sequential handler invocation,
/// handler-throws forwards via onError, completion-while-processing defers, disposal stops queue.</summary>
public class SubscribeAsyncObservableTests
{
    /// <summary>Synthetic error message attached to handler failures.</summary>
    private const string HandlerFailedMessage = "handler failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Settle delay in milliseconds used to confirm completion is deferred.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Verifies that values are handled in order and completion fires.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncProcessesValues_ThenInOrder()
    {
        const int First = 1;
        const int Second = 2;
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SubscribeSynchronous(
            x =>
            {
                results.Add(x);
                return default;
            },
            static _ => { },
            () => completed.TrySetResult(true));
        subject.OnNext(First);
        subject.OnNext(Second);
        subject.OnCompleted();
        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([First, Second]);
    }

    /// <summary>Verifies that a handler exception is forwarded to the error callback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncHandlerThrows_ThenForwardsToOnError()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new(HandlerFailedMessage);
        using var sub =
            subject.SubscribeSynchronous(_ => ValueTask.FromException(expected), ex => faulted.TrySetResult(ex));
        subject.OnNext(TriggerValue);
        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a handler exception is swallowed when no error callback is supplied.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncHandlerThrowsWithoutOnError_ThenNoExceptionEscapes()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        var handlerRan = 0;
        using var sub = subject.SubscribeSynchronous(_ =>
        {
            handlerRan++;
            return ValueTask.FromException(new InvalidOperationException(HandlerFailedMessage));
        });
        subject.OnNext(TriggerValue);
        await Assert.That(handlerRan).IsEqualTo(1);
    }

    /// <summary>Verifies that source errors are forwarded to the error callback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSourceErrors_ThenForwardsToOnError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SubscribeSynchronous(static _ => default, ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that completion arriving while a handler is in flight defers completion until the handler finishes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncCompletesWhileProcessing_ThenDeferredCompletion()
    {
        const int Value = 7;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SubscribeSynchronous(
            async _ => await gate.Task.ConfigureAwait(false),
            () => completed.TrySetResult(true));
        subject.OnNext(Value);
        subject.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(completed.Task.IsCompleted).IsFalse();
        _ = gate.TrySetResult(true);
        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies deferred completion with no completion callback takes the null-callback path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncCompletesWhileProcessingWithoutCallback_ThenNoExceptionEscapes()
    {
        const int Value = 7;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> handled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SubscribeSynchronous(async value =>
        {
            await gate.Task.ConfigureAwait(false);
            _ = handled.TrySetResult(true);
        });
        subject.OnNext(Value);
        subject.OnCompleted();
        _ = gate.TrySetResult(true);
        var done = await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies disposal during an in-flight handler suppresses deferred terminal callbacks.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncDisposedDuringInFlight_ThenSuppressesCompletionAndError()
    {
        const int Value = 7;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> handlerStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? caught = null;
        var completedCount = 0;
        var sub = subject.SubscribeSynchronous(
            async value =>
            {
                _ = handlerStarted.TrySetResult(true);
                await gate.Task.ConfigureAwait(false);
                throw new InvalidOperationException(HandlerFailedMessage);
            },
            ex => caught = ex,
            () => completedCount++);
        subject.OnNext(Value);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        subject.OnCompleted();
        sub.Dispose();
        _ = gate.TrySetResult(true);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(caught).IsNull();
        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies that disposing the subscription stops further handler invocations.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncDisposed_ThenStopsProcessing()
    {
        const int IgnoredValue = 1;
        Subject<int> subject = new();
        var handlerRan = 0;
        var sub = subject.SubscribeSynchronous(value =>
        {
            _ = Interlocked.Increment(ref handlerRan);
            return default;
        });
        sub.Dispose();
        subject.OnNext(IgnoredValue);
        await Assert.That(Volatile.Read(ref handlerRan)).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SubscribeSynchronous(
            x =>
            {
                values.Add(x);
                return default;
            },
            ex => caught = ex,
            () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that an <c>OnCompleted</c> arriving after a prior <c>OnError</c> is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var completedCount = 0;
        InvalidOperationException expected = new("first");
        using var sub = source.SubscribeSynchronous(static _ => default, ex => caught = ex, () => completedCount++);
        source.Observer.OnError(expected);
        source.Observer.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completedCount).IsEqualTo(0);
    }
}
