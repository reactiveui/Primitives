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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncProcessesValues_ThenInOrder()
    {
        const int First = 1;
        const int Second = 2;
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncHandlerThrows_ThenForwardsToOnError()
    {
        const int TriggerValue = 1;
        var subject = new Subject<int>();
        var faulted = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = new InvalidOperationException(HandlerFailedMessage);

        using var sub = subject.SubscribeSynchronous(
            _ => ValueTask.FromException(expected),
            ex => faulted.TrySetResult(ex));

        subject.OnNext(TriggerValue);

        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a handler exception is swallowed when no error callback is supplied.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncHandlerThrowsWithoutOnError_ThenNoExceptionEscapes()
    {
        const int TriggerValue = 1;
        var subject = new Subject<int>();
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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSourceErrors_ThenForwardsToOnError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.SubscribeSynchronous(
            static _ => default,
            ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that completion arriving while a handler is in flight defers completion until the handler finishes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncCompletesWhileProcessing_ThenDeferredCompletion()
    {
        const int Value = 7;
        var subject = new Subject<int>();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = subject.SubscribeSynchronous(
            async _ => await gate.Task.ConfigureAwait(false),
            () => completed.TrySetResult(true));

        subject.OnNext(Value);
        subject.OnCompleted();

        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(completed.Task.IsCompleted).IsFalse();

        gate.TrySetResult(true);

        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies deferred completion with no completion callback takes the null-callback path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncCompletesWhileProcessingWithoutCallback_ThenNoExceptionEscapes()
    {
        const int Value = 7;
        var subject = new Subject<int>();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = subject.SubscribeSynchronous(async _ =>
        {
            await gate.Task.ConfigureAwait(false);
            handled.TrySetResult(true);
        });

        subject.OnNext(Value);
        subject.OnCompleted();
        gate.TrySetResult(true);

        var done = await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies disposal during an in-flight handler suppresses deferred terminal callbacks.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncDisposedDuringInFlight_ThenSuppressesCompletionAndError()
    {
        const int Value = 7;
        var subject = new Subject<int>();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? caught = null;
        var completedCount = 0;

        var sub = subject.SubscribeSynchronous(
            async _ =>
            {
                handlerStarted.TrySetResult(true);
                await gate.Task.ConfigureAwait(false);
                throw new InvalidOperationException(HandlerFailedMessage);
            },
            ex => caught = ex,
            () => completedCount++);

        subject.OnNext(Value);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        subject.OnCompleted();
        sub.Dispose();
        gate.TrySetResult(true);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);

        await Assert.That(caught).IsNull();
        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies that disposing the subscription stops further handler invocations.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncDisposed_ThenStopsProcessing()
    {
        const int IgnoredValue = 1;
        var subject = new Subject<int>();
        var handlerRan = 0;

        var sub = subject.SubscribeSynchronous(_ =>
        {
            Interlocked.Increment(ref handlerRan);
            return default;
        });

        sub.Dispose();
        subject.OnNext(IgnoredValue);

        await Assert.That(Volatile.Read(ref handlerRan)).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completedCount = 0;
        var expected = new InvalidOperationException("first");

        using var sub = source.SubscribeSynchronous(
            static _ => default,
            ex => caught = ex,
            () => completedCount++);

        source.Observer.OnError(expected);
        source.Observer.OnCompleted();

        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completedCount).IsEqualTo(0);
    }
}
