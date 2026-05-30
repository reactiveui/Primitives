// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

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

    /// <summary>Verifies that completion arriving while a handler is in flight defers
    /// completion until the handler finishes.</summary>
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
