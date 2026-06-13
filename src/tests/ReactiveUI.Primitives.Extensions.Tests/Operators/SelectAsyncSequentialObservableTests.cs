// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>SelectAsyncSequential</c> backed by
/// <c>SelectAsyncSequentialObservable&lt;TSource, TResult&gt;</c> — error forwarding,
/// disposal mid-flight, and completion while an in-flight selector is running.</summary>
public class SelectAsyncSequentialObservableTests
{
    /// <summary>Synthetic error message attached to a failing selector.</summary>
    private const string SelectorErrorMessage = "selector failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Settle delay in milliseconds used to let an awaited continuation attempt delivery.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Verifies that <c>SelectAsyncSequential</c> forwards selector exceptions and stops draining the queue afterwards.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequentialSelectorThrows_ThenForwardsErrorAndStops()
    {
        const int First = 1;
        const int Second = 2;
        var subject = new Subject<int>();
        var faulted = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var results = new List<int>();
        var expected = new InvalidOperationException(SelectorErrorMessage);
        using var sub = subject.SelectAsyncSequential(x => x == First ? Task.FromException<int>(expected) : Task.FromResult(x)).Subscribe(results.Add, ex => faulted.TrySetResult(ex));
        subject.OnNext(First);
        subject.OnNext(Second);
        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>SelectAsyncSequential</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequentialSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.SelectAsyncSequential(static x => Task.FromResult(x)).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the subscription mid-flight suppresses further emissions and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequentialDisposedMidFlight_ThenSuppressesEmissionAndCompletion()
    {
        const int TriggerValue = 1;
        var subject = new Subject<int>();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var results = new List<int>();
        var completed = false;
        var sub = subject.SelectAsyncSequential(async x =>
        {
            await gate.Task.ConfigureAwait(false);
            return x;
        }).Subscribe(results.Add, () => completed = true);
        subject.OnNext(TriggerValue);
        subject.OnCompleted();
        sub.Dispose();
        gate.TrySetResult(true);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that completion arriving while a selector is in flight is forwarded after the in-flight selector finishes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequentialCompletesWhileProcessing_ThenDeferredCompletion()
    {
        const int Value = 42;
        var subject = new Subject<int>();
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var results = new List<int>();
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SelectAsyncSequential(async x =>
        {
            await gate.Task.ConfigureAwait(false);
            return x;
        }).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext(Value);
        subject.OnCompleted();

        // Completion must not fire while selector is gated.
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(completed.Task.IsCompleted).IsFalse();
        gate.TrySetResult(true);
        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([Value]);
    }

    /// <summary>Verifies that <c>OnError</c> and a duplicate <c>OnCompleted</c> arriving from
    /// the source after the sink has marked itself terminated are silently dropped via the
    /// <c>_done || _disposed</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterTerminated_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SelectAsyncSequential(static x => Task.FromResult(x)).Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(caught).IsNull();
    }
}
