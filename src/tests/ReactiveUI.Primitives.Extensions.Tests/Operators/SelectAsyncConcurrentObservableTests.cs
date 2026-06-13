// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>SelectAsyncConcurrent</c> backed by
/// <c>SelectAsyncConcurrentObservable&lt;TSource, TResult&gt;</c> — error forwarding,
/// disposal mid-flight, and deferred completion while in-flight selectors finish.</summary>
public class SelectAsyncConcurrentObservableTests
{
    /// <summary>Synthetic error message attached to a failing selector.</summary>
    private const string SelectorErrorMessage = "selector failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Settle delay in milliseconds used to let an awaited continuation attempt delivery.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Max concurrency used for two-in-flight tests.</summary>
    private const int MaxConcurrencyTwo = 2;

    /// <summary>Max concurrency used for four-in-flight tests.</summary>
    private const int MaxConcurrencyFour = 4;

    /// <summary>Verifies that <c>SelectAsyncConcurrent</c> forwards selector exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentSelectorThrows_ThenForwardsError()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new(SelectorErrorMessage);
        using var sub = subject.SelectAsyncConcurrent(_ => Task.FromException<int>(expected), MaxConcurrencyTwo)
            .Subscribe(
                static _ => { },
                ex => faulted.TrySetResult(ex));
        subject.OnNext(TriggerValue);
        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SelectAsyncConcurrent</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SelectAsyncConcurrent(static x => Task.FromResult(x), MaxConcurrencyTwo).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the subscription mid-flight suppresses further emissions and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentDisposedMidFlight_ThenSuppressesEmissionAndCompletion()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        var completed = false;
        var sub = subject.SelectAsyncConcurrent(
            async x =>
            {
                await gate.Task.ConfigureAwait(false);
                return x;
            },
            MaxConcurrencyTwo).Subscribe(results.Add, () => completed = true);
        subject.OnNext(TriggerValue);
        subject.OnCompleted();
        sub.Dispose();
        gate.TrySetResult(true);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that completion arriving while selectors are still in flight is forwarded after all selectors finish.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentCompletesWithInFlight_ThenDeferredCompletion()
    {
        const int First = 1;
        const int Second = 2;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SelectAsyncConcurrent(
            async x =>
            {
                await gate.Task.ConfigureAwait(false);
                return x;
            },
            MaxConcurrencyFour).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext(First);
        subject.OnNext(Second);
        subject.OnCompleted();

        // The selector is gated; nothing should have emitted yet.
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(completed.Task.IsCompleted).IsFalse();
        gate.TrySetResult(true);
        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();

        // Downstream OnNext from this operator is serialized inside the sink's lock, so the
        // list is safely populated by the time completion fires. Order is concurrent so sort.
        int[] sorted = [.. results];
        Array.Sort(sorted);
        await Assert.That(sorted).IsCollectionEqualTo([First, Second]);
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
        using var sub = source.SelectAsyncConcurrent(static x => Task.FromResult(x), 1)
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
