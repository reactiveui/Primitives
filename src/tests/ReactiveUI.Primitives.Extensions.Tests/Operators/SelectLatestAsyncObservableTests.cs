// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>SelectLatestAsync</c> backed by
/// <c>SelectLatestAsyncObservable&lt;TSource, TResult&gt;</c> — error forwarding,
/// disposal mid-flight, stale-id drop path and completion-after-in-flight.</summary>
public class SelectLatestAsyncObservableTests
{
    /// <summary>Synthetic error message attached to a failing selector.</summary>
    private const string SelectorErrorMessage = "selector failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Settle delay in milliseconds used to let an awaited continuation attempt delivery.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Poll interval in milliseconds used while waiting for an emission.</summary>
    private const int PollIntervalMilliseconds = 10;

    /// <summary>Multiplier applied by the gated selector whose result is expected never to be delivered.</summary>
    private const int SuppressedProjectionMultiplier = 2;

    /// <summary>Multiplier applied inside the projection selector.</summary>
    private const int ProjectionMultiplier = 10;

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies that <c>SelectLatestAsync</c> forwards selector exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncSelectorThrows_ThenForwardsError()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new(SelectorErrorMessage);
        using var sub = subject.SelectLatestAsync(_ => Task.FromException<int>(expected)).Subscribe(
            static _ => { },
            ex => faulted.TrySetResult(ex));
        subject.OnNext(TriggerValue);
        var caught = await faulted.Task.WaitAsync(GuardTimeout);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SelectLatestAsync</c> forwards source errors immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SelectLatestAsync(Task.FromResult).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the subscription before the selector completes suppresses any later <c>OnNext</c> / <c>OnCompleted</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncDisposedMidFlight_ThenSuppressesEmissionAndCompletion()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        var completed = false;
        var sub = subject.SelectLatestAsync(async x =>
        {
            await gate.Task.ConfigureAwait(false);
            return x * SuppressedProjectionMultiplier;
        }).Subscribe(results.Add, () => completed = true);
        subject.OnNext(TriggerValue);
        subject.OnCompleted();
        sub.Dispose();
        _ = gate.TrySetResult(true);

        // Give the awaited continuation a chance to attempt delivery.
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that a newer value supersedes a slower in-flight projection, so only the latest result is emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncNewerArrives_ThenOlderResultDropped()
    {
        const int Slow = 1;
        const int Fast = 2;
        Subject<int> subject = new();
        TaskCompletionSource<bool> slowGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SelectLatestAsync(async x =>
        {
            if (x == Slow)
            {
                await slowGate.Task.ConfigureAwait(false);
            }

            return x * ProjectionMultiplier;
        }).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext(Slow);
        subject.OnNext(Fast);

        // Wait for the fast projection to complete and emit.
        while (results.Count == 0)
        {
            await Task.Delay(PollIntervalMilliseconds).ConfigureAwait(false);
        }

        _ = slowGate.TrySetResult(true);
        subject.OnCompleted();
        await completed.Task.WaitAsync(GuardTimeout);

        // Only the latest (Fast) projection's result should appear.
        await Assert.That(results).IsCollectionEqualTo([Fast * ProjectionMultiplier]);
    }

    /// <summary>Verifies that source completion before any value still completes downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncSourceCompletesWithNoValues_ThenForwardsCompletion()
    {
        Subject<int> subject = new();
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SelectLatestAsync(Task.FromResult).Subscribe(
            static _ => { },
            () => completed.TrySetResult(true));
        subject.OnCompleted();
        var done = await completed.Task.WaitAsync(GuardTimeout);
        await Assert.That(done).IsTrue();
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
        using var sub = source.SelectLatestAsync(Task.FromResult)
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(completedCount).IsLessThanOrEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
