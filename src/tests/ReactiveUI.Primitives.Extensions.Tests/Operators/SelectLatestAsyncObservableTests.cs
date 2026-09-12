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

    /// <summary>Multiplier applied by the gated selector whose result is expected never to be delivered.</summary>
    private const int SuppressedProjectionMultiplier = 2;

    /// <summary>Multiplier applied inside the projection selector.</summary>
    private const int ProjectionMultiplier = 10;

    /// <summary>Verifies that <c>SelectLatestAsync</c> forwards selector exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncSelectorThrows_ThenForwardsError()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<Exception> faulted = new();
        InvalidOperationException expected = new(SelectorErrorMessage);
        using var sub = subject.SelectLatestAsync(_ => Task.FromException<int>(expected)).Subscribe(
            static _ => { },
            ex => faulted.TrySetResult(ex));
        subject.OnNext(TriggerValue);
        var caught = await faulted.Task;
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

        // The gate completes its continuations inline, so releasing it runs the selector's tail here.
        TaskCompletionSource<bool> gate = new();
        TaskCompletionSource<bool> selectorResumed = new();
        List<int> results = [];
        var completed = false;
        var sub = subject.SelectLatestAsync(async x =>
        {
            await gate.Task.ConfigureAwait(false);
            _ = selectorResumed.TrySetResult(true);
            return x * SuppressedProjectionMultiplier;
        }).Subscribe(results.Add, () => completed = true);
        subject.OnNext(TriggerValue);
        subject.OnCompleted();
        sub.Dispose();
        gate.SetResult(true);
        await selectorResumed.Task;
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

        // The gate completes its continuations inline, so releasing it runs the stale projection's tail here.
        TaskCompletionSource<bool> slowGate = new();
        TaskCompletionSource<bool> slowResumed = new();
        List<int> results = [];
        TaskCompletionSource<bool> completed = new();
        using var sub = subject.SelectLatestAsync(async x =>
        {
            if (x == Slow)
            {
                await slowGate.Task.ConfigureAwait(false);
                _ = slowResumed.TrySetResult(true);
            }

            return x * ProjectionMultiplier;
        }).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext(Slow);
        subject.OnNext(Fast);

        // The Fast projection is ungated, so its result is already delivered.
        await Assert.That(results).IsCollectionEqualTo([Fast * ProjectionMultiplier]);
        slowGate.SetResult(true);
        await slowResumed.Task;
        subject.OnCompleted();
        await completed.Task;

        // Only the latest (Fast) projection's result should appear.
        await Assert.That(results).IsCollectionEqualTo([Fast * ProjectionMultiplier]);
    }

    /// <summary>Verifies that source completion before any value still completes downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncSourceCompletesWithNoValues_ThenForwardsCompletion()
    {
        Subject<int> subject = new();
        TaskCompletionSource<bool> completed = new();
        using var sub = subject.SelectLatestAsync(Task.FromResult).Subscribe(
            static _ => { },
            () => completed.TrySetResult(true));
        subject.OnCompleted();
        var done = await completed.Task;
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
        await Assert.That(completedCount).IsLessThanOrEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
