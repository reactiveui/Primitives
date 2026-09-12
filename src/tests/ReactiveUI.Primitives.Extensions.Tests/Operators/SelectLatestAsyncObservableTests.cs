// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests latest projection delivery, errors, and disposal during projection.</summary>
public class SelectLatestAsyncObservableTests
{
    /// <summary>Synthetic error message attached to a failing selector.</summary>
    private const string SelectorErrorMessage = "selector failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Multiplier applied inside the projection selector.</summary>
    private const int ProjectionMultiplier = 10;

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
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        var completed = false;
        SelectLatestAsyncObservable<int, int>.SelectLatestAsyncSink sink = new(Observer.Create<int>(results.Add, () => completed = true), _ => gate.Task);
        var processing = sink.OnNextAsync(TriggerValue);
        sink.Dispose();
        gate.SetResult(TriggerValue);
        await processing;
        sink.OnCompleted();
        sink.SignalCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies source completion waits for the latest projection and is delivered once.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task SourceCompletionWaitsForLatestProjection()
    {
        const int Value = 1;
        TaskCompletionSource<int> projection = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];
        var completions = 0;
        using SelectLatestAsyncObservable<int, int>.SelectLatestAsyncSink sink = new(
            Observer.Create<int>(values.Add, () => completions++),
            _ => projection.Task);
        var processing = sink.OnNextAsync(Value);
        sink.OnCompleted();
        await Assert.That(completions).IsZero();
        projection.SetResult(Value);
        await processing;
        sink.SignalCompleted();
        await Assert.That(values).IsCollectionEqualTo([Value]);
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies that a newer value supersedes a slower in-flight projection, so only the latest result is emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsyncNewerArrives_ThenOlderResultDropped()
    {
        const int Slow = 1;
        const int Fast = 2;
        TaskCompletionSource<int> slowGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        var completed = false;
        using SelectLatestAsyncObservable<int, int>.SelectLatestAsyncSink sink = new(
            Observer.Create<int>(results.Add, () => completed = true),
            value => value == Slow ? slowGate.Task : Task.FromResult(value * ProjectionMultiplier));
        var slow = sink.OnNextAsync(Slow);
        await sink.OnNextAsync(Fast);
        await Assert.That(results).IsCollectionEqualTo([Fast * ProjectionMultiplier]);
        slowGate.SetResult(Slow * ProjectionMultiplier);
        await slow;
        sink.OnCompleted();
        await Assert.That(completed).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([Fast * ProjectionMultiplier]);
    }

    /// <summary>Verifies an empty source completes downstream.</summary>
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
        using var sub = source.SelectLatestAsync(Task.FromResult).Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
