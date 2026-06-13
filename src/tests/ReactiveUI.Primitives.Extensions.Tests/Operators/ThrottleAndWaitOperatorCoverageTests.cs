// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>ThrottleOnScheduler</c>, <c>WaitUntil</c>,
/// <c>TakeUntilInclusive</c>, and <c>SynchronizeAsync</c> — paths the happy-path
/// tests do not reach (error propagation, predicate throws, dispose, completion-flush).</summary>
public class ThrottleAndWaitOperatorCoverageTests
{
    /// <summary>Throttle interval used across the scheduler-driven tests.</summary>
    private const int ThrottleTicks = 100;

    /// <summary>Tick advance guaranteed to fire any pending throttle scheduling.</summary>
    private const int AdvancePastWindowTicks = 200;

    /// <summary>Message used for synthesised source exceptions across the error-propagation tests.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Message used for synthesised predicate-throws exceptions.</summary>
    private const string PredicateFailedMessage = "predicate failed";

    /// <summary>Sample value 1 used by the predicate-walk tests.</summary>
    private const int Sample1 = 1;

    /// <summary>Sample value 2 used by the predicate-walk tests.</summary>
    private const int Sample2 = 2;

    /// <summary>Sample value 3 used by the predicate-walk tests (also the inclusive match boundary).</summary>
    private const int Sample3 = 3;

    /// <summary>Sample value 4 used by the predicate-walk tests (ignored once the match completes).</summary>
    private const int Sample4 = 4;

    /// <summary>Match threshold large enough that no Sample value satisfies the predicate.</summary>
    private const int NoMatchThreshold = 100;

    /// <summary>Value that does satisfy <see cref = "NoMatchThreshold"/> so the dispose test can confirm nothing is delivered after disposal.</summary>
    private const int MatchValue = 500;

    /// <summary>Verifies that <c>ThrottleOnScheduler</c> forwards source errors and cancels the pending emission.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerSourceErrors_ThenForwardsErrorAndDropsPending()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(results.Add, ex => caught = ex);
        subject.OnNext(Sample1);
        subject.OnError(expected);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that completion with a pending value flushes the latest value before completing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerCompletesWithPending_ThenFlushesLatestThenCompletes()
    {
        const int Pending = 7;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(results.Add, () => completed = true);
        subject.OnNext(Pending);
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Pending]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that completion with no pending value just completes (no flushed value).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerCompletesEmpty_ThenCompletesWithoutValue()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler)
            .Subscribe(results.Add, () => completed = true);
        subject.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that disposing a throttle subscription stops further deliveries even when a scheduled
    /// emission was already enqueued.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerDisposed_ThenScheduledEmissionDropped()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(Sample1);
        sub.Dispose();
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies <c>WaitUntil</c> forwards source errors when no match has occurred yet.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.WaitUntil(static x => x > NoMatchThreshold).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnNext(Sample1);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>WaitUntil</c> completes if the source completes before any element matches.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilSourceCompletesWithoutMatch_ThenCompletesWithoutValue()
    {
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.WaitUntil(static x => x > NoMatchThreshold)
            .Subscribe(results.Add, () => completed = true);
        subject.OnNext(Sample1);
        subject.OnNext(Sample2);
        subject.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that a throwing predicate is surfaced as an OnError and stops further deliveries.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilPredicateThrows_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        List<int> results = [];
        InvalidOperationException expected = new(PredicateFailedMessage);
        using var sub = subject.WaitUntil((Func<int, bool>)(_ => throw expected))
            .Subscribe(results.Add, ex => caught = ex);
        subject.OnNext(Sample1);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that disposing a WaitUntil subscription before a match stops downstream emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilDisposedBeforeMatch_ThenNoDownstreamEmission()
    {
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        var sub = subject.WaitUntil(static x => x > NoMatchThreshold).Subscribe(results.Add, () => completed = true);
        sub.Dispose();
        subject.OnNext(MatchValue);
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that <c>TakeUntilInclusive</c> emits the matching element then completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilInclusivePredicateMatches_ThenEmitsThroughMatchAndCompletes()
    {
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.TakeUntil(static x => x >= Sample3).Subscribe(results.Add, () => completed = true);
        subject.OnNext(Sample1);
        subject.OnNext(Sample2);
        subject.OnNext(Sample3);
        subject.OnNext(Sample4);
        await Assert.That(results).IsCollectionEqualTo([Sample1, Sample2, Sample3]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>TakeUntilInclusive</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilInclusiveSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.TakeUntil(static x => x >= NoMatchThreshold).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnNext(Sample1);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a throwing predicate on <c>TakeUntilInclusive</c> surfaces as OnError.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilInclusivePredicateThrows_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        List<int> results = [];
        InvalidOperationException expected = new(PredicateFailedMessage);
        using var sub = subject.TakeUntil((Func<int, bool>)(_ => throw expected))
            .Subscribe(results.Add, ex => caught = ex);
        subject.OnNext(Sample1);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>TakeUntilInclusive</c> completes when the source completes before any match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilInclusiveSourceCompletesBeforeMatch_ThenCompletes()
    {
        Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.TakeUntil(static x => x >= NoMatchThreshold)
            .Subscribe(results.Add, () => completed = true);
        subject.OnNext(Sample1);
        subject.OnNext(Sample2);
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Sample1, Sample2]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies <c>SynchronizeAsync</c> forwards source errors to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSynchronizeAsyncSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SynchronizeAsync().Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>SynchronizeAsync</c> forwards completion to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSynchronizeAsyncSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int> subject = new();
        var completed = false;
        using var sub = subject.SynchronizeAsync().Subscribe(
            static _ => { },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that disposing a <c>SynchronizeAsync</c> subscription suppresses subsequent emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSynchronizeAsyncDisposed_ThenSuppressesSubsequentEmissions()
    {
        Subject<int> subject = new();
        var emissions = 0;
        var sub = subject.SynchronizeAsync().Subscribe(tuple =>
        {
            Interlocked.Increment(ref emissions);
            tuple.Sync.Dispose();
        });
        sub.Dispose();
        subject.OnNext(Sample1);
        subject.OnNext(Sample2);
        await Assert.That(emissions).IsEqualTo(0);
    }
}
