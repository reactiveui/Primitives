// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage batch for several small synchronous operators:
/// <c>DetectStale</c>, <c>BufferUntilIdle</c>, <c>DebounceImmediate</c>,
/// <c>DebounceUntil</c>, <c>Schedule</c> (value and source overloads),
/// <c>LatestOrDefault</c>, <c>Pairwise</c>, <c>WaitUntil</c>,
/// <c>SwitchIfEmpty</c>. Tests focus on the terminal/error/disposal branches
/// that the existing happy-path tests don't already cover.</summary>
public class ScheduledAndDebounceSyncOperatorTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Standard scheduler tick window used by the timed operators.</summary>
    private const int WindowTicks = 100;

    /// <summary>Advance amount that exceeds the window once.</summary>
    private const int AdvancePastWindowTicks = 101;

    /// <summary>Sentinel value 1.</summary>
    private const int Value1 = 1;

    /// <summary>Sentinel value 2.</summary>
    private const int Value2 = 2;

    /// <summary>Sentinel value 3.</summary>
    private const int Value3 = 3;

    /// <summary>Fallback sentinel.</summary>
    private const int Fallback = 99;

    /// <summary>Verifies that <c>DetectStale</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDetectStaleSourceErrors_ThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.DetectStale(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>DetectStale</c> forwards source completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDetectStaleSourceCompletes_ThenForwardsCompletion()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var completed = false;
        using var sub = subject.DetectStale(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>BufferUntilIdle</c> flushes pending values then forwards errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilIdleSourceErrors_ThenFlushesThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<IList<int>>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.BufferUntilIdle(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add, ex => caught = ex);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0]).IsCollectionEqualTo([Value1, Value2]);
    }

    /// <summary>Verifies that <c>DebounceImmediate</c> emits the first value inline and debounces the rest.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediate_ThenFirstInlineThenDebouncedTail()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.DebounceImmediate(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnNext(Value3);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value1, Value3]);
    }

    /// <summary>Verifies that <c>DebounceImmediate</c> flushes pending values then completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateCompletesWithPending_ThenFlushesThenCompletes()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;
        using var sub = subject.DebounceImmediate(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add, () => completed = true);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Value1, Value2]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>DebounceImmediate</c> flushes pending values then forwards errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceImmediateSourceErrors_ThenFlushesThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.DebounceImmediate(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add, ex => caught = ex);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsCollectionEqualTo([Value1, Value2]);
    }

    /// <summary>Verifies that <c>DebounceUntil</c> emits values that satisfy the condition immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilConditionTrue_ThenImmediate()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.DebounceUntil(TimeSpan.FromTicks(WindowTicks), static x => x >= Value3, scheduler).Subscribe(results.Add);
        subject.OnNext(Value3);
        await Assert.That(results).IsCollectionEqualTo([Value3]);
    }

    /// <summary>Verifies that <c>DebounceUntil</c> debounces values that don't satisfy the condition.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilConditionFalse_ThenDebounced()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.DebounceUntil(TimeSpan.FromTicks(WindowTicks), static _ => false, scheduler).Subscribe(results.Add);
        subject.OnNext(Value1);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
    }

    /// <summary>Verifies that <c>Schedule(this T value, TimeSpan, ISequencer)</c> emits the value after the delay.</summary>
    /// <remarks>The operator preserves the original <c>Observable.Create</c>-based semantics —
    /// the scheduled callback emits <c>OnNext</c> only; <c>OnCompleted</c> is not signalled.</remarks>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleValueWithDelay_ThenEmitsAfterDelay()
    {
        var scheduler = new VirtualClock();
        var results = new List<int>();
        using var sub = Value1.Schedule(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add);
        await Assert.That(results).IsEmpty();
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
    }

    /// <summary>Verifies that <c>Schedule(this T value, DateTimeOffset, ISequencer)</c> emits at the absolute time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleValueAbsolute_ThenEmitsAtTime()
    {
        var scheduler = new VirtualClock();
        var results = new List<int>();
        var due = scheduler.Now.AddTicks(WindowTicks);
        using var sub = Value1.Schedule(due, scheduler).Subscribe(results.Add);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
    }

    /// <summary>Verifies that <c>Schedule(this IObservable&lt;T&gt;, TimeSpan, ISequencer)</c>
    /// dispatches each <c>OnNext</c> via the scheduler after the configured delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleSourceWithDelay_ThenEmitsAfterDelay()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = ((IObservable<int>)subject).Schedule(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(Value1);
        await Assert.That(results).IsEmpty();
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
    }

    /// <summary>Verifies that <c>Schedule(this IObservable&lt;T&gt;, DateTimeOffset, ISequencer)</c> dispatches each <c>OnNext</c> at the absolute time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleSourceAbsolute_ThenEmitsAtTime()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        var due = scheduler.Now.AddTicks(WindowTicks);
        using var sub = ((IObservable<int>)subject).Schedule(due, scheduler).Subscribe(results.Add);
        subject.OnNext(Value2);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value2]);
    }

    /// <summary>Verifies that <c>LatestOrDefault</c> emits the default seed first, then distinct values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefault_ThenSeedThenDistinctValues()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.LatestOrDefault(Fallback).Subscribe(results.Add);
        subject.OnNext(Fallback);
        subject.OnNext(Value1);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnCompleted();
        await Assert.That(results).IsCollectionEqualTo([Fallback, Value1, Value2]);
    }

    /// <summary>Verifies that <c>LatestOrDefault</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefaultSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.LatestOrDefault(Fallback).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>Pairwise</c> produces adjacent pairs from the source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwise_ThenAdjacentPairs()
    {
        var subject = new Subject<int>();
        var results = new List<(int Previous, int Current)>();
        using var sub = subject.Pairwise().Subscribe(results.Add);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnNext(Value3);
        await Assert.That(results).IsCollectionEqualTo([(Value1, Value2), (Value2, Value3)]);
    }

    /// <summary>Verifies that <c>Pairwise</c> emits nothing for a single-element source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwiseSingleElement_ThenEmpty()
    {
        var subject = new Subject<int>();
        var results = new List<(int Previous, int Current)>();
        var completed = false;
        using var sub = subject.Pairwise().Subscribe(results.Add, () => completed = true);
        subject.OnNext(Value1);
        subject.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>Pairwise</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwiseSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.Pairwise().Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>WaitUntil</c> emits the first matching value and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilMatches_ThenEmitsAndCompletes()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;
        using var sub = subject.WaitUntil(static x => x >= Value3).Subscribe(results.Add, () => completed = true);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        subject.OnNext(Value3);
        subject.OnNext(Fallback);
        await Assert.That(results).IsCollectionEqualTo([Value3]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>WaitUntil</c> forwards source errors before a match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.WaitUntil(static _ => false).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SwitchIfEmpty</c> emits the fallback when the source completes empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptySourceEmpty_ThenEmitsFallback()
    {
        var results = new List<int>();
        var completed = false;
        using var sub = Observable.Empty<int>().SwitchIfEmpty(Observable.Return(Fallback)).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([Fallback]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>SwitchIfEmpty</c> passes the source through when it emits.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptySourceNonEmpty_ThenPassthrough()
    {
        var results = new List<int>();
        var completed = false;
        using var sub = Observable.Return(Value1).SwitchIfEmpty(Observable.Return(Fallback)).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>SwitchIfEmpty</c> forwards source errors without switching.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptySourceErrors_ThenForwardsError()
    {
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = Observable.Throw<int>(expected).SwitchIfEmpty(Observable.Return(Fallback)).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
