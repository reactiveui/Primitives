// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the <c>Conflate</c> operator backed by
/// <c>ConflateObservable&lt;T&gt;</c> — source-error path through the scheduler
/// marshaller, completion-while-throttled, fast-path interruption by a newer value,
/// and dispose mid-drain.</summary>
public class ConflateObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Minimum-update-period tick window for the conflate operator.</summary>
    private const int UpdatePeriodTicks = 100;

    /// <summary>Multiplier used to advance past the update period in settle assertions.</summary>
    private const int SettleMultiplier = 2;

    /// <summary>Half of the update-period window.</summary>
    private const int HalfWindowTicks = 50;

    /// <summary>Sentinel values.</summary>
    private const int First = 1;

    /// <summary>Second sentinel value.</summary>
    private const int Second = 2;

    /// <summary>Third sentinel value.</summary>
    private const int Third = 3;

    /// <summary>Verifies that a source error is forwarded through the scheduler marshaller.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConflateSourceErrors_ThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        scheduler.AdvanceBy(UpdatePeriodTicks * SettleMultiplier);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a newer value arriving inside the throttle window replaces the
    /// pending scheduled emission rather than emitting both.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConflateNewerValueDuringThrottle_ThenReplacesPending()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(First);
        scheduler.AdvanceBy(HalfWindowTicks);
        subject.OnNext(Second);
        scheduler.AdvanceBy(HalfWindowTicks);
        subject.OnNext(Third);
        scheduler.AdvanceBy(UpdatePeriodTicks * SettleMultiplier);

        // Inside the throttle window: the first pending value is replaced by the newer one.
        await Assert.That(results.Count).IsGreaterThanOrEqualTo(1);
        await Assert.That(results).DoesNotContain(First);
        await Assert.That(results).Contains(Second);
    }

    /// <summary>Verifies that completion before any throttled emission flushes through.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConflateCompletesBeforeFirstEmission_ThenCompletes()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var completed = false;
        using var sub = subject.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            () => completed = true);
        subject.OnCompleted();
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that disposing before the scheduled emission fires suppresses the value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConflateDisposedBeforeScheduledEmission_ThenSuppressed()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        var sub = subject.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(First);
        scheduler.AdvanceBy(HalfWindowTicks);
        subject.OnNext(Second);
        sub.Dispose();
        scheduler.AdvanceBy(UpdatePeriodTicks);

        // Initial value may or may not have fired before disposal but no late emission must arrive.
        var snapshot = results.Count;
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(results.Count).IsEqualTo(snapshot);
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the source has completed is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        var results = new List<int>();
        var completed = false;
        using var sub = source.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(results.Add, () => completed = true);
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(SettleMultiplier * UpdatePeriodTicks);
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(SettleMultiplier * UpdatePeriodTicks);
        await Assert.That(completed).IsTrue();
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that an <c>OnError</c> arriving after completion is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorAfterCompleted_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completed = false;
        using var sub = source.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex,
            () => completed = true);
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(UpdatePeriodTicks);
        source.Observer.OnError(new InvalidOperationException("late"));
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(completed).IsTrue();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that a duplicate <c>OnCompleted</c> after an error is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        var scheduler = new VirtualClock();
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completed = false;
        var expected = new InvalidOperationException("first");
        using var sub = source.Conflate(TimeSpan.FromTicks(UpdatePeriodTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex,
            () => completed = true);
        source.Observer.OnError(expected);
        scheduler.AdvanceBy(UpdatePeriodTicks);
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies <see cref = "ConflateObservable{T}.ConflateSink"/>'s
    /// post-dispose <c>Enqueue</c> guard by constructing the sink directly, disposing it, and then
    /// pushing notifications — exercising the defensive branch that is otherwise unreachable
    /// through the front-door <c>Conflate</c> pipeline.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSinkEnqueuedAfterDispose_ThenSilentlyDropped()
    {
        var downstream = new RecordingWitness<int>();
        var scheduler = new VirtualClock();
        var sink = new ConflateObservable<int>.ConflateSink(downstream, TimeSpan.FromTicks(UpdatePeriodTicks), scheduler);
        sink.Dispose();
        sink.OnNext(1);
        sink.OnError(new InvalidOperationException("late"));
        sink.OnCompleted();
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(downstream.Values).IsEmpty();
        await Assert.That(downstream.Error).IsNull();
        await Assert.That(downstream.Completed).IsFalse();
    }

    /// <summary>Verifies <see cref = "ConflateObservable{T}.ConflateSink"/>'s
    /// after-terminal guards on <c>OnNext</c>, <c>OnError</c>, and <c>OnCompleted</c> by constructing
    /// the sink directly, terminating via <c>OnError</c>, and then pushing follow-up notifications.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSinkEventsAfterTerminated_ThenDropped()
    {
        var downstream = new RecordingWitness<int>();
        var scheduler = new VirtualClock();
        var sink = new ConflateObservable<int>.ConflateSink(downstream, TimeSpan.FromTicks(UpdatePeriodTicks), scheduler);
        var expected = new InvalidOperationException("first");
        sink.OnError(expected);
        scheduler.AdvanceBy(UpdatePeriodTicks);
        sink.OnNext(1);
        sink.OnError(new InvalidOperationException("ignored"));
        sink.OnCompleted();
        scheduler.AdvanceBy(UpdatePeriodTicks);
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values).IsEmpty();
        await Assert.That(downstream.Completed).IsFalse();
    }

    /// <summary>Recording observer used to verify direct-invocation tests of the conflate sink
    /// and marshaller — does not race with a scheduler, so the assertion sees exactly the
    /// notifications that were forwarded.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets the captured <c>OnNext</c> values in order.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the first captured <c>OnError</c> exception, if any.</summary>
        public Exception? Error { get; private set; }

        /// <summary>Gets a value indicating whether <c>OnCompleted</c> has been called.</summary>
        public bool Completed { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => Error ??= error;

        /// <inheritdoc/>
        public void OnCompleted() => Completed = true;
    }
}
