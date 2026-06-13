// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Coverage for <c>ObserveOnObservable</c> (reached via <c>ObserveOnSafe</c>) — the
/// immediate-scheduler passthrough, the queue-and-drain marshaller's value / error / completion
/// forwarding, dispose teardown, and the attach-after-terminated branch of the shared drain state.</summary>
public class ObserveOnObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Second sentinel value (kept as a constant to satisfy the no-magic-number rule).</summary>
    private const int SecondValue = 2;

    /// <summary>Verifies the immediate scheduler is special-cased to forward straight through the source
    /// subscription without the queue-and-drain machinery.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenImmediateScheduler_ThenForwardsDirectly()
    {
        var source = new Subject<int>();
        var values = new List<int>();
        var completed = false;
        using var sub = source.ObserveOnSafe(Sequencer.Immediate).Subscribe(values.Add, () => completed = true);
        source.OnNext(1);
        source.OnNext(SecondValue);
        source.OnCompleted();
        await Assert.That(values).IsCollectionEqualTo([1, SecondValue]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies queued values are drained downstream in FIFO order on the scheduler thread.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenValuesMarshalled_ThenForwardedInOrderOnDrain()
    {
        var scheduler = new VirtualClock();
        var source = new Subject<int>();
        var values = new List<int>();
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(values.Add);
        source.OnNext(1);
        source.OnNext(SecondValue);

        // Nothing forwarded until the scheduled drain pass runs.
        await Assert.That(values).IsEmpty();
        scheduler.AdvanceBy(1);
        await Assert.That(values).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Verifies a source error is forwarded through the scheduler marshaller.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceErrors_ThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var source = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        source.OnError(expected);
        scheduler.AdvanceBy(1);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies source completion is forwarded through the scheduler marshaller.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletes_ThenForwardsCompletion()
    {
        var scheduler = new VirtualClock();
        var source = new Subject<int>();
        var completed = false;
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(
            static _ =>
        {
        },
            () => completed = true);
        source.OnCompleted();
        scheduler.AdvanceBy(1);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies disposing tears down the upstream subscription and stops forwarding queued values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedBeforeDrain_ThenTearsDownAndDropsQueued()
    {
        var scheduler = new VirtualClock();
        var source = new Subject<int>();
        var values = new List<int>();
        var sub = source.ObserveOnSafe(scheduler).Subscribe(values.Add);
        source.OnNext(1);
        await Assert.That(source.HasObservers).IsTrue();
        sub.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        scheduler.AdvanceBy(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies the upstream subscription is disposed when the source terminates synchronously during
    /// subscribe — the drain runs inline (terminating the sink) before <c>AttachSourceSubscription</c> records
    /// the handle, so the late attach disposes it instead.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceTerminatesDuringSubscribe_ThenLateAttachDisposesSubscription()
    {
        var expected = new InvalidOperationException(SourceErrorMessage);
        var source = new SyncErroringObservable<int>(expected);
        Exception? caught = null;
        using var sub = source.ObserveOnSafe(new InlineScheduler()).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(source.Subscription.IsDisposed).IsTrue();
    }

    /// <summary>Observable that synchronously errors during <c>Subscribe</c> and exposes the subscription
    /// handle it returned so tests can assert it was disposed.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to emit synchronously.</param>
    private sealed class SyncErroringObservable<T>(Exception error) : IObservable<T>
    {
        /// <summary>Gets the subscription handle returned from the most recent subscribe.</summary>
        public BooleanDisposable Subscription { get; } = new();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnError(error);
            return Subscription;
        }
    }

    /// <summary>Scheduler that runs scheduled work synchronously on the calling thread, so a drain pass
    /// executes inline during the schedule call. Distinct instance from <see cref = "Sequencer.Immediate"/>
    /// so the operator's immediate-scheduler passthrough does not apply.</summary>
    private sealed class InlineScheduler : ISequencer
    {
        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.MinValue;

        /// <inheritdoc/>
        public long Timestamp => Now.UtcTicks;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => item.Execute();

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => item.Execute();
    }
}
