// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests immediate delivery, scheduled notifications, and subscription disposal.</summary>
public class ObserveOnObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Second sentinel value.</summary>
    private const int SecondValue = 2;

    /// <summary>Verifies the immediate scheduler forwards straight through, without queue-and-drain.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenImmediateScheduler_ThenForwardsDirectly()
    {
        Subject<int> source = new();
        List<int> values = [];
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
        VirtualClock scheduler = new();
        Subject<int> source = new();
        List<int> values = [];
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
        VirtualClock scheduler = new();
        Subject<int> source = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(
            static _ => { },
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
        VirtualClock scheduler = new();
        Subject<int> source = new();
        var completed = false;
        using var sub = source.ObserveOnSafe(scheduler).Subscribe(
            static _ => { },
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
        VirtualClock scheduler = new();
        Subject<int> source = new();
        List<int> values = [];
        var sub = source.ObserveOnSafe(scheduler).Subscribe(values.Add);
        source.OnNext(1);
        await Assert.That(source.HasObservers).IsTrue();
        sub.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        scheduler.AdvanceBy(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies the upstream handle is disposed when the source terminates during subscribe, before the sink records it.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceTerminatesDuringSubscribe_ThenLateAttachDisposesSubscription()
    {
        InvalidOperationException expected = new(SourceErrorMessage);
        SyncErroringObservable<int> source = new(expected);
        Exception? caught = null;
        using var sub = source.ObserveOnSafe(new InlineScheduler()).Subscribe(
            static _ => { },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(source.Subscription.IsDisposed).IsTrue();
    }

    /// <summary>Observable that errors during <c>Subscribe</c> and exposes the handle it returned.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to emit synchronously.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2315:A type that owns a disposable should be disposable",
        Justification = "The operator under test owns disposal of the exposed handle; this double only hands it back.")]
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

    /// <summary>Runs work inline while retaining the operator's queued delivery path.</summary>
    private sealed class InlineScheduler : ISequencer
    {
        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.MinValue;

        /// <inheritdoc/>
        public long Timestamp => Now.UtcTicks;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => item.Execute();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification = "Both ISequencer Schedule overloads must exist and run inline; neither can forward to the other.")]
        public void Schedule(IWorkItem item, long dueTimestamp) => item.Execute();
    }
}
