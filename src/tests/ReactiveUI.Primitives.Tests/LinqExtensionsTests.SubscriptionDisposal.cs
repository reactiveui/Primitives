// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests disposal during scheduled operator callbacks.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>Disposal before an accepted inner subscription starts prevents subscribing to that source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FlatMap_DisposedBeforeInnerSubscribe_DoesNotAttachInnerSource()
    {
        using Signal<int> source = new();
        using Signal<int> inner = new();
        RecordingWitness<int> observer = new();
        LinqExtensions.FlatMapCoordinator<int, int> coordinator = new(source, _ => inner, observer);
        using var subscription = coordinator.Run();

        coordinator.Dispose();
        coordinator.SubscribeInner(inner);
        inner.OnNext(1);

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(inner.HasObservers).IsFalse();
        await Assert.That(observer.Values.Count).IsEqualTo(0);
    }

    /// <summary>Disposing a FlatMap coordinator before it runs leaves nothing subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FlatMap_DisposedBeforeRun_LeavesNothingSubscribed()
    {
        using Signal<int> source = new();
        using Signal<int> inner = new();
        RecordingWitness<int> observer = new();
        LinqExtensions.FlatMapCoordinator<int, int> coordinator = new(source, _ => inner, observer);

        coordinator.Dispose();
        using var subscription = coordinator.Run();

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>An outer source that fails while subscribing forwards its error with no inner source attached.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FlatMap_OuterFailsDuringSubscribe_ForwardsError()
    {
        InvalidOperationException error = new("outer");
        RecordingWitness<int> observer = new();

        using var subscription = new ScriptedObservable<int>(outer => outer.OnError(error))
            .FlatMap(static _ => Signal.Emit(1))
            .Subscribe(observer);

        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
    }

    /// <summary>An outer error while an inner source is active releases both subscriptions before forwarding the error.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FlatMap_OuterFailsWhileInnerActive_ReleasesBothSubscriptions()
    {
        using Signal<int> source = new();
        using Signal<int> inner = new();
        RecordingWitness<int> observer = new();
        InvalidOperationException error = new("outer");
        using var subscription = source.FlatMap(_ => inner).Subscribe(observer);

        source.OnNext(1);
        source.OnError(error);

        await Assert.That(inner.HasObservers).IsFalse();
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
    }

    /// <summary>An inner source that completes twice advances the coordinator once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FlatMap_InnerCompletesTwice_CompletesDownstreamOnce()
    {
        using Signal<int> source = new();
        IObserver<int>? innerObserver = null;
        RecordingWitness<int> observer = new();
        using var subscription = source
            .FlatMap(_ => new ScriptedObservable<int>(captured => innerObserver = captured))
            .Subscribe(observer);

        source.OnNext(1);
        innerObserver!.OnCompleted();
        innerObserver.OnCompleted();
        source.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>A probe disposes the timer returned after an inline callback has already disposed the subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Probe_DisposedDuringInlineTick_DisposesLateTimerHandle()
    {
        const int IgnoredValue = 2;
        InlineProbeSequencer sequencer = new();
        using Signal<int> source = new();
        List<int> values = [];
        IDisposable? subscription = null;
        subscription = source.Probe(TimeSpan.Zero, sequencer).Subscribe(value =>
        {
            values.Add(value);
            subscription!.Dispose();
        });
        using var cleanup = subscription;

        source.OnNext(1);
        source.OnNext(IgnoredValue);

        await Assert.That(values.SequenceEqual([1])).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(sequencer.ScheduledItemDisposed).IsTrue();
    }

    /// <summary>A terminal callback during a timer's clock read prevents the captured value from following the error.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Calm_SourceErrorsDuringClockRead_DropsCapturedValue()
    {
        ClockCallbackSequencer sequencer = new();
        using Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Calm(TimeSpan.Zero, sequencer).Subscribe(observer);
        source.OnNext(1);
        InvalidOperationException error = new("source");
        sequencer.OnClockRead = () => source.OnError(error);

        sequencer.RunPending();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>Runs the scheduled probe immediately and retains its cancellation state for inspection.</summary>
    private sealed class InlineProbeSequencer : ISequencer
    {
        /// <summary>The item executed inline.</summary>
        private IWorkItem? _item;

        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <summary>Gets whether the executed item was cancelled after scheduling returned.</summary>
        internal bool ScheduledItemDisposed => _item is IsDisposed { IsDisposed: true };

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            _item = item;
            item.Execute();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);
    }

    /// <summary>Runs a source terminal callback during the next clock read inside a manually driven timer.</summary>
    private sealed class ClockCallbackSequencer : ISequencer
    {
        /// <summary>The timer waiting for explicit execution.</summary>
        private IWorkItem? _pending;

        /// <inheritdoc/>
        public DateTimeOffset Now
        {
            get
            {
                var callback = OnClockRead;
                OnClockRead = null;
                callback?.Invoke();
                return DateTimeOffset.UnixEpoch;
            }
        }

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <summary>Gets or sets the callback run on the next clock read.</summary>
        internal Action? OnClockRead { get; set; }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => _pending = item;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Executes the pending timer once.</summary>
        internal void RunPending()
        {
            var item = _pending;
            _pending = null;
            item!.Execute();
        }
    }
}
