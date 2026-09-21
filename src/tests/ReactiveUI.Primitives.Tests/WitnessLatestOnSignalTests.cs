// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the <c>WitnessLatestOn</c> signal, which keeps one pending slot and delivers only the newest value on a sequencer.</summary>
public sealed class WitnessLatestOnSignalTests
{
    /// <summary>The first value pushed through the signal.</summary>
    private const int First = 1;

    /// <summary>The second value pushed through the signal.</summary>
    private const int Second = 2;

    /// <summary>The third value pushed through the signal.</summary>
    private const int Third = 3;

    /// <summary>A single virtual tick, enough to drain the sequencer queue.</summary>
    private static readonly TimeSpan SingleTick = TimeSpan.FromTicks(1);

    /// <summary>The longest a cross-thread push may take before the sink lock is considered held.</summary>
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The signal is built directly from a source and a sequencer and subscribes on the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExtensionBuildsTheSignalThatRequiresTheCurrentThread()
    {
        VirtualClock clock = new();
        Signal<int> source = new();

        var viaExtension = source.WitnessLatestOn(clock);
        WitnessLatestOnSignal<int> direct = new(source, clock);

        await Assert.That(viaExtension).IsTypeOf<WitnessLatestOnSignal<int>>();
        await Assert.That(viaExtension.GetType()).IsEqualTo(direct.GetType());
        await Assert.That(direct.IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Values that arrive while a drain is pending overwrite the single slot, so only the newest is delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PendingValuesAreOverwrittenAndOnlyTheLatestIsDelivered()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);
        source.OnNext(Second);
        source.OnNext(Third);

        await Assert.That(log.Entries.Count).IsEqualTo(0);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N3"])).IsTrue();
    }

    /// <summary>A value that arrives after a drain finished schedules a fresh drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueAfterAFinishedDrainIsDeliveredByANewDrain()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);
        clock.AdvanceBy(SingleTick);
        source.OnNext(Second);
        clock.AdvanceBy(SingleTick);
        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "N2"])).IsTrue();
    }

    /// <summary>A value pushed re-entrantly during delivery is held, then delivered once by the same drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueArrivingDuringDeliveryIsHeldAndDeliveredOnceByTheSameDrain()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();
        log.OnValue = value =>
        {
            if (value == First)
            {
                source.OnNext(Second);
                source.OnNext(Third);
            }
        };

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "N3"])).IsTrue();

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "N3"])).IsTrue();
    }

    /// <summary>Completion that arrives during delivery is delivered after the value being delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionArrivingDuringDeliveryFollowsTheCurrentValue()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();
        log.OnValue = _ => source.OnCompleted();

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "C"])).IsTrue();
    }

    /// <summary>Completion delivers the pending value first and then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionDeliversThePendingValueThenCompletes()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);
        source.OnNext(Second);
        source.OnCompleted();

        await Assert.That(log.Entries.Count).IsEqualTo(0);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N2", "C"])).IsTrue();
    }

    /// <summary>Completion with no pending value is deferred to the sequencer and delivered alone.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionWithoutAPendingValueIsDeliveredOnTheSequencer()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnCompleted();

        await Assert.That(log.Entries.Count).IsEqualTo(0);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["C"])).IsTrue();
    }

    /// <summary>An error delivers the pending value first and then the error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorDeliversThePendingValueThenTheError()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();
        InvalidOperationException expected = new("latest");

        using var subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);
        source.OnNext(Second);
        source.OnError(expected);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N2", "E"])).IsTrue();
        await Assert.That(log.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>An error wins over a completion that arrived before it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorWinsOverAnEarlierCompletion()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        LogObserver log = new();

        using var subscription = new ScriptedObservable<int>(observer => upstream = observer)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnNext(First);
        upstream.OnCompleted();
        upstream.OnError(new InvalidOperationException("late"));

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "E"])).IsTrue();
    }

    /// <summary>A completion that arrives after an error is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletionAfterAnErrorIsDropped()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        LogObserver log = new();

        using var subscription = new ScriptedObservable<int>(observer => upstream = observer)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnError(new InvalidOperationException("first"));
        upstream.OnCompleted();
        upstream.OnNext(First);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["E"])).IsTrue();
    }

    /// <summary>Values that arrive after completion are dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValuesAfterCompletionAreDropped()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        LogObserver log = new();

        using var subscription = new ScriptedObservable<int>(observer => upstream = observer)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnCompleted();
        upstream.OnCompleted();
        upstream.OnNext(First);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["C"])).IsTrue();
    }

    /// <summary>A delivered completion disposes the upstream subscription and nothing more is delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeliveredCompletionDisposesUpstreamAndStopsDelivery()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        CountingDisposable upstreamSubscription = new();
        LogObserver log = new();

        using var subscription = new ScriptedObservable<int>(observer => upstream = observer, upstreamSubscription)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnNext(First);
        upstream.OnCompleted();

        clock.AdvanceBy(SingleTick);

        await Assert.That(upstreamSubscription.DisposeCount).IsEqualTo(1);

        upstream.OnNext(Second);
        upstream.OnCompleted();
        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "C"])).IsTrue();
        await Assert.That(upstreamSubscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A delivered error disposes the upstream subscription and nothing more is delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeliveredErrorDisposesUpstreamAndStopsDelivery()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        CountingDisposable upstreamSubscription = new();
        LogObserver log = new();

        using var subscription = new ScriptedObservable<int>(observer => upstream = observer, upstreamSubscription)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnError(new InvalidOperationException("boom"));

        clock.AdvanceBy(SingleTick);

        await Assert.That(upstreamSubscription.DisposeCount).IsEqualTo(1);

        upstream.OnNext(First);
        upstream.OnError(new InvalidOperationException("again"));
        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["E"])).IsTrue();
    }

    /// <summary>Dispose drops the pending value and disposes the upstream subscription exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDropsThePendingValueAndDisposesUpstreamOnce()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        CountingDisposable upstreamSubscription = new();
        LogObserver log = new();

        var subscription = new ScriptedObservable<int>(observer => upstream = observer, upstreamSubscription)
            .WitnessLatestOn(clock)
            .Subscribe(log);
        upstream!.OnNext(First);
        subscription.Dispose();
        subscription.Dispose();

        upstream.OnNext(Second);
        upstream.OnCompleted();
        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.Count).IsEqualTo(0);
        await Assert.That(upstreamSubscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing from inside a delivery drops the value that arrived during it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDuringDeliveryDropsTheHeldValue()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        LogObserver log = new();
        IDisposable? subscription = null;
        log.OnValue = _ =>
        {
            source.OnNext(Second);
            subscription?.Dispose();
        };

        subscription = source.WitnessLatestOn(clock).Subscribe(log);
        source.OnNext(First);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1"])).IsTrue();
    }

    /// <summary>Values and completion reach the observer without the sink's lock held, so another thread can push meanwhile.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValuesAndCompletionAreDeliveredOutsideTheSinkLock()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        List<bool> nonBlocking = [];
        LogObserver log = new();
        log.OnValue = value => nonBlocking.Add(value == First
            ? IsNonBlocking(() => upstream!.OnNext(Second))
            : IsNonBlocking(() => upstream!.OnCompleted()));
        log.OnTerminal = () => nonBlocking.Add(IsNonBlocking(() => upstream!.OnNext(Third)));

        using var subscription = new ScriptedObservable<int>(observer =>
            {
                upstream = observer;
                observer.OnNext(First);
            })
            .WitnessLatestOn(clock)
            .Subscribe(log);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "N2", "C"])).IsTrue();
        await Assert.That(nonBlocking.SequenceEqual([true, true, true])).IsTrue();
    }

    /// <summary>An error reaches the observer without the sink's lock held, so another thread can push meanwhile.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ErrorsAreDeliveredOutsideTheSinkLock()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        List<bool> nonBlocking = [];
        LogObserver log = new();
        log.OnValue = _ => nonBlocking.Add(IsNonBlocking(() => upstream!.OnError(new InvalidOperationException("gate"))));
        log.OnTerminal = () => nonBlocking.Add(IsNonBlocking(() => upstream!.OnNext(Second)));

        using var subscription = new ScriptedObservable<int>(observer =>
            {
                upstream = observer;
                observer.OnNext(First);
            })
            .WitnessLatestOn(clock)
            .Subscribe(log);

        clock.AdvanceBy(SingleTick);

        await Assert.That(log.Entries.SequenceEqual(["N1", "E"])).IsTrue();
        await Assert.That(nonBlocking.SequenceEqual([true, true])).IsTrue();
    }

    /// <summary>Runs an action on another thread and reports whether it finished, which it cannot while this thread holds the sink lock.</summary>
    /// <param name="push">The action that pushes into the sink.</param>
    /// <returns>True when the other thread finished within the timeout; otherwise, false.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsNonBlocking(Action push)
    {
        Thread pusher = new(() => push()) { IsBackground = true };
        pusher.Start();
        return pusher.Join(ProbeTimeout);
    }

    /// <summary>An observer that records values, errors and completion in arrival order.</summary>
    private sealed class LogObserver : IObserver<int>
    {
        /// <summary>Gets the ordered log; values are <c>N&lt;value&gt;</c>, errors are <c>E</c> and completion is <c>C</c>.</summary>
        internal List<string> Entries { get; } = [];

        /// <summary>Gets the recorded errors.</summary>
        internal List<Exception> Errors { get; } = [];

        /// <summary>Gets or sets the action invoked after each value is recorded.</summary>
        internal Action<int>? OnValue { get; set; }

        /// <summary>Gets or sets the action invoked after an error or completion is recorded.</summary>
        internal Action? OnTerminal { get; set; }

        /// <summary>Records a completion callback and runs <see cref="OnTerminal"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
            Entries.Add("C");
            OnTerminal?.Invoke();
        }

        /// <summary>Records an error callback and runs <see cref="OnTerminal"/>.</summary>
        /// <param name="error">The error to record.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
            Entries.Add("E");
            Errors.Add(error);
            OnTerminal?.Invoke();
        }

        /// <summary>Records a value callback and runs <see cref="OnValue"/>.</summary>
        /// <param name="value">The value to record.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value)
        {
            Entries.Add($"N{value}");
            OnValue?.Invoke(value);
        }
    }

    /// <summary>A disposable that counts how many times it has been disposed.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of dispose calls.</summary>
        internal int DisposeCount { get; private set; }

        /// <summary>Counts a dispose call.</summary>
        public void Dispose() => DisposeCount++;
    }
}
