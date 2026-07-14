// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the operator coordinators hold the Rx grammar even when a source breaks it: a notification that
/// arrives after the sequence has terminated, or after the subscription was disposed, is dropped rather than
/// forwarded. Sources here are scripted so that disposing the subscription does not unhook them, which is what
/// lets the test deliver the illegal notifications the guards exist for.
/// </summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>The quiet period used by the timer-driven guard tests.</summary>
    private static readonly TimeSpan GuardPeriod = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies <c>Pair</c> drops values that arrive after the paired sequence has completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PairDropsValuesDeliveredAfterTheZippedSequenceCompletes()
    {
        IObserver<int>? left = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .Pair(Signal.None<int>(), static (first, second) => first + second)
            .Subscribe(witness);
        await Assert.That(witness.Completed).IsEqualTo(1);
        left!.OnNext(One);
        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies disposing a <c>Blend</c> subscription stops values from the inner sequences.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BlendStopsForwardingInnerValuesOnceTheSubscriptionIsDisposed()
    {
        Signal<IObservable<int>> outer = new();
        Signal<int> inner = new();
        RecordingWitness<int> witness = new();
        var subscription = outer.Blend().Subscribe(witness);
        outer.OnNext(inner);
        inner.OnNext(One);
        subscription.Dispose();
        inner.OnNext(Two);
        await Assert.That(witness.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(inner.HasObservers).IsFalse();
    }

    /// <summary>Verifies <c>Shift</c> drops values a source delivers after it has already completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftDropsValuesDeliveredAfterTheSourceCompletes()
    {
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Shift(TimeSpan.Zero, Sequencer.CurrentThread)
            .Subscribe(witness);
        source!.OnNext(One);
        source.OnCompleted();
        source.OnNext(Two);
        await Assert.That(witness.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies <c>Calm</c> subscribes through the current-thread trampoline and emits the quiet value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CalmOnTheCurrentThreadSequencerEmitsTheQuietValue()
    {
        Signal<int> source = new();
        List<int> values = [];
        using var subscription = source.Calm(TimeSpan.Zero, Sequencer.CurrentThread).Subscribe(values.Add);
        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();
        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies a stale <c>Calm</c> timer tick does not re-emit the value it already delivered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CalmIgnoresAStaleTimerTickAndPostTerminalNotifications()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Calm(GuardPeriod, sequencer)
            .Subscribe(witness);
        source!.OnNext(One);
        sequencer.Advance(GuardPeriod);
        sequencer.RunPending();
        sequencer.RunStaleTick();
        await Assert.That(witness.Values.SequenceEqual([One])).IsTrue();
        source.OnCompleted();
        source.OnCompleted();
        source.OnError(new InvalidOperationException("late"));
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }

    /// <summary>Verifies disposing a <c>Reattempt</c> subscription stops the retried source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReattemptStopsForwardingOnceTheSubscriptionIsDisposed()
    {
        Signal<int> source = new();
        RecordingWitness<int> witness = new();
        var subscription = source.Reattempt(Two).Subscribe(witness);
        source.OnNext(One);
        subscription.Dispose();
        source.OnNext(Two);
        await Assert.That(witness.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
    }
}
