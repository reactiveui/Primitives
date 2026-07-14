// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the sampling and flat-map coordinators hold the Rx grammar when a source breaks it: a stale timer
/// tick, a notification after a terminal, and a notification after disposal are all dropped instead of
/// forwarded. Sources here are scripted so that disposing the subscription does not unhook them, which is what
/// lets the test deliver the illegal notifications the guards exist for.
/// </summary>
public partial class SignalOperatorParityMixinsTests
{
    /// <summary>The sample period used by the probe guard tests.</summary>
    private static readonly TimeSpan ProbePeriod = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies a stale sample tick does not re-emit, and disposal stops later samples.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeIgnoresAStaleTimerTickAndStopsSamplingAfterDisposal()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(ProbePeriod, sequencer)
            .Subscribe(witness);
        source!.OnNext(First);
        sequencer.RunPending();
        sequencer.RunStaleTick();
        await Assert.That(witness.Values.SequenceEqual([First])).IsTrue();
        subscription.Dispose();
        source.OnNext(Second);
        sequencer.RunPending();
        await Assert.That(witness.Values.SequenceEqual([First])).IsTrue();
    }

    /// <summary>Verifies the sampler drops every notification a source delivers after its terminal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeDropsNotificationsDeliveredAfterTheSourceTerminates()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(ProbePeriod, sequencer)
            .Subscribe(witness);
        source!.OnCompleted();
        source.OnCompleted();
        source.OnError(new InvalidOperationException("late"));
        source.OnNext(First);
        sequencer.RunPending();
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
        await Assert.That(witness.Values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a disposed flat-map forwards nothing from either the outer or the active inner source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FlatMapDropsOuterAndInnerNotificationsAfterTheSubscriptionIsDisposed()
    {
        IObserver<int>? outer = null;
        IObserver<int>? inner = null;
        RecordingWitness<int> witness = new();
        var subscription = new ScriptedObservable<int>(observer => outer = observer)
            .FlatMap<int, int>(_ => new ScriptedObservable<int>(observer => inner = observer))
            .Subscribe(witness);
        outer!.OnNext(First);
        inner!.OnNext(Second);
        subscription.Dispose();
        inner.OnNext(Third);
        inner.OnCompleted();
        outer.OnNext(Fourth);
        outer.OnError(new InvalidOperationException("late"));
        await Assert.That(witness.Values.SequenceEqual([Second])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(0);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }
}
