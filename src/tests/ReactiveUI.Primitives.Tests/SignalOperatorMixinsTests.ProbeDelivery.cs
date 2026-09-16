// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Probe</c> delivers samples without holding a lock while the observer runs.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>An observer that marshals to another thread which completes the source is not deadlocked by the sample delivery, and completion follows the sample.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeObserverMarshallingWhileTheOtherThreadCompletesDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Add(value);
            dispatcher.Invoke(() => source!.OnCompleted());
        });
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        var worker = BackgroundThread.Start(sequencer.RunPending);

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>A stale sample tick that finds no new value since the last sample delivers nothing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeStaleTickWithoutANewValueDeliversNothing()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> downstream = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        sequencer.RunPending();
        sequencer.RunStaleTick();

        await Assert.That(downstream.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>Completion delivers the value waiting since the last tick, and a later stale tick delivers nothing further.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeEarlierTickFiringAfterCompletionDeliversNothing()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> downstream = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        sequencer.RunPending();
        source.OnNext(Two);
        source.OnCompleted();
        sequencer.RunStaleTick();

        await Assert.That(downstream.Values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>A value that arrives before the first tick is delivered when the source completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeCompletionDeliversTheValueStillWaiting()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> downstream = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        source.OnCompleted();

        await Assert.That(downstream.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>Completion with no value waiting since the last tick delivers only the completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeCompletionWithNothingWaitingDeliversOnlyCompletion()
    {
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        RecordingWitness<int> downstream = new();
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        sequencer.RunPending();
        source.OnCompleted();

        await Assert.That(downstream.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>An error raised from another thread while a sample is delivered does not wait for the observer and follows the sample.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ProbeErrorRaisedDuringSampleDeliveryFollowsTheSample()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("probe-delivery-error");
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Probe(TimeSpan.FromTicks(Ten), sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        var owner = BackgroundThread.Start(sequencer.RunPending);
        inside.Wait();
        await BackgroundThread.Start(() => source!.OnError(expected));
        await Assert.That(downstream.Error).IsNull();
        release.Set();
        await owner;

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }
}
