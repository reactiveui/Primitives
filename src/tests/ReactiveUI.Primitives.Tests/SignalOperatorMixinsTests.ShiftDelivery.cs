// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Shift</c> delivers due notifications without holding a lock while the observer runs.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>
    /// An observer that marshals to another thread which completes the source is not deadlocked, and the completion is
    /// delivered on the next due tick.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftObserverMarshallingWhileTheOtherThreadCompletesDoesNotDeadlock()
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
        var dueTime = TimeSpan.FromTicks(Ten);
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Shift(dueTime, sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        sequencer.Advance(dueTime);
        var worker = BackgroundThread.Start(sequencer.RunPending);
        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        sequencer.Advance(dueTime);
        sequencer.RunPending();

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>An error raised from another thread while a due value is delivered does not wait for the observer and is delivered after the value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftErrorRaisedDuringDeliveryFollowsTheValue()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        ManualSequencer sequencer = new();
        IObserver<int>? source = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("shift-delivery-error");
        var dueTime = TimeSpan.FromTicks(Ten);
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .Shift(dueTime, sequencer)
            .Subscribe(downstream);

        source!.OnNext(One);
        sequencer.Advance(dueTime);
        var owner = BackgroundThread.Start(sequencer.RunPending);
        inside.Wait();
        await BackgroundThread.Start(() => source!.OnError(expected));
        release.Set();
        await owner;
        sequencer.Advance(dueTime);
        sequencer.RunPending();

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }
}
