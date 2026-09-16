// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>TakeUntil</c> serializes deliveries without holding a lock while the observer runs.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>An observer that marshals to another thread which fires the stop signal is not deadlocked, and completion follows the value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeUntilObserverMarshallingWhileTheOtherThreadStopsTheSequenceDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        IObserver<int>? source = null;
        IObserver<int>? stop = null;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Add(value);
            dispatcher.Invoke(() => stop!.OnNext(One));
        });
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .TakeUntil(new ScriptedObservable<int>(observer => stop = observer))
            .Subscribe(downstream);

        var worker = BackgroundThread.Start(() => source!.OnNext(One));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>
    /// A stop signal raised from another thread while a value is delivered does not wait for the observer, completes after
    /// the value, and drops values pushed after it.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeUntilStopRaisedDuringDeliveryCompletesAfterTheValue()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? source = null;
        IObserver<int>? stop = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        using var subscription = new ScriptedObservable<int>(observer => source = observer)
            .TakeUntil(new ScriptedObservable<int>(observer => stop = observer))
            .Subscribe(downstream);

        var owner = BackgroundThread.Start(() => source!.OnNext(One));
        inside.Wait();
        await BackgroundThread.Start(() => stop!.OnNext(One));
        await BackgroundThread.Start(() => source!.OnNext(Two));
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>A stop-signal error raised before disposal while another thread delivers is still delivered, and disposal does not wait.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeUntilErrorRaisedBeforeDisposeIsStillDelivered()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? source = null;
        IObserver<int>? stop = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("take-until-delivery-error");
        var subscription = new ScriptedObservable<int>(observer => source = observer)
            .TakeUntil(new ScriptedObservable<int>(observer => stop = observer))
            .Subscribe(downstream);

        var owner = BackgroundThread.Start(() => source!.OnNext(One));
        inside.Wait();
        await BackgroundThread.Start(() => stop!.OnError(expected));
        var disposer = BackgroundThread.Start(subscription.Dispose);
        await Assert.That(await BackgroundThread.FinishesPromptly(disposer)).IsTrue();
        release.Set();
        await owner;

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }
}
