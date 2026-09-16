// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that the fork-join witness serializes deliveries without holding a lock while user code runs.</summary>
public sealed partial class ForkJoinWitnessTests
{
    /// <summary>The number of values each side pushes in the contention test.</summary>
    private const int ContendedValuesPerSide = 2_000;

    /// <summary>The multiplier the joining selectors apply to the left value.</summary>
    private const int OneHundred = 100;

    /// <summary>A value pushed after a side completed.</summary>
    private const int Late = 5;

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a source, and
    /// the joined result is delivered once.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForkJoinWitnessObserverMarshallingToAnotherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            new ForkJoinSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () => PrimeBothButTheRightCompletion(left!, right!),
            () => right!.OnCompleted(),
            () => left!.OnNext(Late),
            "102");
    }

    /// <summary>
    /// A selector that marshals synchronously to another thread is not deadlocked when that thread pushes a source, and
    /// the joined result is delivered once.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForkJoinWitnessSelectorMarshallingToAnotherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedSelectorMarshallingDoesNotDeadlock(
            selector => new ForkJoinSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                selector),
            () => PrimeBothButTheRightCompletion(left!, right!),
            () => right!.OnCompleted(),
            () => left!.OnNext(Late),
            "102");
    }

    /// <summary>Both sides pushing and completing from separate threads deliver one result joining each side's last value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForkJoinWitnessConcurrentSourcesJoinTheLastValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        ConcurrentQueue<(int Left, int Right)> values = new();
        CallbackRecordingWitness<(int Left, int Right)> downstream = new(values.Enqueue);

        using var subscription = new ForkJoinWitness<int, int, (int, int)>(downstream, static (a, b) => (a, b)).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));

        await Task.WhenAll(
            BackgroundThread.Start(() => PushSequenceAndComplete(left!)),
            BackgroundThread.Start(() => PushSequenceAndComplete(right!)));

        await Assert.That(values.Count).IsEqualTo(One);
        await Assert.That(values.TryPeek(out var result) ? result : default).IsEqualTo((ContendedValuesPerSide, ContendedValuesPerSide));
        await Assert.That(downstream.IsCompleted).IsTrue();
    }

    /// <summary>An error or value raised while the joined result is being delivered is dropped, and completion follows the result.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForkJoinWitnessNotificationsRaisedWhileTheResultIsDeliveredAreDropped()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Add(value);
            inside.Set();
            release.Wait();
        });

        using var subscription = new ForkJoinWitness<int, int, int>(downstream, static (a, b) => (a * OneHundred) + b).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));
        PrimeBothButTheRightCompletion(left!, right!);
        var owner = BackgroundThread.Start(right!.OnCompleted);
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            left!.OnError(new InvalidOperationException("late"));
            right.OnNext(Late);
        });
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("102");
        await Assert.That(downstream.Error).IsNull();
        await Assert.That(downstream.IsCompleted).IsTrue();
    }

    /// <summary>Completion with an empty left source, completed after the right, completes without a result.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForkJoinWitnessCompletesWithoutResultWhenTheLeftIsEmpty()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        right.Observer!.OnNext(Two);
        right.Observer.OnCompleted();
        left.Observer!.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Gives the left source a value and completion, and the right source a value.</summary>
    /// <param name="left">The left source observer.</param>
    /// <param name="right">The right source observer.</param>
    private static void PrimeBothButTheRightCompletion(IObserver<int> left, IObserver<int> right)
    {
        left.OnNext(One);
        left.OnCompleted();
        right.OnNext(Two);
    }

    /// <summary>Pushes an ascending sequence from one up to the contention count, then completes.</summary>
    /// <param name="observer">The source observer to push into.</param>
    private static void PushSequenceAndComplete(IObserver<int> observer)
    {
        for (var value = 1; value <= ContendedValuesPerSide; value++)
        {
            observer.OnNext(value);
        }

        observer.OnCompleted();
    }
}
