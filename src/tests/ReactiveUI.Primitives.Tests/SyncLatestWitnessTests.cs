// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SyncLatestWitness{TLeft, TRight, TResult}"/>.</summary>
public sealed class SyncLatestWitnessTests
{
    /// <summary>The first source value.</summary>
    private const int One = 1;

    /// <summary>The second source value.</summary>
    private const int Two = 2;

    /// <summary>The multiplier the combining selectors apply to the left value.</summary>
    private const int OneHundred = 100;

    /// <summary>Completion waits for both sources, and a repeated completion delivers nothing more.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessCompletesOnlyOnceBothSourcesComplete()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new SyncLatestWitness<int, int, int>(downstream, static (a, b) => a + b).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));
        left!.OnNext(One);
        right!.OnNext(Two);
        left.OnCompleted();
        var completedBeforeRight = downstream.Completed;
        right.OnCompleted();
        right.OnCompleted();

        await Assert.That(completedBeforeRight).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(One);
        await Assert.That(downstream.Values.SequenceEqual([One + Two])).IsTrue();
    }

    /// <summary>Only the first error reaches the observer, and nothing is delivered after it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessDeliversOnlyTheFirstError()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");

        using var subscription = new SyncLatestWitness<int, int, int>(downstream, static (a, b) => a + b).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));
        left!.OnNext(One);
        right!.OnError(expected);
        left.OnError(new InvalidOperationException("second"));
        right.OnNext(Two);

        await Assert.That(downstream.Errors.Count).IsEqualTo(One);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the other
    /// source, and the handed-over combination is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessObserverMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () => right!.OnNext(One),
            () => left!.OnNext(One),
            () => right!.OnNext(Two),
            "101,102");
    }

    /// <summary>
    /// A selector that marshals synchronously to another thread is not deadlocked when that thread pushes the other source,
    /// and the handed-over combination is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessSelectorMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedSelectorMarshallingDoesNotDeadlock(
            selector => new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                selector),
            () => right!.OnNext(One),
            () => left!.OnNext(One),
            () => right!.OnNext(Two),
            "101,102");
    }

    /// <summary>Both sides pushing from separate threads never overlap downstream, and every update produces a combination.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SyncLatestWitnessConcurrentSourcesDeliverEveryCombinationWithoutOverlap() =>
        MergeDeliveryAssertions.LatestSourcesDeliverEveryCombinationWithoutOverlap(
            static (left, right, observer) => new SyncLatestWitness<int, int, (int Left, int Right)>(observer, static (a, b) => (a, b)).Run(left, right));

    /// <summary>An error raised while another thread delivers follows the combinations queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessErrorRaisedDuringDeliveryFollowsTheQueuedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedErrorRaisedDuringDeliveryFollowsTheQueuedValues(
            new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () => right!.OnNext(0),
            () => left!.OnNext(One),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!,
            "100,101,102");
    }

    /// <summary>Completion raised while another thread delivers follows the combinations queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessCompletionRaisedDuringDeliveryFollowsTheQueuedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedCompletionRaisedDuringDeliveryFollowsTheQueuedValues(
            new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () => right!.OnNext(0),
            () => left!.OnNext(One),
            () =>
            {
                right!.OnNext(One);
                right.OnCompleted();
                left!.OnCompleted();
            },
            "100,101");
    }

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessValuePushedWhileTheTerminalWaitsIsDropped()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedValuePushedWhileTheTerminalWaitsIsDropped(
            new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => a + b),
            () => right!.OnNext(0),
            () => left!.OnNext(One),
            () => right!,
            () => right!.OnNext(Two));
    }

    /// <summary>An observer that throws on a direct delivery leaves the witness able to deliver the next combination.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncLatestWitnessObserverThrowingOnDirectDeliveryLeavesItUsable()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(
            new SyncLatestSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => a + b),
            () => left!.OnNext(One),
            () => right!.OnNext(One),
            () => right!.OnNext(Two));
    }
}
