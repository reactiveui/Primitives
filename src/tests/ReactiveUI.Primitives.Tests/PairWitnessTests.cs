// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="PairWitness{TLeft, TRight, TResult}"/>.</summary>
public sealed class PairWitnessTests
{
    /// <summary>The first two pairs when each selector returns the left value times one hundred plus the right.</summary>
    private const string FirstTwoPairs = "101,202";

    /// <summary>The first source value.</summary>
    private const int One = 1;

    /// <summary>The second source value.</summary>
    private const int Two = 2;

    /// <summary>The third source value.</summary>
    private const int Three = 3;

    /// <summary>The multiplier the pairing selectors apply to the left value.</summary>
    private const int OneHundred = 100;

    /// <summary>A completed left source with unmatched values completes only once the right source completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessWaitsForTheRightSourceWhileTheCompletedLeftHasUnmatchedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = Pair(new ScriptedObservable<int>(observer => left = observer), new ScriptedObservable<int>(observer => right = observer), downstream);
        left!.OnNext(One);
        left.OnNext(Two);
        left.OnCompleted();
        right!.OnNext(OneHundred);
        var completedBeforeRight = downstream.Completed;
        right.OnCompleted();

        await Assert.That(completedBeforeRight).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(One);
        await Assert.That(downstream.Values.SequenceEqual([OneHundred + One])).IsTrue();
    }

    /// <summary>A completed right source with unmatched values completes only once the left source completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessWaitsForTheLeftSourceWhileTheCompletedRightHasUnmatchedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = Pair(new ScriptedObservable<int>(observer => left = observer), new ScriptedObservable<int>(observer => right = observer), downstream);
        right!.OnNext(OneHundred);
        right.OnNext(OneHundred + One);
        right.OnCompleted();
        left!.OnNext(One);
        var completedBeforeLeft = downstream.Completed;
        left.OnCompleted();

        await Assert.That(completedBeforeLeft).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(One);
        await Assert.That(downstream.Values.SequenceEqual([OneHundred + One])).IsTrue();
    }

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the other
    /// source, and the pair it completes is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessObserverMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two),
            FirstTwoPairs);
    }

    /// <summary>
    /// A selector that marshals synchronously to another thread is not deadlocked when that thread pushes the other source,
    /// and the pair it completes is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessSelectorMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedSelectorMarshallingDoesNotDeadlock(
            selector => new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                selector),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two),
            FirstTwoPairs);
    }

    /// <summary>Both sides pushing from separate threads never overlap downstream, and every index produces its pair in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task PairWitnessConcurrentSourcesDeliverEveryPairInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ZippedSourcesDeliverEveryPairInOrderWithoutOverlap(
            static (left, right, observer) => new PairWitness<int, int, (int Left, int Right)>(observer, static (a, b) => (a, b)).Run(left, right));

    /// <summary>
    /// An error raised while another thread delivers follows the pairs queued before it, and wins over a completion
    /// queued before it.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessErrorRaisedDuringDeliveryFollowsTheQueuedPairs()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedErrorRaisedDuringDeliveryFollowsTheQueuedValues(
            new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () =>
            {
                left!.OnNext(Two);
                left.OnCompleted();
            },
            () => right!,
            FirstTwoPairs);
    }

    /// <summary>
    /// A completion raised while another thread delivers follows the pairs queued before it, and a value queued behind it
    /// is dropped.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessCompletionRaisedDuringDeliveryFollowsTheQueuedPairs()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedCompletionRaisedDuringDeliveryFollowsTheQueuedValues(
            new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => (a * OneHundred) + b),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
                right.OnNext(Three);
            },
            () => left!.OnNext(One),
            () =>
            {
                left!.OnNext(Two);
                left.OnCompleted();
                left.OnNext(Three);
            },
            FirstTwoPairs);
    }

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessValuePushedWhileTheTerminalWaitsIsDropped()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedValuePushedWhileTheTerminalWaitsIsDropped(
            new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => a + b),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => right!,
            () => left!.OnNext(Two));
    }

    /// <summary>An observer that throws on a direct delivery leaves the witness able to deliver the next pair.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessObserverThrowingOnDirectDeliveryLeavesItUsable()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(
            new PairSignal<int, int, int>(
                new ScriptedObservable<int>(observer => left = observer),
                new ScriptedObservable<int>(observer => right = observer),
                static (a, b) => a + b),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two));
    }

    /// <summary>Only the first error reaches the observer, and nothing is delivered after it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PairWitnessDeliversOnlyTheFirstError()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");

        using var subscription = Pair(new ScriptedObservable<int>(observer => left = observer), new ScriptedObservable<int>(observer => right = observer), downstream);
        left!.OnNext(One);
        right!.OnError(expected);
        left.OnError(new InvalidOperationException("second"));
        right.OnNext(Two);

        await Assert.That(downstream.Errors.Count).IsEqualTo(One);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>Subscribes a pair witness that adds the paired values.</summary>
    /// <param name="left">The left source.</param>
    /// <param name="right">The right source.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MultipleDisposable Pair(IObservable<int> left, IObservable<int> right, IObserver<int> observer) =>
        new PairWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);
}
