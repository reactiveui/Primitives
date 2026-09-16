// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Zip</c> serializes deliveries without holding a lock while the selector or observer runs.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>The first two zipped results when each selector returns the left value times one hundred plus the right.</summary>
    private const string FirstTwoZippedPairs = "101,202";

    /// <summary>
    /// A zip observer that marshals synchronously to another thread is not deadlocked when that thread pushes the other
    /// source, and the pair it completes is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipObserverMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var zipped = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            zipped,
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two),
            FirstTwoZippedPairs);
    }

    /// <summary>
    /// A zip selector that marshals synchronously to another thread is not deadlocked when that thread pushes the other
    /// source, and the pair it completes is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipSelectorMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;

        await MergeDeliveryAssertions.CombinedSelectorMarshallingDoesNotDeadlock(
            selector => new ScriptedObservable<int>(observer => left = observer)
                .Zip(new ScriptedObservable<int>(observer => right = observer), selector),
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two),
            FirstTwoZippedPairs);
    }

    /// <summary>Both sides pushing from separate threads never overlap downstream, and every index produces its pair in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ZipConcurrentSourcesDeliverEveryPairInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ZippedSourcesDeliverEveryPairInOrderWithoutOverlap(
            static (left, right, observer) => left.Zip(right, static (a, b) => (a, b)).Subscribe(observer));

    /// <summary>
    /// An error raised while another thread delivers follows the pairs queued before it, and wins over a completion
    /// queued before it.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipErrorRaisedDuringDeliveryFollowsTheQueuedPairs()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var zipped = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedErrorRaisedDuringDeliveryFollowsTheQueuedValues(
            zipped,
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
            FirstTwoZippedPairs);
    }

    /// <summary>
    /// A completion raised while another thread delivers follows the pairs queued before it, and a value queued behind it
    /// is dropped.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipCompletionRaisedDuringDeliveryFollowsTheQueuedPairs()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var zipped = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedCompletionRaisedDuringDeliveryFollowsTheQueuedValues(
            zipped,
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
            FirstTwoZippedPairs);
    }

    /// <summary>A zip value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipValuePushedWhileTheTerminalWaitsIsDropped()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var zipped = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b);

        await MergeDeliveryAssertions.CombinedValuePushedWhileTheTerminalWaitsIsDropped(
            zipped,
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => right!,
            () => left!.OnNext(Two));
    }

    /// <summary>A zip observer that throws on a direct delivery leaves the zip able to deliver the next pair.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipObserverThrowingOnDirectDeliveryLeavesItUsable()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var zipped = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b);

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(
            zipped,
            () =>
            {
                right!.OnNext(One);
                right.OnNext(Two);
            },
            () => left!.OnNext(One),
            () => left!.OnNext(Two));
    }

    /// <summary>Only the first zip error reaches the observer, and nothing is delivered after it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipDeliversOnlyTheFirstError()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");

        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b)
            .Subscribe(downstream);
        left!.OnNext(One);
        right!.OnError(expected);
        left.OnError(new InvalidOperationException("second"));
        right.OnNext(Two);

        await Assert.That(downstream.Errors.Count).IsEqualTo(One);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }
}
