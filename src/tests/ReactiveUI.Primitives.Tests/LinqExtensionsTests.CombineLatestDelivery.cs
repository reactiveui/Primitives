// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>CombineLatest</c> serializes deliveries without holding a lock while the observer runs.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>
    /// A two-source observer that marshals synchronously to another thread is not deadlocked when that thread pushes the
    /// other source, and the handed-over combination is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairObserverMarshallingToTheOtherSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            combined,
            () => right!.OnNext(One),
            () => left!.OnNext(One),
            () => right!.OnNext(Two),
            "101,102");
    }

    /// <summary>
    /// A three-source observer that marshals synchronously to another thread is not deadlocked when that thread pushes a
    /// sibling source, and the handed-over combination is delivered.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestTripleObserverMarshallingToASiblingSourceThreadDoesNotDeadlock()
    {
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
        var combined = new ScriptedObservable<int>(observer => first = observer).CombineLatest(
            new ScriptedObservable<int>(observer => second = observer),
            new ScriptedObservable<int>(observer => third = observer),
            static (a, b, c) => (a * OneHundred) + b + c);

        await MergeDeliveryAssertions.CombinedObserverMarshallingDoesNotDeadlock(
            combined,
            () =>
            {
                second!.OnNext(0);
                third!.OnNext(One);
            },
            () => first!.OnNext(One),
            () => third!.OnNext(Two),
            "101,102");
    }

    /// <summary>A value pushed by the observer itself is combined after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestValueRaisedByTheObserverIsCombinedAfterItReturns()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        List<string> log = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            log.Add($"start{value}");
            if (value == One + One)
            {
                left!.OnNext(Two);
            }

            log.Add($"end{value}");
        });

        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b)
            .Subscribe(downstream);
        right!.OnNext(One);
        left!.OnNext(One);

        await Assert.That(string.Join(",", log)).IsEqualTo("start2,end2,start3,end3");
    }

    /// <summary>Both sides pushing from separate threads never overlap downstream, and every update produces a combination.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task CombineLatestConcurrentSourcesDeliverEveryCombinationWithoutOverlap() =>
        MergeDeliveryAssertions.LatestSourcesDeliverEveryCombinationWithoutOverlap(
            static (left, right, observer) => left.CombineLatest(right, static (a, b) => (a, b)).Subscribe(observer));

    /// <summary>Only the first error reaches the observer, and nothing is delivered after it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairDeliversOnlyTheFirstError()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");

        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b)
            .Subscribe(downstream);
        left!.OnNext(One);
        right!.OnError(expected);
        left.OnError(new InvalidOperationException("second"));
        right.OnNext(Two);

        await Assert.That(downstream.Errors.Count).IsEqualTo(One);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>Pair values pushed from other threads while a delivery runs are combined in arrival order once it returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairValuesQueuedDuringDeliveryFollowInOrder()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedValuesQueuedDuringDeliveryFollowInOrder(
            combined,
            () => right!.OnNext(0),
            () => left!.OnNext(One),
            () => right!.OnNext(One),
            () => right!.OnNext(Two),
            "100,101,102");
    }

    /// <summary>Triple values pushed from other threads while a delivery runs are combined in arrival order once it returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestTripleValuesQueuedDuringDeliveryFollowInOrder()
    {
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
        var combined = new ScriptedObservable<int>(observer => first = observer).CombineLatest(
            new ScriptedObservable<int>(observer => second = observer),
            new ScriptedObservable<int>(observer => third = observer),
            static (a, b, c) => (a * OneHundred) + b + c);

        await MergeDeliveryAssertions.CombinedValuesQueuedDuringDeliveryFollowInOrder(
            combined,
            () =>
            {
                second!.OnNext(0);
                third!.OnNext(0);
            },
            () => first!.OnNext(One),
            () => second!.OnNext(One),
            () => third!.OnNext(Two),
            "100,101,103");
    }

    /// <summary>Left pair values pushed from other threads while a delivery runs are combined in arrival order once it returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairLeftValuesQueuedDuringDeliveryFollowInOrder()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => (a * OneHundred) + b);

        await MergeDeliveryAssertions.CombinedValuesQueuedDuringDeliveryFollowInOrder(
            combined,
            () => left!.OnNext(0),
            () => right!.OnNext(One),
            () => left!.OnNext(One),
            () => left!.OnNext(Two),
            "1,101,201");
    }

    /// <summary>A pair observer that throws on a direct right delivery leaves the combination able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairObserverThrowingOnDirectRightDeliveryLeavesItUsable()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b);

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(combined, () => left!.OnNext(One), () => right!.OnNext(One), () => right!.OnNext(Two));
    }

    /// <summary>A pair observer that throws on a direct delivery leaves the combination able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairObserverThrowingOnDirectDeliveryLeavesItUsable()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b);

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(combined, () => right!.OnNext(One), () => left!.OnNext(One), () => left!.OnNext(Two));
    }

    /// <summary>A triple observer that throws on a direct delivery leaves the combination able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestTripleObserverThrowingOnDirectDeliveryLeavesItUsable()
    {
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
        var combined = new ScriptedObservable<int>(observer => first = observer).CombineLatest(
            new ScriptedObservable<int>(observer => second = observer),
            new ScriptedObservable<int>(observer => third = observer),
            static (a, b, c) => a + b + c);

        await MergeDeliveryAssertions.ThrowingObserverLeavesCombinationUsable(
            combined,
            () =>
            {
                second!.OnNext(0);
                third!.OnNext(One);
            },
            () => first!.OnNext(One),
            () => first!.OnNext(Two));
    }

    /// <summary>A pair value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestPairValuePushedWhileTheTerminalWaitsIsDropped()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        var combined = new ScriptedObservable<int>(observer => left = observer)
            .CombineLatest(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b);

        await MergeDeliveryAssertions.CombinedValuePushedWhileTheTerminalWaitsIsDropped(
            combined,
            () => right!.OnNext(0),
            () => left!.OnNext(One),
            () => left!,
            () => right!.OnNext(Two));
    }

    /// <summary>A triple value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombineLatestTripleValuePushedWhileTheTerminalWaitsIsDropped()
    {
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
        var combined = new ScriptedObservable<int>(observer => first = observer).CombineLatest(
            new ScriptedObservable<int>(observer => second = observer),
            new ScriptedObservable<int>(observer => third = observer),
            static (a, b, c) => a + b + c);

        await MergeDeliveryAssertions.CombinedValuePushedWhileTheTerminalWaitsIsDropped(
            combined,
            () =>
            {
                second!.OnNext(0);
                third!.OnNext(0);
            },
            () => first!.OnNext(One),
            () => second!,
            () => third!.OnNext(Two));
    }
}
