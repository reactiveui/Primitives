// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that the switch-map sink serializes deliveries and keeps only the newest inner subscription.</summary>
public partial class SwitchMapTests
{
    /// <summary>An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the inner source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapObserverMarshallingToAnotherInnerThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitchMap));

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitchMap));

    /// <summary>Inner pushes from separate threads never overlap downstream, and each thread's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapConcurrentInnerPushesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitchMap));

    /// <summary>An inner error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapInnerErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitchMap));

    /// <summary>An inner value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitchMap));

    /// <summary>Superseded inner notifications raised while another thread delivers are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapSupersededInnerNotificationsRaisedDuringDeliveryAreDropped() =>
        MergeDeliveryAssertions.SupersededInnerNotificationsRaisedDuringDeliveryAreDropped(SubscribeSwitchMap);

    /// <summary>Completion raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapOuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitchMap);

    /// <summary>An outer error raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapOuterErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitchMap);

    /// <summary>Concurrent outer values never leave a superseded inner subscribed: the inner whose subscription returns last but is older is disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapConcurrentOuterValuesKeepOnlyTheNewestInnerSubscribed() =>
        MergeDeliveryAssertions.OverlappingSwitchesKeepOnlyTheNewestInnerSubscribed(SubscribeSwitchMap);

    /// <summary>Completion raised before disposal while another thread delivers is still delivered once; later notifications are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapTerminalRaisedBeforeDisposeIsStillDelivered() =>
        MergeDeliveryAssertions.TerminalRaisedBeforeDisposeIsStillDelivered(SubscribeSwitchMap);

    /// <summary>An inner subscription that returns after the sink was disposed is disposed at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapInnerSubscribedWhileDisposingIsDisposed() =>
        MergeDeliveryAssertions.InnerSubscribedWhileDisposingIsDisposed(SubscribeSwitchMap);

    /// <summary>An inner source that fails while it is being subscribed delivers the error and has its subscription disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchMapInnerFailingWhileSubscribingIsDisposed() =>
        MergeDeliveryAssertions.InnerFailingWhileSubscribingIsDisposed(SubscribeSwitchMap);

    /// <summary>After the first terminal notification, outer values, errors and completion are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchMapIgnoresOuterNotificationsAfterTheFirstTerminal()
    {
        IObserver<string>? outer = null;
        IObserver<int>? inner = null;
        var projections = 0;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new(Boom);

        using var subscription = new SwitchMapSignal<string, int>(
                new ScriptedObservable<string>(observer => outer = observer),
                _ =>
                {
                    projections++;
                    return new ScriptedObservable<int>(observer => inner = observer);
                })
            .Subscribe(downstream);
        outer!.OnNext(KeyA);
        inner!.OnError(expected);
        outer.OnNext(KeyB);
        outer.OnError(new InvalidOperationException("late"));
        outer.OnCompleted();

        await Assert.That(projections).IsEqualTo(Once + Once);
        await Assert.That(downstream.Errors.Count).IsEqualTo(Once);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>Delivering the terminal notification releases the outer and the active inner subscriptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchMapReleasesSubscriptionsOnceCompletionIsDelivered()
    {
        IObserver<string>? outer = null;
        IObserver<int>? inner = null;
        RecordingDisposable outerSubscription = new();
        RecordingDisposable innerSubscription = new();
        RecordingWitness<int> downstream = new();

        using var subscription = new SwitchMapSignal<string, int>(
                new ScriptedObservable<string>(observer => outer = observer, outerSubscription),
                _ => new ScriptedObservable<int>(observer => inner = observer, innerSubscription))
            .Subscribe(downstream);
        outer!.OnNext(KeyA);
        outer.OnCompleted();
        inner!.OnNext(Ten);

        await Assert.That(innerSubscription.DisposeCount).IsEqualTo(0);

        inner.OnCompleted();

        await Assert.That(downstream.Values.SequenceEqual(_tenOnly)).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(Once);
        await Assert.That(outerSubscription.DisposeCount).IsEqualTo(Once);
        await Assert.That(innerSubscription.DisposeCount).IsEqualTo(Once);
    }

    /// <summary>Subscribes a switch map that projects each outer value to itself.</summary>
    /// <param name="sources">The outer source of inner sources.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeSwitchMap(IObservable<IObservable<int>> sources, IObserver<int> observer) =>
        new SwitchMapSignal<IObservable<int>, int>(sources, static inner => inner).Subscribe(observer);
}
