// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that the switch witness serializes deliveries without holding a lock while user code runs.</summary>
public sealed partial class SwitchWitnessTests
{
    /// <summary>An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the inner source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessObserverMarshallingToAnotherInnerThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>Inner pushes from separate threads never overlap downstream, and each thread's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessConcurrentInnerPushesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>An inner error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessInnerErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>An inner value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>Superseded inner notifications raised while another thread delivers are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessSupersededInnerNotificationsRaisedDuringDeliveryAreDropped() =>
        MergeDeliveryAssertions.SupersededInnerNotificationsRaisedDuringDeliveryAreDropped(SubscribeSwitch);

    /// <summary>Completion raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessOuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitch);

    /// <summary>An outer error raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessOuterErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitch);

    /// <summary>An earlier switch whose inner subscription returns last is disposed, and the newest inner stays subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessOverlappingSwitchesKeepOnlyTheNewestInnerSubscribed() =>
        MergeDeliveryAssertions.OverlappingSwitchesKeepOnlyTheNewestInnerSubscribed(SubscribeSwitch);

    /// <summary>Completion raised before disposal while another thread delivers is still delivered once; later notifications are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessTerminalRaisedBeforeDisposeIsStillDelivered() =>
        MergeDeliveryAssertions.TerminalRaisedBeforeDisposeIsStillDelivered(SubscribeSwitch);

    /// <summary>An inner subscription that returns after the witness was disposed is disposed at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessInnerSubscribedWhileDisposingIsDisposed() =>
        MergeDeliveryAssertions.InnerSubscribedWhileDisposingIsDisposed(SubscribeSwitch);

    /// <summary>An inner source that fails while it is being subscribed delivers the error and has its subscription disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchWitnessInnerFailingWhileSubscribingIsDisposed() =>
        MergeDeliveryAssertions.InnerFailingWhileSubscribingIsDisposed(SubscribeSwitch);

    /// <summary>
    /// A completion from the previous inner that arrives while the next inner is being subscribed is ignored, so the
    /// witness still waits for the next inner.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchWitnessIgnoresPreviousInnerCompletionWhileTheNextInnerSubscribes()
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new SwitchWitness<int>(downstream).Run(new ScriptedObservable<IObservable<int>>(observer => outer = observer));
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        outer.OnNext(new ScriptedObservable<int>(observer =>
        {
            second = observer;
            first!.OnCompleted();
        }));
        outer.OnCompleted();

        await Assert.That(downstream.Completed).IsEqualTo(0);

        second!.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(One);
    }

    /// <summary>An error from the current inner that follows an outer error is not delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchWitnessDropsCurrentInnerErrorAfterOuterError()
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("outer");

        using var subscription = new SwitchWitness<int>(downstream).Run(new ScriptedObservable<IObservable<int>>(observer => outer = observer));
        outer!.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        outer.OnError(expected);
        inner!.OnError(new InvalidOperationException("inner"));

        await Assert.That(downstream.Errors).HasSingleItem();
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Subscribes a switch witness over the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeSwitch(IObservable<IObservable<int>> sources, IObserver<int> observer) =>
        new SwitchWitness<int>(observer).Run(sources);
}
