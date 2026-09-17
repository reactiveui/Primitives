// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests generation changes, notification ordering and delivery serialization in the switch coordinator.</summary>
public sealed class SwitchCoordinatorTests
{
    /// <summary>The value emitted by the replacement source.</summary>
    private const int ReplacementValue = 2;

    /// <summary>The value emitted by a superseded source.</summary>
    private const int StaleValue = 3;

    /// <summary>Switching preserves ordered delivery and runs the observer without holding the coordinator gate.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SwitchTo_SwitchBetweenValues_DeliversWithoutHoldingGate()
    {
        Signal<IObservable<int>> outer = new();
        CapturingObservable first = new();
        CapturingObservable second = new();
        List<int> values = [];
        LinqExtensions.SwitchCoordinator<int>? coordinator = null;
        var heldGate = false;
        using var subscription = outer.SwitchTo().Subscribe(value =>
        {
            heldGate |= IsHeld(coordinator!.Gate);
            values.Add(value);
        });
        coordinator = (LinqExtensions.SwitchCoordinator<int>)subscription;

        outer.OnNext(first);
        first.Observer!.OnNext(1);
        outer.OnNext(second);
        first.Observer.OnNext(StaleValue);
        second.Observer!.OnNext(ReplacementValue);

        await Assert.That(heldGate).IsFalse();
        await Assert.That(values.SequenceEqual([1, ReplacementValue])).IsTrue();
    }

    /// <summary>A value accepted before a switch is retained, and later values from that generation are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryBeginSource_BetweenInnerValues_RejectsPreviousGeneration()
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);

        await Assert.That(coordinator.TryBeginSource(out var first)).IsTrue();
        coordinator.OnNext(first, 1);
        await Assert.That(coordinator.TryBeginSource(out var second)).IsTrue();
        coordinator.OnNext(first, StaleValue);
        coordinator.OnError(first, new InvalidOperationException("stale"));
        coordinator.OnCompleted(first);
        coordinator.OnOuterCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).IsEmpty();

        coordinator.OnNext(second, ReplacementValue);
        coordinator.OnCompleted(second);

        await Assert.That(observer.Values.SequenceEqual([1, ReplacementValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>
    /// Only the current generation's subscription is kept: a superseded one is disposed at once, a displaced one when the
    /// next generation installs, and any installed after disposal immediately.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Install_GenerationOrder_KeepsOnlyTheCurrentSubscription()
    {
        RecordingDisposable superseded = new();
        RecordingDisposable displaced = new();
        RecordingDisposable current = new();
        RecordingDisposable late = new();
        LinqExtensions.SwitchCoordinator<int> coordinator = new(new RecordingWitness<int>());

        _ = coordinator.TryBeginSource(out var first);
        _ = coordinator.TryBeginSource(out var second);
        coordinator.Install(second, displaced);
        coordinator.Install(first, superseded);
        _ = coordinator.TryBeginSource(out var third);
        coordinator.Install(third, current);

        await Assert.That(superseded.DisposeCount).IsEqualTo(1);
        await Assert.That(displaced.DisposeCount).IsEqualTo(1);
        await Assert.That(current.DisposeCount).IsEqualTo(0);

        coordinator.Dispose();
        coordinator.Install(third, late);

        await Assert.That(current.DisposeCount).IsEqualTo(1);
        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Completion waits for both sources and rejects subsequent values and source generations.</summary>
    /// <param name="outerFirst">True when the outer source completes before the active inner source.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnCompleted_SourceOrder_CompletesOnce(bool outerFirst)
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);
        await Assert.That(coordinator.TryBeginSource(out var version)).IsTrue();

        if (outerFirst)
        {
            coordinator.OnOuterCompleted();
        }
        else
        {
            coordinator.OnCompleted(version);
        }

        await Assert.That(observer.Completed).IsEqualTo(0);

        if (outerFirst)
        {
            coordinator.OnCompleted(version);
        }
        else
        {
            coordinator.OnOuterCompleted();
        }

        coordinator.OnCompleted(version);
        coordinator.OnOuterCompleted();
        coordinator.OnNext(version, 1);
        coordinator.OnError(version, new InvalidOperationException("late inner"));
        coordinator.OnOuterError(new InvalidOperationException("late outer"));

        await Assert.That(coordinator.TryBeginSource(out _)).IsFalse();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>The first source error is forwarded and prevents subsequent errors, values, and completion.</summary>
    /// <param name="outerFirst">True when the outer source reports the first error.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnError_SourceOrder_ForwardsFirstError(bool outerFirst)
    {
        RecordingWitness<int> observer = new();
        using LinqExtensions.SwitchCoordinator<int> coordinator = new(observer);
        await Assert.That(coordinator.TryBeginSource(out var version)).IsTrue();
        InvalidOperationException first = new("first");
        InvalidOperationException second = new("second");

        if (outerFirst)
        {
            coordinator.OnOuterError(first);
            coordinator.OnError(version, second);
        }
        else
        {
            coordinator.OnError(version, first);
            coordinator.OnOuterError(second);
        }

        coordinator.OnNext(version, 1);
        coordinator.OnCompleted(version);
        coordinator.OnOuterCompleted();

        await Assert.That(coordinator.TryBeginSource(out _)).IsFalse();
        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(first);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the inner source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_ObserverMarshallingToAnotherInnerThread_DoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_ValueRaisedByTheObserver_IsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>Inner pushes from separate threads never overlap downstream, and each thread's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_ConcurrentInnerPushes_DeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>An inner error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_InnerErrorRaisedDuringDelivery_FollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>An inner value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_ValuePushedWhileTheTerminalWaits_IsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(MergeDeliveryAssertions.OverSharedInner(SubscribeSwitch));

    /// <summary>Superseded inner notifications raised while another thread delivers are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_SupersededInnerNotificationsRaisedDuringDelivery_AreDropped() =>
        MergeDeliveryAssertions.SupersededInnerNotificationsRaisedDuringDeliveryAreDropped(SubscribeSwitch);

    /// <summary>Completion raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_OuterCompletionRaisedDuringDelivery_FollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitch);

    /// <summary>An outer error raised while another thread delivers follows the queued values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_OuterErrorRaisedDuringDelivery_FollowsTheQueuedValues() =>
        MergeDeliveryAssertions.OuterErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeSwitch);

    /// <summary>An earlier switch whose inner subscription returns last is disposed, and the newest inner stays subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_OverlappingSwitches_KeepOnlyTheNewestInnerSubscribed() =>
        MergeDeliveryAssertions.OverlappingSwitchesKeepOnlyTheNewestInnerSubscribed(SubscribeSwitch);

    /// <summary>Completion raised before disposal while another thread delivers is still delivered once; later notifications are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_TerminalRaisedBeforeDispose_IsStillDelivered() =>
        MergeDeliveryAssertions.TerminalRaisedBeforeDisposeIsStillDelivered(SubscribeSwitch);

    /// <summary>An inner subscription that returns after the switch was disposed is disposed at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_InnerSubscribedWhileDisposing_IsDisposed() =>
        MergeDeliveryAssertions.InnerSubscribedWhileDisposingIsDisposed(SubscribeSwitch);

    /// <summary>An inner source that fails while it is being subscribed delivers the error and has its subscription disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SwitchTo_InnerFailingWhileSubscribing_IsDisposed() =>
        MergeDeliveryAssertions.InnerFailingWhileSubscribingIsDisposed(SubscribeSwitch);

    /// <summary>Subscribes <c>SwitchTo</c> over the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeSwitch(IObservable<IObservable<int>> sources, IObserver<int> observer) =>
        sources.SwitchTo().Subscribe(observer);

    /// <summary>Reports whether the calling thread owns the gate.</summary>
    /// <param name="gate">The coordinator synchronization gate.</param>
    /// <returns>True when the calling thread owns the gate; otherwise, false.</returns>
    private static bool IsHeld(Lock gate)
    {
#if NET9_0_OR_GREATER
        return gate.IsHeldByCurrentThread;
#else
        return Monitor.IsEntered(gate);
#endif
    }

    /// <summary>Retains the inner observer for notifications after a source switch.</summary>
    private sealed class CapturingObservable : IObservable<int>
    {
        /// <summary>Gets the observer captured by subscription.</summary>
        public IObserver<int>? Observer { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<int> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }
}
