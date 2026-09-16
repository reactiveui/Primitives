// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Blend</c> and <c>BlendUnique</c> serialize deliveries without holding a lock while the observer runs.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>
    /// A blend observer that marshals synchronously to another thread is not deadlocked when that thread pushes a sibling
    /// inner source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendObserverMarshallingToAnotherSourceThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(SubscribeBlend);

    /// <summary>A blend value pushed by the observer itself is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(SubscribeBlend);

    /// <summary>Blend inner sources pushing from separate threads never overlap downstream and keep their order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(SubscribeBlend);

    /// <summary>A blend error raised while another thread delivers follows the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeBlend);

    /// <summary>A blend value pushed while the terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(SubscribeBlend);

    /// <summary>Blend completes only once the outer source and every inner source have completed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BlendCompletesOnceTheOuterAndEveryInnerSourceComplete()
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new ScriptedObservable<IObservable<int>>(observer => outer = observer).Blend().Subscribe(downstream);
        outer!.OnNext(new ScriptedObservable<int>(static observer =>
        {
            observer.OnNext(One);
            observer.OnCompleted();
        }));
        outer.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        outer.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(0);

        inner!.OnCompleted();
        await Assert.That(downstream.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(One);
    }

    /// <summary>A null blend inner source fails the blend and suppresses completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BlendNullInnerSourceFailsTheBlend()
    {
        RecordingWitness<int> downstream = new();

        using var subscription = new ScriptedObservable<IObservable<int>>(static observer =>
        {
            observer.OnNext(null!);
            observer.OnCompleted();
        }).Blend().Subscribe(downstream);

        await Assert.That(downstream.Errors.Count).IsEqualTo(One);
        await Assert.That(downstream.Errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>
    /// A distinct-merge observer that marshals synchronously to another thread is not deadlocked when that thread pushes a
    /// sibling source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendUniqueObserverMarshallingToAnotherSourceThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(SubscribeBlendUnique);

    /// <summary>A distinct-merge value pushed by the observer itself is compared and delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendUniqueValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(SubscribeBlendUnique);

    /// <summary>Distinct-merge sources pushing from separate threads never overlap downstream and keep their order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendUniqueConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(SubscribeBlendUnique);

    /// <summary>A distinct-merge error raised while another thread delivers follows the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendUniqueErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeBlendUnique);

    /// <summary>A distinct-merge value pushed while the terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task BlendUniqueValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(SubscribeBlendUnique);

    /// <summary>Duplicates queued behind a running delivery are compared in arrival order and suppressed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task BlendUniqueDuplicatesQueuedDuringDeliveryAreSuppressedInArrivalOrder()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Add(value);
            if (inside.IsSet)
            {
                return;
            }

            inside.Set();
            release.Wait();
        });

        using var subscription = LinqExtensions
            .BlendUnique(new ScriptedObservable<int>(observer => left = observer), new ScriptedObservable<int>(observer => right = observer))
            .Subscribe(downstream);
        var owner = BackgroundThread.Start(() => left!.OnNext(One));
        inside.Wait();
        await BackgroundThread.Start(() => right!.OnNext(One));
        await BackgroundThread.Start(() => right!.OnNext(Two));
        await BackgroundThread.Start(() => right!.OnNext(Two));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>Subscribes <c>Blend</c> over an outer source that yields the sources and completes.</summary>
    /// <param name="sources">The inner sources.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeBlend(IObservable<int>[] sources, IObserver<int> observer) =>
        new ScriptedObservable<IObservable<int>>(outer =>
        {
            foreach (var source in sources)
            {
                outer.OnNext(source);
            }

            outer.OnCompleted();
        }).Blend().Subscribe(observer);

    /// <summary>Subscribes <c>BlendUnique</c> over the sources.</summary>
    /// <param name="sources">The sources.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeBlendUnique(IObservable<int>[] sources, IObserver<int> observer) =>
        LinqExtensions.BlendUnique(sources).Subscribe(observer);
}
