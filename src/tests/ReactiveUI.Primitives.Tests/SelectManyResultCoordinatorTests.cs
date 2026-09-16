// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests projected SelectMany completion tracking and delivery serialization across inner sources.</summary>
public sealed class SelectManyResultCoordinatorTests
{
    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a value
    /// through a sibling inner source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ObserverMarshallingToAnotherInnerThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(Subscribe);

    /// <summary>A value pushed by the observer itself is projected and delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(Subscribe);

    /// <summary>Inner sources pushing from separate threads never overlap downstream, and each source's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ConcurrentInnerSourcesDeliverEveryValueInOrderWithoutOverlap() =>
        MergeDeliveryAssertions.ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(Subscribe);

    /// <summary>An error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        MergeDeliveryAssertions.ErrorRaisedDuringDeliveryFollowsTheQueuedValues(Subscribe);

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ValuePushedWhileTheTerminalWaitsIsDropped() =>
        MergeDeliveryAssertions.ValuePushedWhileTheTerminalWaitsIsDropped(Subscribe);

    /// <summary>A source value that arrives after the first error neither subscribes an inner source nor completes the sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SourceValueAfterTheFirstErrorIsNotSubscribed()
    {
        IObserver<int>? outer = null;
        var subscriptions = 0;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("outer");

        using var subscription = new SelectManyResultCoordinator<int, int, int>(
                downstream,
                _ => new ScriptedObservable<int>(_ => subscriptions++),
                static (_, value) => value)
            .Run(new ScriptedObservable<int>(observer => outer = observer));
        outer!.OnError(expected);
        outer.OnNext(1);
        outer.OnCompleted();

        await Assert.That(subscriptions).IsEqualTo(0);
        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>Subscribes a coordinator whose outer source yields the index of each scenario source and whose projection passes inner values through.</summary>
    /// <param name="sources">The inner sources.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable Subscribe(IObservable<int>[] sources, IObserver<int> observer) =>
        new SelectManyResultCoordinator<int, int, int>(observer, index => sources[index], static (_, value) => value)
            .Run(new ScriptedObservable<int>(outer =>
            {
                for (var index = 0; index < sources.Length; index++)
                {
                    outer.OnNext(index);
                }
            }));
}
