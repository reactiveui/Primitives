// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Chain</c> serializes deliveries without holding a lock while user code runs.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>
    /// A chain observer that marshals synchronously to another thread is not deadlocked when that thread completes the inner
    /// source, pushes the next one and completes the outer source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainObserverMarshallingWhileTheOtherThreadAdvancesTheChainDoesNotDeadlock() =>
        ChainDeliveryAssertions.ObserverMarshallingWhileTheOtherThreadAdvancesTheChainDoesNotDeadlock(SubscribeChain);

    /// <summary>A chain value pushed by the observer itself is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        ChainDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(SubscribeChain);

    /// <summary>Chain outer completion raised from another thread is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainCompletionRaisedDuringDeliveryFollowsTheQueuedValues() =>
        ChainDeliveryAssertions.CompletionRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeChain);

    /// <summary>A chain outer error raised from another thread follows the queued values and stops the chain.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainOuterErrorRaisedDuringDeliveryFollowsTheQueuedValuesAndStopsTheChain() =>
        ChainDeliveryAssertions.OuterErrorRaisedDuringDeliveryFollowsTheQueuedValuesAndStopsTheChain(SubscribeChain);

    /// <summary>A chain inner error raised from another thread follows the queued values and wins over later terminals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainInnerErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        ChainDeliveryAssertions.InnerErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeChain);

    /// <summary>Chain inner sources are subscribed one at a time in source order while pushes and completions contend.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainInnerSourcesAreSubscribedOneAtATimeInOrderUnderContention() =>
        ChainDeliveryAssertions.InnerSourcesAreSubscribedOneAtATimeInOrderUnderContention(SubscribeChain);

    /// <summary>Chain completion raised before disposal while another thread delivers is still delivered once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainTerminalRaisedBeforeDisposeIsStillDelivered() =>
        ChainDeliveryAssertions.TerminalRaisedBeforeDisposeIsStillDelivered(SubscribeChain);

    /// <summary>Chain notifications raised after disposal are dropped and the queued inner source is never subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainNotificationsRaisedAfterDisposeAreDropped() =>
        ChainDeliveryAssertions.NotificationsRaisedAfterDisposeAreDropped(SubscribeChain);

    /// <summary>Subscribes <c>Chain</c> over the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeChain(IObservable<IObservable<int>> sources, IObserver<int> observer) =>
        sources.Chain().Subscribe(observer);
}
