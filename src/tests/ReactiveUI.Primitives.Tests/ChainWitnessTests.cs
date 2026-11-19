// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <see cref="ChainWitness{T}"/> serializes deliveries without holding a lock while user code runs.</summary>
public sealed class ChainWitnessTests
{
    /// <summary>Iteration count for deep synchronous stack-safety verification.</summary>
    private const int StackSafetyDepth = 10_000;

    /// <summary>Second inner value used by reentrant subscription tests.</summary>
    private const int SecondValue = 2;

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread completes the inner
    /// source, pushes the next one and completes the outer source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessObserverMarshallingWhileTheOtherThreadAdvancesTheChainDoesNotDeadlock() =>
        ChainDeliveryAssertions.ObserverMarshallingWhileTheOtherThreadAdvancesTheChainDoesNotDeadlock(SubscribeChain);

    /// <summary>A value pushed by the observer itself is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        ChainDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(SubscribeChain);

    /// <summary>Outer completion raised from another thread is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessCompletionRaisedDuringDeliveryFollowsTheQueuedValues() =>
        ChainDeliveryAssertions.CompletionRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeChain);

    /// <summary>An outer error raised from another thread follows the queued values and stops the chain.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessOuterErrorRaisedDuringDeliveryFollowsTheQueuedValuesAndStopsTheChain() =>
        ChainDeliveryAssertions.OuterErrorRaisedDuringDeliveryFollowsTheQueuedValuesAndStopsTheChain(SubscribeChain);

    /// <summary>An inner error raised from another thread follows the queued values and wins over later terminals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessInnerErrorRaisedDuringDeliveryFollowsTheQueuedValues() =>
        ChainDeliveryAssertions.InnerErrorRaisedDuringDeliveryFollowsTheQueuedValues(SubscribeChain);

    /// <summary>Inner sources are subscribed one at a time in source order while pushes and completions contend.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessInnerSourcesAreSubscribedOneAtATimeInOrderUnderContention() =>
        ChainDeliveryAssertions.InnerSourcesAreSubscribedOneAtATimeInOrderUnderContention(SubscribeChain);

    /// <summary>Completion raised before disposal while another thread delivers is still delivered once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessTerminalRaisedBeforeDisposeIsStillDelivered() =>
        ChainDeliveryAssertions.TerminalRaisedBeforeDisposeIsStillDelivered(SubscribeChain);

    /// <summary>Notifications raised after disposal are dropped and the queued inner source is never subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ChainWitnessNotificationsRaisedAfterDisposeAreDropped() =>
        ChainDeliveryAssertions.NotificationsRaisedAfterDisposeAreDropped(SubscribeChain);

    /// <summary>An inner source completing inside Subscribe while the outer pushes the next source does not leak the second subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_InnerCompletesDuringSubscribeAndOuterPushesNext_DisposesSecondInnerOnChainDispose()
    {
        using Signal<IObservable<int>> outer = new();
        var secondDisposals = 0;
        var second = Signal.Create<int>(_ => new ActionDisposable(() => secondDisposals++));
        var first = Signal.Create<int>(innerObserver =>
        {
            innerObserver.OnCompleted();
            outer.OnNext(second);
            return EmptyDisposable.Instance;
        });

        var chainSub = SubscribeChain(outer, new RecordingWitness<int>());
        outer.OnNext(first);
        chainSub.Dispose();

        await Assert.That(secondDisposals).IsEqualTo(1);
    }

    /// <summary>Outer emissions raised while an inner source is subscribing are queued and subscribed sequentially without losing handles.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_OuterEmitsDuringInnerSubscribe_StreamsInOrderAndDisposesActiveInner()
    {
        using Signal<IObservable<int>> outer = new();
        var secondDisposals = 0;
        IObserver<int>? secondObserver = null;
        var second = Signal.Create<int>(obs =>
        {
            secondObserver = obs;
            return new ActionDisposable(() => secondDisposals++);
        });
        var first = Signal.Create<int>(obs =>
        {
            outer.OnNext(second);
            obs.OnNext(1);
            obs.OnCompleted();
            return EmptyDisposable.Instance;
        });

        RecordingWitness<int> downstream = new();
        var chainSub = SubscribeChain(outer, downstream);
        outer.OnNext(first);
        secondObserver!.OnNext(SecondValue);
        chainSub.Dispose();

        await Assert.That(downstream.Values.SequenceEqual([1, SecondValue])).IsTrue();
        await Assert.That(secondDisposals).IsEqualTo(1);
    }

    /// <summary>Chaining ten thousand synchronous inner sources completes iteratively without stack overflow.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TenThousandSynchronousInnerSources_CompletesWithoutStackOverflow()
    {
        var sources = new IObservable<int>[StackSafetyDepth];
        for (var i = 0; i < StackSafetyDepth; i++)
        {
            sources[i] = Signal.Emit(1);
        }

        var sum = 0;
        _ = Signal.Chain(sources).Subscribe(x => sum += x);
        await Assert.That(sum).IsEqualTo(StackSafetyDepth);
    }

    /// <summary>Subscribes a chain witness over the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable SubscribeChain(IObservable<IObservable<int>> sources, IObserver<int> observer) =>
        new ChainWitness<int>(observer).Run(sources);
}
