// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests blend completion tracking and delivery serialization across inner sources.</summary>
public sealed class BlendWitnessTests
{
    /// <summary>The message of the error raised for a null inner source.</summary>
    private const string NullSourceMessage = "Blend source contained null.";

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a value
    /// through a sibling source.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock() =>
        MergeDeliveryAssertions.ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(Subscribe);

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ValueRaisedByTheObserverIsDeliveredAfterItReturns() =>
        MergeDeliveryAssertions.ValueRaisedByTheObserverIsDeliveredAfterItReturns(Subscribe);

    /// <summary>Sources pushing from separate threads never overlap downstream, and each source's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap() =>
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

    /// <summary>An outer observable completes the blend only once it and every inner source have completed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OuterObservableCompletesOnceItAndEveryInnerSourceComplete()
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new BlendWitness<int>(downstream).Run(new ScriptedObservable<IObservable<int>>(observer => outer = observer));
        outer!.OnNext(new ScriptedObservable<int>(static observer =>
        {
            observer.OnNext(1);
            observer.OnCompleted();
        }));
        outer.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        outer.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(0);

        inner!.OnCompleted();
        await Assert.That(downstream.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>Completion that follows the first error is not delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionAfterTheFirstErrorIsSuppressed()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("blend");

        using var subscription = new BlendWitness<int>(downstream).Run(
        [
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer),
        ]);
        left!.OnError(expected);
        left.OnCompleted();
        right!.OnCompleted();

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>A null inner source fails the blend and suppresses its completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullSourceFailsTheBlend()
    {
        RecordingWitness<int> downstream = new();

        using var subscription = new BlendWitness<int>(downstream).Run([null!]);

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0].Message).IsEqualTo(NullSourceMessage);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>Subscribes a blend witness over the sources.</summary>
    /// <param name="sources">The sources to blend.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable Subscribe(IObservable<int>[] sources, IObserver<int> observer) =>
        new BlendWitness<int>(observer).Run(sources);
}
