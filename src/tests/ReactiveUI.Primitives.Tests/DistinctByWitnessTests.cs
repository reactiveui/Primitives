// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the distinct-key observer's terminal and failure handling.</summary>
public class DistinctByWitnessTests
{
    /// <summary>A failed value callback propagates its exception and releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ObserverThrows_DisposesSubscription()
    {
        RecordingDisposable subscription = new();
        using DistinctByWitness<int, int> witness = new(new ThrowingWitness<int>(throwOnNext: true), static value => value, null);
        witness.SetSubscription(subscription);

        await Assert.That(() => witness.OnNext(1)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The first source error ends distinct-key delivery and releases its subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_ForwardsOnceAndIgnoresLaterNotifications()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable subscription = new();
        using DistinctByWitness<int, int> witness = new(observer, static value => value, null);
        witness.SetSubscription(subscription);
        InvalidOperationException error = new("source");

        witness.OnError(error);
        witness.OnNext(1);
        witness.OnError(error);
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
