// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the type filter's failure handling.</summary>
public class KeepTypeWitnessTests
{
    /// <summary>A failed value callback propagates its exception and releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ObserverThrows_DisposesSubscription()
    {
        RecordingDisposable subscription = new();
        using KeepTypeWitness<int> witness = new(new ThrowingWitness<int>(throwOnNext: true));
        witness.SetSubscription(subscription);

        await Assert.That(() => witness.OnNext(1)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Errors pass through the filter and release the subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_ForwardsErrorAndDisposesSubscription()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable subscription = new();
        using KeepTypeWitness<int> witness = new(observer);
        witness.SetSubscription(subscription);
        InvalidOperationException error = new("source");

        witness.OnError(error);

        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
