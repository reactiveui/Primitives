// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests array collection notifications.</summary>
public class CollectArrayWitnessTests
{
    /// <summary>An upstream error is forwarded without emitting a partial array and releases the subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_DiscardsPartialResultAndDisposesSubscription()
    {
        RecordingWitness<int[]> observer = new();
        RecordingDisposable subscription = new();
        using CollectArrayWitness<int> witness = new(observer);
        witness.SetSubscription(subscription);
        witness.OnNext(1);
        InvalidOperationException error = new("source");

        witness.OnError(error);

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
