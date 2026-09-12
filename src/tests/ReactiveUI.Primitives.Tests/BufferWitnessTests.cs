// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests buffered observer termination.</summary>
public class BufferWitnessTests
{
    /// <summary>A duplicate error cannot emit a partial batch or notify the observer twice.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_Repeated_ForwardsOnlyFirstError()
    {
        const int WindowSize = 2;
        RecordingWitness<IList<int>> observer = new();
        RecordingDisposable subscription = new();
        using BufferWitness<int> witness = new(observer, WindowSize, 0);
        witness.SetSubscription(subscription);
        witness.OnNext(1);
        InvalidOperationException error = new("source");

        witness.OnError(error);
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
