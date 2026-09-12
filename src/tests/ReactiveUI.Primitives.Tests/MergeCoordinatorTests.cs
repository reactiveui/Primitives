// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests merge completion tracking across inner sources.</summary>
public sealed class MergeCoordinatorTests
{
    /// <summary>Repeated completion cannot consume another inner source's active count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RepeatedInnerCompletionKeepsTheSiblingActive()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        ScriptedObservable<int> leftSource = new(observer => left = observer);
        ScriptedObservable<int> rightSource = new(observer => right = observer);

        using var subscription = new MergeCoordinator<int>(downstream).Run([leftSource, rightSource]);

        left!.OnCompleted();
        left.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(0);

        right!.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(1);
        await Assert.That(right).IsNotNull();
    }
}
