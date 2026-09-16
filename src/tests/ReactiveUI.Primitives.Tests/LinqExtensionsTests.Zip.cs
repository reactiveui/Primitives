// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests two-source <c>Zip</c> completion.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>A completed left source with unmatched values completes the zip only once the right source completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipWaitsForTheRightSourceWhileTheCompletedLeftHasUnmatchedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b)
            .Subscribe(downstream);
        left!.OnNext(One);
        left.OnNext(Two);
        left.OnCompleted();
        right!.OnNext(OneHundred);
        var completedBeforeRight = downstream.Completed;
        right.OnCompleted();

        await Assert.That(completedBeforeRight).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(One);
        await Assert.That(downstream.Values.SequenceEqual([OneHundred + One])).IsTrue();
    }

    /// <summary>A completed right source with unmatched values completes the zip only once the left source completes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ZipWaitsForTheLeftSourceWhileTheCompletedRightHasUnmatchedValues()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();

        using var subscription = new ScriptedObservable<int>(observer => left = observer)
            .Zip(new ScriptedObservable<int>(observer => right = observer), static (a, b) => a + b)
            .Subscribe(downstream);
        right!.OnNext(OneHundred);
        right.OnNext(OneHundred + One);
        right.OnCompleted();
        left!.OnNext(One);
        var completedBeforeLeft = downstream.Completed;
        left.OnCompleted();

        await Assert.That(completedBeforeLeft).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(One);
        await Assert.That(downstream.Values.SequenceEqual([OneHundred + One])).IsTrue();
    }
}
