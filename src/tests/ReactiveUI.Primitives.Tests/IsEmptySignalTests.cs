// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for <c>IsEmpty</c> over an ordinary source, which does not require the current thread and therefore
/// settles on the direct subscribe path rather than the current-thread trampoline the timer sources use.
/// </summary>
public sealed class IsEmptySignalTests
{
    /// <summary>The value produced by the non-empty source.</summary>
    private const int SourceValue = 7;

    /// <summary>Verifies an ordinary source that completes without values emits <see langword="true"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EmptySourceEmitsTrueThroughTheDirectSubscribePath()
    {
        RecordingWitness<bool> witness = new();

        using var subscription = new ScriptedObservable<int>(static observer => observer.OnCompleted())
            .IsEmpty()
            .Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual([true])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies an ordinary source that produces a value emits <see langword="false"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NonEmptySourceEmitsFalseThroughTheDirectSubscribePath()
    {
        RecordingWitness<bool> witness = new();

        using var subscription = new ScriptedObservable<int>(static observer =>
            {
                observer.OnNext(SourceValue);
                observer.OnCompleted();
            })
            .IsEmpty()
            .Subscribe(witness);

        await Assert.That(witness.Values.SequenceEqual([false])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
    }
}
