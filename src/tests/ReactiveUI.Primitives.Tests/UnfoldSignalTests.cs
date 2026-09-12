// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> unfold generation contracts.</summary>
public class UnfoldSignalTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>Verifies an unfold rejects a null observer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void UnfoldValidatesObserver() => Assert.Throws<ArgumentNullException>(static () => Signal.Unfold(
            First,
            static value => value < Second,
            static value => value + 1,
            static value => value)
        .Subscribe(null!));

    /// <summary>Verifies an unfold emits values while the condition holds and then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnfoldEmitsWhileConditionHolds()
    {
        List<int> unfolded = [];
        var unfoldCompleted = 0;
        _ = Signal.Unfold(First, static value => value <= Second, static value => value + 1, static value => value)
            .Subscribe(unfolded.Add, static error => throw error, () => unfoldCompleted++);
        await Assert.That(unfolded.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(unfoldCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies an observer receives the generated sequence, or only a completion when the condition starts false.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnfoldEmitsTheGeneratedSequenceToObservers()
    {
        RecordingWitness<int> observed = new();
        Signal.Unfold(First, static value => value <= Second, static value => value + 1, static value => value)
            .Subscribe(observed)
            .Dispose();

        await Assert.That(observed.Values.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(observed.Completed).IsEqualTo(1);

        RecordingWitness<int> exhausted = new();
        Signal.Unfold(First, static value => value < First, static value => value + 1, static value => value)
            .Subscribe(exhausted)
            .Dispose();

        await Assert.That(exhausted.Values.Count).IsEqualTo(0);
        await Assert.That(exhausted.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies an unfold never requires subscription on the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnfoldNeverRequiresCurrentThreadSubscription()
    {
        var unfold = (IRequireCurrentThread<int>)Signal.Unfold(
            First,
            static value => value <= Second,
            static value => value + 1,
            static value => value);

        await Assert.That(unfold.IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }
}
