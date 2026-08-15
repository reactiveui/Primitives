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

    /// <summary>Covers unfold subscribe argument validation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void UnfoldValidatesObserver() => Assert.Throws<ArgumentNullException>(static () => Signal.Unfold(
            First,
            static value => value < Second,
            static value => value + 1,
            static value => value)
        .Subscribe(null!));

    /// <summary>Covers unfold emission while the condition holds.</summary>
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

    /// <summary>
    /// The observer surface must generate the same sequence as the callback surface: the unfold runs to
    /// exhaustion on subscribe, and a condition that is false from the start yields nothing but a completion.
    /// </summary>
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

    /// <summary>An unfold runs entirely inline on the subscriber's thread, so it never demands a particular one.</summary>
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
