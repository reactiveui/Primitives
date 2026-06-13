// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
    [Test]
    public void UnfoldValidatesObserver() => Assert.Throws<ArgumentNullException>(() => Signal.Unfold(
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
        Signal.Unfold(First, static value => value <= Second, static value => value + 1, static value => value)
            .Subscribe(unfolded.Add, error => throw error, () => unfoldCompleted++);
        await Assert.That(unfolded.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(unfoldCompleted).IsEqualTo(1);
    }
}
