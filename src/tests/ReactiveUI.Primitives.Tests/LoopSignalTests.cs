// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the infinite <c>Loop</c> signal repeats its value until a bounding operator stops it.</summary>
public sealed class LoopSignalTests
{
    /// <summary>The value repeated by the loop.</summary>
    private const int RepeatedValue = 4;

    /// <summary>The number of repetitions a bounded loop is asked for.</summary>
    private const int RequestedRepetitions = 3;

    /// <summary>The values a three-repetition loop must observe.</summary>
    private static readonly int[] ExpectedValues = [RepeatedValue, RepeatedValue, RepeatedValue];

    /// <summary>Verifies a loop bounded by <c>Take</c> repeats the value exactly the requested number of times and stops.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LoopBoundedByTakeRepeatsTheValueAndStops()
{
        List<int> values = [];
        var completions = 0;
        using var subscription = Signal.Loop(RepeatedValue)
            .Take(RequestedRepetitions)
            .Subscribe(values.Add, static _ => { }, () => completions++);
        await Assert.That(values.SequenceEqual(ExpectedValues)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }
}
