// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the infinite <c>Loop</c> signal, whose current-thread trampoline repeats a value until a bounding
/// operator disposes the subscription. A bounded loop must stop repeating once the bound is reached instead of
/// livelocking the subscribing thread.
/// </summary>
public sealed class LoopSignalTests
{
    /// <summary>The value repeated by the loop.</summary>
    private const int RepeatedValue = 4;

    /// <summary>The number of repetitions a bounded loop is asked for.</summary>
    private const int RequestedRepetitions = 3;

    /// <summary>The values a three-repetition loop must observe.</summary>
    private static readonly int[] ExpectedValues = [RepeatedValue, RepeatedValue, RepeatedValue];

    /// <summary>How long the bounded loop is given to finish before it is declared livelocked.</summary>
    private static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Verifies a loop bounded by <c>Take</c> repeats the value exactly the requested number of times and stops.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LoopBoundedByTakeRepeatsTheValueAndStops()
    {
        List<int> values = [];
        var completions = 0;

        // The loop runs its ticks on the subscribing thread's trampoline, so subscribe on a dedicated thread:
        // a bounded loop returns in milliseconds, but a regression that livelocked it would otherwise hang the run.
        var worker = Task.Run(() =>
        {
            using var subscription = Signal.Loop(RepeatedValue)
                .Take(RequestedRepetitions)
                .Subscribe(values.Add, static _ => { }, () => completions++);
        });

        await Assert.That(await Task.WhenAny(worker, Task.Delay(CompletionTimeout)) == worker).IsTrue();
        await worker;
        await Assert.That(values.SequenceEqual(ExpectedValues)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }
}
