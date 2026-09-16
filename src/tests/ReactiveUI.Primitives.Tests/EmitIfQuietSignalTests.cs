// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests quiet-period value delivery.</summary>
public sealed class EmitIfQuietSignalTests
{
    /// <summary>A zero quiet period forwards every value immediately.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Subscribe_WhenDueTimeIsZero_ThenForwardsEveryValue()
    {
        const int secondValue = 2;
        RecordingWitness<int> observer = new();
        EmitIfQuietSignal<int> source = new(Signal.Sequence(1, secondValue), TimeSpan.Zero, Sequencer.Immediate);

        using var subscription = source.Subscribe(observer);

        await Assert.That(observer.Values.SequenceEqual([1, secondValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>A null source is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSource_ThrowsArgumentNull() =>
        await Assert.That(static () => new EmitIfQuietSignal<int>(null!, TimeSpan.Zero, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null sequencer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSequencer_ThrowsArgumentNull() =>
        await Assert.That(static () => new EmitIfQuietSignal<int>(Signal.None<int>(), TimeSpan.Zero, null!))
            .ThrowsExactly<ArgumentNullException>();
}
