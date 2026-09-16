// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests time-window buffering.</summary>
public sealed class BufferSignalTests
{
    /// <summary>A zero window emits each source value as a separate buffer without scheduling.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Subscribe_WhenWindowIsZero_ThenEmitsIndividualBuffers()
    {
        const int secondValue = 2;
        RecordingWitness<IList<int>> observer = new();
        BufferSignal<int> source = new(Signal.Sequence(1, secondValue), TimeSpan.Zero, Sequencer.Immediate);

        using var subscription = source.Subscribe(observer);

        await Assert.That(observer.Values.Count).IsEqualTo(secondValue);
        await Assert.That(observer.Values[0].SequenceEqual([1])).IsTrue();
        await Assert.That(observer.Values[1].SequenceEqual([secondValue])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>A null source is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSource_ThrowsArgumentNull() =>
        await Assert.That(static () => new BufferSignal<int>(null!, TimeSpan.Zero, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null sequencer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSequencer_ThrowsArgumentNull() =>
        await Assert.That(static () => new BufferSignal<int>(Signal.None<int>(), TimeSpan.Zero, null!))
            .ThrowsExactly<ArgumentNullException>();
}
