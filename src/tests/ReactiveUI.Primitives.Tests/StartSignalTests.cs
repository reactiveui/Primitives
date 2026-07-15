// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for <see cref="StartSignal{T}"/>'s current-thread requirement, which downstream current-thread
/// operators query to decide whether they must enter the trampoline before subscribing to it.
/// </summary>
public sealed class StartSignalTests
{
    /// <summary>The value produced by the start function.</summary>
    private const int ProducedValue = 5;

    /// <summary>Verifies a start signal scheduled on the current-thread sequencer requires the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartSignalOnTheCurrentThreadSequencerRequiresTheCurrentThread()
    {
        StartSignal<int> signal = new(static () => ProducedValue, Sequencer.CurrentThread);

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Verifies a start signal scheduled off the current-thread sequencer does not require the current thread.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartSignalOffTheCurrentThreadSequencerDoesNotRequireTheCurrentThread()
    {
        StartSignal<int> signal = new(static () => ProducedValue, Sequencer.Immediate);

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }
}
