// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests empty signal completion.</summary>
public sealed class EmptySignalTests
{
    /// <summary>The immediate sequencer completes inline without requesting a current-thread trampoline.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Subscribe_WhenImmediate_ThenCompletesInline()
    {
        EmptySignal<int> source = new(Sequencer.Immediate);
        RecordingWitness<int> observer = new();

        using var subscription = source.Subscribe(observer);

        await Assert.That(source.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }
}
