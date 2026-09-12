// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests deferred source creation.</summary>
public sealed class DeferSignalTests
{
    /// <summary>Each subscription creates its source directly without requesting a current-thread trampoline.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Subscribe_WhenRepeated_ThenCreatesOneSourcePerObserver()
    {
        const int secondValue = 2;
        var calls = 0;
        DeferSignal<int> source = new(() =>
        {
            calls++;
            return Signal.Return(calls);
        });
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();

        using var firstSubscription = source.Subscribe(first);
        using var secondSubscription = source.Subscribe(second);

        await Assert.That(source.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(first.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(second.Values.SequenceEqual([secondValue])).IsTrue();
    }
}
