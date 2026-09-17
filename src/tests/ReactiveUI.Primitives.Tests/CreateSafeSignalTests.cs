// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests subscriptions to <see cref="CreateSafeSignal{T}"/>.</summary>
public sealed class CreateSafeSignalTests
{
    /// <summary>The value the subscribe delegate emits.</summary>
    private const int Value = 4;

    /// <summary>A subscribe delegate that returns no disposable still delivers and yields a disposable subscription.</summary>
    /// <param name="currentThreadRequired">Whether subscription is dispatched through the current-thread sequencer.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Subscribe_DelegateReturnsNull_DeliversAndDisposes(bool currentThreadRequired)
    {
        RecordingWitness<int> observer = new();
        CreateSafeSignal<int> signal = new(
            static witness =>
            {
                witness.OnNext(Value);
                return null!;
            },
            currentThreadRequired);

        using (signal.Subscribe(observer))
        {
            await Assert.That(observer.Values.SequenceEqual([Value])).IsTrue();
        }
    }
}
