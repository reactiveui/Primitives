// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into windows ended by their own closing signal.</summary>
public class SliceClosingSignalTests
{
    /// <summary>Each subscription asks for its own closing signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TwiceAsksForClosingSignalsPerSubscription()
    {
        const int subscriptions = 2;
        var requests = 0;
        SliceClosingSignal<string, int> signal = new(
            Signal.Silent<string>(),
            () =>
            {
                requests++;
                return Signal.Silent<int>();
            });

        using var first = signal.Subscribe(new WindowRecordingWitness<string>());
        using var second = signal.Subscribe(new WindowRecordingWitness<string>());

        await Assert.That(requests).IsEqualTo(subscriptions);
    }

    /// <summary>A null source or selector is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        var source = Signal.None<string>();

        await Assert.That(static () => new SliceClosingSignal<string, int>(null!, static () => Signal.Silent<int>())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceClosingSignal<string, int>(source, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        SliceClosingSignal<string, int> signal = new(Signal.None<string>(), static () => Signal.Silent<int>());

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
