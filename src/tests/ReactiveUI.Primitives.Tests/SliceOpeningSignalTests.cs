// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that opens a window per opening signal emission.</summary>
public class SliceOpeningSignalTests
{
    /// <summary>Each subscription subscribes to the source and the opening signal on its own.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_SubscribesSourceAndOpenings()
    {
        Signal<string> source = new();
        Signal<int> openings = new();

        using var subscription = new SliceOpeningSignal<string, int, int>(source, openings, static _ => Signal.Silent<int>())
            .Subscribe(new WindowRecordingWitness<string>());

        await Assert.That(source.HasObservers).IsTrue();
        await Assert.That(openings.HasObservers).IsTrue();
    }

    /// <summary>A null argument is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        var source = Signal.None<string>();
        var openings = Signal.None<int>();

        await Assert.That(() => new SliceOpeningSignal<string, int, int>(null!, openings, static _ => Signal.Silent<int>())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceOpeningSignal<string, int, int>(source, null!, static _ => Signal.Silent<int>())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceOpeningSignal<string, int, int>(source, openings, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        SliceOpeningSignal<string, int, int> signal = new(Signal.None<string>(), Signal.None<int>(), static _ => Signal.Silent<int>());

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
