// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into windows delimited by a boundary signal.</summary>
public class SliceBoundarySignalTests
{
    /// <summary>Each subscription subscribes to the source and the boundary on its own.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_SubscribesSourceAndBoundary()
    {
        Signal<string> source = new();
        Signal<int> boundary = new();

        using var subscription = new SliceBoundarySignal<string, int>(source, boundary).Subscribe(new WindowRecordingWitness<string>());

        await Assert.That(source.HasObservers).IsTrue();
        await Assert.That(boundary.HasObservers).IsTrue();
    }

    /// <summary>A null source or boundary is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        var source = Signal.None<string>();

        await Assert.That(() => new SliceBoundarySignal<string, string>(null!, source)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceBoundarySignal<string, string>(source, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        SliceBoundarySignal<string, string> signal = new(Signal.None<string>(), Signal.None<string>());

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
