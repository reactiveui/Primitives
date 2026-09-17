// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation of <see cref="CollectSignal{T}"/>.</summary>
public sealed class CollectSignalTests
{
    /// <summary>A null source is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSource_ThrowsArgumentNull() =>
        await Assert.That(static () => new CollectSignal<int>(null!, TimeSpan.Zero, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null sequencer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSequencer_ThrowsArgumentNull() =>
        await Assert.That(static () => new CollectSignal<int>(Signal.None<int>(), TimeSpan.Zero, null!))
            .ThrowsExactly<ArgumentNullException>();
}
