// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into windows of a fixed number of values.</summary>
public class SliceCountSignalTests
{
    /// <summary>Each subscription opens its own windows.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TwiceSlicesEachSubscriptionOnItsOwn()
    {
        const int windowSize = 2;
        Signal<string> source = new();
        SliceCountSignal<string> signal = new(source, windowSize, windowSize);
        WindowRecordingWitness<string> first = new();
        WindowRecordingWitness<string> second = new();

        using var firstSubscription = signal.Subscribe(first);
        source.OnNext("a");
        using var secondSubscription = signal.Subscribe(second);
        source.OnNext("b");

        await Assert.That(first.Text).IsEqualTo("open w0 w0:a w0:b w0:done open w1");
        await Assert.That(second.Text).IsEqualTo("open w0 w0:b");
    }

    /// <summary>A null source or a non-positive size is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_InvalidArguments_Throw()
    {
        const int windowSize = 2;
        var source = Signal.None<string>();

        await Assert.That(static () => new SliceCountSignal<string>(null!, windowSize, windowSize)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceCountSignal<string>(source, 0, windowSize)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new SliceCountSignal<string>(source, windowSize, 0)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        const int windowSize = 2;
        SliceCountSignal<string> signal = new(Signal.None<string>(), windowSize, windowSize);

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
