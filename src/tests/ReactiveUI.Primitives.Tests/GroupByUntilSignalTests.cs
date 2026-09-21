// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into keyed groups that end with a duration signal.</summary>
public class GroupByUntilSignalTests
{
    /// <summary>The subscription is the shared subscription that owns the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ReturnsSharedSubscription()
    {
        Signal<string> source = new();
        GroupByUntilSignal<string, string, string, int> signal = new(source, static v => v, static v => v, static _ => Signal.Silent<int>(), null, 0);

        using var subscription = signal.Subscribe(new RecordingWitness<GroupedSignal<string, string>>());

        await Assert.That(subscription).IsTypeOf<SharedSubscription>();
        await Assert.That(source.HasObservers).IsTrue();
    }

    /// <summary>A null argument is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        var source = Signal.None<string>();

        await Assert.That(static () => new GroupByUntilSignal<string, string, string, int>(null!, static v => v, static v => v, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilSignal<string, string, string, int>(source, null!, static v => v, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilSignal<string, string, string, int>(source, static v => v, null!, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilSignal<string, string, string, int>(source, static v => v, static v => v, null!, null, 0))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A negative capacity is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NegativeCapacity_ThrowsArgumentOutOfRange() =>
        await Assert.That(static () => new GroupByUntilSignal<string, string, string, int>(Signal.None<string>(), static v => v, static v => v, static _ => Signal.Silent<int>(), null, -1))
            .ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        GroupByUntilSignal<string, string, string, int> signal = new(Signal.None<string>(), static v => v, static v => v, static _ => Signal.Silent<int>(), null, 0);

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
