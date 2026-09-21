// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the cold signal that splits a source into keyed groups.</summary>
public class GroupBySignalTests
{
    /// <summary>Each subscription groups the source independently.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_TwiceGroupsEachSubscriptionOnItsOwn()
    {
        Signal<string> source = new();
        GroupBySignal<string, char, string> signal = new(source, static v => v[0], static v => v, null, 0);
        GroupRecordingWitness<char, string> first = new();
        GroupRecordingWitness<char, string> second = new();

        using var firstSubscription = signal.Subscribe(first);
        source.OnNext("a1");
        using var secondSubscription = signal.Subscribe(second);
        source.OnNext("a2");

        await Assert.That(first.Text).IsEqualTo("open ga ga:a1 ga:a2");
        await Assert.That(second.Text).IsEqualTo("open ga ga:a2");
    }

    /// <summary>The subscription is the shared subscription that owns the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ReturnsSharedSubscription()
    {
        Signal<string> source = new();
        GroupBySignal<string, string, string> signal = new(source, static v => v, static v => v, null, 0);

        using var subscription = signal.Subscribe(new RecordingWitness<GroupedSignal<string, string>>());

        await Assert.That(subscription).IsTypeOf<SharedSubscription>();
    }

    /// <summary>A null source or selector is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        var source = Signal.None<string>();

        await Assert.That(static () => new GroupBySignal<string, string, string>(null!, static v => v, static v => v, null, 0)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupBySignal<string, string, string>(source, null!, static v => v, null, 0)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupBySignal<string, string, string>(source, static v => v, null!, null, 0)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>A negative capacity is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NegativeCapacity_ThrowsArgumentOutOfRange() =>
        await Assert.That(static () => new GroupBySignal<string, string, string>(Signal.None<string>(), static v => v, static v => v, null, -1))
            .ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>A null observer is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_NullObserver_ThrowsArgumentNull()
    {
        GroupBySignal<string, string, string> signal = new(Signal.None<string>(), static v => v, static v => v, null, 0);

        await Assert.That(() => signal.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
