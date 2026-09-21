// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the keyed sub-signal of a group.</summary>
public class GroupedSignalTests
{
    /// <summary>The group exposes its key and forwards subscriptions to its window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ForwardsToTheWindow()
    {
        SliceWindow<int> window = new(new());
        GroupedSignal<string, int> group = new("key", window);
        RecordingWitness<int> observer = new();
        using var subscription = group.Subscribe(observer);

        window.Publish(1);
        window.Complete();

        await Assert.That(group.Key).IsEqualTo("key");
        await Assert.That(observer.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>A null window is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullWindow_ThrowsArgumentNull() =>
        await Assert.That(static () => new GroupedSignal<string, int>("key", null!)).ThrowsExactly<ArgumentNullException>();
}
