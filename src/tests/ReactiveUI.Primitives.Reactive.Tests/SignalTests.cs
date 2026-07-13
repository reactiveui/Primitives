// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Reactive.Signals;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Smoke tests confirming the Reactive leaf's recompiled shared signals behave correctly.</summary>
public class SignalTests
{
    /// <summary>The values replayed by <c>Signal.FromEnumerable</c>.</summary>
    private static readonly int[] Values = [1, 2, 3];

    /// <summary>The leaf replays an enumerable source through the shared signal pipeline.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FromEnumerable_ReplaysAllValues()
    {
        List<int> received = [];
        using var sub = Signal.FromEnumerable(Values).Subscribe(received.Add);

        await Assert.That(received).IsEquivalentTo(Values, EqualityComparer<int>.Default);
    }

    /// <summary>The leaf's RxVoid binds to <see cref="Unit"/>, so <c>Start</c> yields a System.Reactive unit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Start_YieldsSystemReactiveUnit()
    {
        var ran = false;
        object? emitted = null;
        using var sub = Signal.Start((Action)(() => ran = true), ImmediateScheduler.Instance)
            .Subscribe(value => emitted = value);

        await Assert.That(ran).IsTrue();
        await Assert.That(emitted).IsTypeOf<Unit>();
    }
}
