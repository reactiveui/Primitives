// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Async.Reactive.Tests;

/// <summary>Smoke tests confirming the Reactive async leaf's recompiled shared source behaves correctly.</summary>
public class ReactiveAsyncTests
{
    /// <summary>Sentinel value used by the tests.</summary>
    private const int Sentinel = 42;

    /// <summary>The Core engine compiled into the leaf still emits values through the async subscribe path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Return_EmitsValueAndCompletes()
    {
        var result = await SignalAsync.Return(Sentinel).ToListAsync();
        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(Sentinel);
    }

    /// <summary>The leaf's RxVoid binds to <see cref="Unit"/>, so <c>AsSignal</c> yields a System.Reactive unit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsSignal_YieldsSystemReactiveUnit()
    {
        var result = await SignalAsync.Return(1).AsSignal().ToListAsync();
        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0]).IsTypeOf<Unit>();
    }

    /// <summary>The ISequencer scheduling seam binds to <see cref="IScheduler"/>, so <c>WitnessOn</c> accepts a System.Reactive scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WitnessOn_OnScheduler_EmitsValue()
    {
        var result = await SignalAsync.Return(Sentinel).WitnessOn(ImmediateScheduler.Instance).ToListAsync();
        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(Sentinel);
    }
}
