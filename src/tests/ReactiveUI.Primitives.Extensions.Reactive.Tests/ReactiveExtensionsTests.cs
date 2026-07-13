// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Reactive.Tests;

/// <summary>Smoke tests confirming the Reactive Extensions leaf's recompiled shared operators behave correctly.</summary>
public class ReactiveExtensionsTests
{
    /// <summary>Value pushed through the signal conversion; only its type is asserted.</summary>
    private const int SourceValue = 42;

    /// <summary>Values pushed through the scheduled array source.</summary>
    private static readonly int[] SourceValues = [1, 2, 3];

    /// <summary>The leaf's RxVoid binds to <see cref="Unit"/>, so <c>AsSignal</c> yields a System.Reactive unit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsSignal_YieldsSystemReactiveUnit()
    {
        object? emitted = null;
        using var sub = Observables.Return(SourceValue).AsSignal().Subscribe(value => emitted = value);

        await Assert.That(emitted).IsTypeOf<Unit>();
    }

    /// <summary>The ISequencer scheduling seam binds to <see cref="IScheduler"/>, so a scheduled source emits all values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FromArray_OnScheduler_EmitsAllValues()
    {
        List<int> received = [];
        using var sub = SourceValues.FromArray(ImmediateScheduler.Instance).Subscribe(received.Add);

        await Assert.That(received).IsEquivalentTo(SourceValues, EqualityComparer<int>.Default);
    }
}
