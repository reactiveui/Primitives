// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="UniqueBySignal{T, TKey}"/>.</summary>
public sealed class UniqueBySignalTests
{
    /// <summary>A throwing key selector on a single-element Emit source is invoked rather than skipped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_SingleItemEmitWithThrowingKeySelector_InvokesKeySelectorAndThrows()
    {
        InvalidOperationException expected = new("key-fault");
        RecordingWitness<int> observer = new();

        var thrown = Assert.Throws<InvalidOperationException>(() =>
            Signal.Emit(1).UniqueBy<int, int>(_ => throw expected).Subscribe(observer));
        var thrownRx = Assert.Throws<InvalidOperationException>(() =>
            Signal.Emit(1).DistinctUntilChangedBy<int, int>(_ => throw expected).Subscribe(observer));

        await Assert.That(thrown).IsSameReferenceAs(expected);
        await Assert.That(thrownRx).IsSameReferenceAs(expected);
        await Assert.That(observer.Values).IsEmpty();
    }
}
