// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CountPredicateSignal{T}"/>, <see cref="LongCountPredicateSignal{T}"/>, and <see cref="AnyPredicateSignal{T}"/>.</summary>
public sealed class CountPredicateSignalTests
{
    /// <summary>Range length.</summary>
    private const int RangeLength = 8;

    /// <summary>Expected even element count.</summary>
    private const int ExpectedEvenCount = 4;

    /// <summary>Threshold for LongCount test.</summary>
    private const int Five = 5;

    /// <summary>Expected long count above five.</summary>
    private const long ExpectedLongCount = 3L;

    /// <summary>Where followed by Count fuses into CountPredicateSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Count_AfterWhere_FusesIntoCountPredicateSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength).Where(static x => (x & 1) == 0).Count();
        await Assert.That(pipeline).IsTypeOf<CountPredicateSignal<int>>();

        List<int> observed = [];
        _ = pipeline.Subscribe(observed.Add);
        await Assert.That(observed.SequenceEqual([ExpectedEvenCount])).IsTrue();
    }

    /// <summary>Where followed by LongCount fuses into LongCountPredicateSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task LongCount_AfterWhere_FusesIntoLongCountPredicateSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength).Where(static x => x > Five).LongCount();
        await Assert.That(pipeline).IsTypeOf<LongCountPredicateSignal<int>>();

        List<long> observed = [];
        _ = pipeline.Subscribe(observed.Add);
        await Assert.That(observed.SequenceEqual([ExpectedLongCount])).IsTrue();
    }

    /// <summary>Where followed by Any fuses into AnyPredicateSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Any_AfterWhere_FusesIntoAnyPredicateSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength).Where(static x => x == Five).Any();
        await Assert.That(pipeline).IsTypeOf<AnyPredicateSignal<int>>();

        List<bool> observed = [];
        _ = pipeline.Subscribe(observed.Add);
        await Assert.That(observed.SequenceEqual([true])).IsTrue();
    }
}
