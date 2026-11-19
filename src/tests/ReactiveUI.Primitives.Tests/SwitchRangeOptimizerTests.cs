// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SwitchRangeOptimizer"/> and <c>Switch</c> subscribe-time list evaluation.</summary>
public sealed class SwitchRangeOptimizerTests
{
    /// <summary>Range length.</summary>
    private const int Two = 2;

    /// <summary>Second range start.</summary>
    private const int Ten = 10;

    /// <summary>Second range final element.</summary>
    private const int Eleven = 11;

    /// <summary>Replacement non-range element.</summary>
    private const int FortyTwo = 42;

    /// <summary>Verifies <see cref="SwitchRangeOptimizer.TrySubscribeRanges{T}"/> emits range lists directly, completes empty lists, and rejects non-range inner sources.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TrySubscribeRanges_EmitsRangeListsAndRejectsNonRanges()
    {
        RecordingWitness<int> rangeObserver = new();
        var ok = SwitchRangeOptimizer.TrySubscribeRanges(
            Signal.FromEnumerable([Signal.Sequence(1, Two), Signal.Sequence(Ten, Two)]),
            rangeObserver);
        await Assert.That(ok).IsTrue();
        await Assert.That(rangeObserver.Values.SequenceEqual([1, Two, Ten, Eleven])).IsTrue();
        await Assert.That(rangeObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> emptyObserver = new();
        var okEmpty = SwitchRangeOptimizer.TrySubscribeRanges(
            Signal.FromEnumerable<IObservable<int>>([]),
            emptyObserver);
        await Assert.That(okEmpty).IsTrue();
        await Assert.That(emptyObserver.Values).IsEmpty();
        await Assert.That(emptyObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> nonRangeObserver = new();
        var rejectedNonRange = SwitchRangeOptimizer.TrySubscribeRanges(
            Signal.FromEnumerable([Signal.Sequence(1, Two), Signal.Return(FortyTwo)]),
            nonRangeObserver);
        await Assert.That(rejectedNonRange).IsFalse();
        await Assert.That(nonRangeObserver.Values).IsEmpty();
        await Assert.That(nonRangeObserver.Completed).IsEqualTo(0);
    }

    /// <summary>Verifies <c>Switch</c>, <c>SwitchTo</c>, and <see cref="Signal.Switch{T}"/> observe list mutations made after building the pipeline.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Switch_MutatingBackingListAfterBuild_ReflectsCurrentListOnEachSubscribe()
    {
        List<IObservable<int>> sources = [Signal.Sequence(1, Two)];
        var switchedViaRxName = Signal.FromEnumerable(sources).Switch();
        var switchedViaExtension = Signal.FromEnumerable(sources).SwitchTo();
        var switchedViaStatic = Signal.Switch(Signal.FromEnumerable(sources));

        sources.Add(Signal.Sequence(Ten, Two));

        RecordingWitness<int> rxNameAddedObserver = new();
        using var sub0 = switchedViaRxName.Subscribe(rxNameAddedObserver);
        await Assert.That(rxNameAddedObserver.Values.SequenceEqual([1, Two, Ten, Eleven])).IsTrue();
        await Assert.That(rxNameAddedObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> addedRangeObserver = new();
        using var sub1 = switchedViaExtension.Subscribe(addedRangeObserver);
        await Assert.That(addedRangeObserver.Values.SequenceEqual([1, Two, Ten, Eleven])).IsTrue();
        await Assert.That(addedRangeObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> staticAddedObserver = new();
        using var sub2 = switchedViaStatic.Subscribe(staticAddedObserver);
        await Assert.That(staticAddedObserver.Values.SequenceEqual([1, Two, Ten, Eleven])).IsTrue();

        sources[1] = Signal.Return(FortyTwo);

        RecordingWitness<int> rxNameReplacedObserver = new();
        using var sub3 = switchedViaRxName.Subscribe(rxNameReplacedObserver);
        await Assert.That(rxNameReplacedObserver.Values.SequenceEqual([1, Two, FortyTwo])).IsTrue();
        await Assert.That(rxNameReplacedObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> replacedNonRangeObserver = new();
        using var sub4 = switchedViaExtension.Subscribe(replacedNonRangeObserver);
        await Assert.That(replacedNonRangeObserver.Values.SequenceEqual([1, Two, FortyTwo])).IsTrue();
        await Assert.That(replacedNonRangeObserver.Completed).IsEqualTo(1);

        RecordingWitness<int> staticReplacedObserver = new();
        using var sub5 = switchedViaStatic.Subscribe(staticReplacedObserver);
        await Assert.That(staticReplacedObserver.Values.SequenceEqual([1, Two, FortyTwo])).IsTrue();
    }
}
