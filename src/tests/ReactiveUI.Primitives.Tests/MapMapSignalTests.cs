// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="MapMapSignal{TSource, TMiddle, TResult}"/>.</summary>
public sealed class MapMapSignalTests
{
    /// <summary>Multiplier applied in the second stage.</summary>
    private const int Multiplier = 2;

    /// <summary>Range length.</summary>
    private const int RangeLength = 3;

    /// <summary>Expected projected sequence.</summary>
    private static readonly int[] Expected = [4, 6, 8];

    /// <summary>Chaining two Map calls produces a single MapMapSignal that delivers composed results.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_FollowedByMap_FusesIntoMapMapSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength)
            .Map(static x => x + 1)
            .Map(static x => x * Multiplier);

        await Assert.That(pipeline).IsTypeOf<MapMapSignal<int, int, int>>();
        List<int> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(Expected)).IsTrue();
    }

    /// <summary>
    /// Verifies <see cref="MapMapSignal{TSource, TMiddle, TResult}"/> observer subscriptions on range and live sources
    /// plus <see cref="MapMapWitness{TSource, TMiddle, TResult}"/> terminals.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MapMapSignal_ObserverSubscribeOnRangeAndLiveSource_ForwardsExpectedNotifications()
    {
        MapMapSignal<int, int, int> rangeSignal = new(
            Signal.Sequence(1, RangeLength),
            static x => x + 1,
            static x => x * Multiplier);
        await Assert.That(rangeSignal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(() => rangeSignal.Subscribe((IObserver<int>)null!)).Throws<ArgumentNullException>();

        RecordingWitness<int> rangeObserver = new();
        using var rangeSub = rangeSignal.Subscribe(rangeObserver);
        await Assert.That(rangeObserver.Values.SequenceEqual(Expected)).IsTrue();
        await Assert.That(rangeObserver.Completed).IsEqualTo(1);

        using Signal<int> live = new();
        MapMapSignal<int, int, int> liveSignal = new(live, static x => x + 1, static x => x * Multiplier);
        RecordingWitness<int> liveObserver = new();
        using (liveSignal.Subscribe(liveObserver))
        {
            live.OnNext(1);
            live.OnCompleted();
        }

        await Assert.That(liveObserver.Values.SequenceEqual([Expected[0]])).IsTrue();
        await Assert.That(liveObserver.Completed).IsEqualTo(1);

        using Signal<int> errSource = new();
        RecordingWitness<int> errObserver = new();
        using var errSub = new MapMapSignal<int, int, int>(errSource, static x => x, static x => x).Subscribe(errObserver);
        InvalidOperationException boom = new("boom");
        errSource.OnError(boom);
        await Assert.That(errObserver.Errors.Single()).IsSameReferenceAs(boom);
    }
}
