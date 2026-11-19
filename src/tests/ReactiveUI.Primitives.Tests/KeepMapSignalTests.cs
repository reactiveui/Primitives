// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="KeepMapSignal{TSource, TResult}"/>.</summary>
public sealed class KeepMapSignalTests
{
    /// <summary>Scale multiplier.</summary>
    private const int Scale = 10;

    /// <summary>Range length.</summary>
    private const int RangeLength = 4;

    /// <summary>Expected projected even values.</summary>
    private static readonly int[] Expected = [20, 40];

    /// <summary>Chaining Keep then Map produces a KeepMapSignal that filters before projecting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Keep_FollowedByMap_FusesIntoKeepMapSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength)
            .Keep(static x => (x & 1) == 0)
            .Map(static x => x * Scale);

        await Assert.That(pipeline).IsTypeOf<KeepMapSignal<int, int>>();
        List<int> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(Expected)).IsTrue();
    }

    /// <summary>
    /// Live <see cref="KeepMapSignal{TSource, TResult}"/> subscriptions allocate <see cref="KeepMapWitness{TSource, TResult}"/>
    /// and forward values, completion, errors, and disposal.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepMapSignal_LiveSource_ForwardsValuesErrorsCompletionAndCurrentThreadFlag()
    {
        using Signal<int> source = new();
        KeepMapSignal<int, int> signal = new(source, static x => (x & 1) == 0, static x => x * Scale);
        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<int> observer = new();
        using (signal.Subscribe(observer))
        {
            source.OnNext(1);
            source.OnNext(RangeLength);
            source.OnCompleted();
        }

        await Assert.That(observer.Values.SequenceEqual([RangeLength * Scale])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);

        using Signal<int> errSource = new();
        RecordingWitness<int> errObserver = new();
        using var errSub = new KeepMapSignal<int, int>(errSource, static _ => true, static x => x).Subscribe(errObserver);
        InvalidOperationException boom = new("boom");
        errSource.OnError(boom);
        await Assert.That(errObserver.Errors.Single()).IsSameReferenceAs(boom);
    }
}
