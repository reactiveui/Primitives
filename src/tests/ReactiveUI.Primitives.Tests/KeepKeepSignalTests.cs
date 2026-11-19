// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="KeepKeepSignal{T}"/>.</summary>
public sealed class KeepKeepSignalTests
{
    /// <summary>Minimum threshold.</summary>
    private const int LowerBound = 2;

    /// <summary>Range length.</summary>
    private const int RangeLength = 6;

    /// <summary>Expected filtered values.</summary>
    private static readonly int[] Expected = [4, 6];

    /// <summary>Chaining two Keep calls produces a KeepKeepSignal that applies both predicates in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Keep_FollowedByKeep_FusesIntoKeepKeepSignal()
    {
        var pipeline = Signal.Sequence(1, RangeLength)
            .Keep(static x => (x & 1) == 0)
            .Keep(static x => x > LowerBound);

        await Assert.That(pipeline).IsTypeOf<KeepKeepSignal<int>>();
        List<int> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(Expected)).IsTrue();
    }

    /// <summary>Live <see cref="KeepKeepSignal{T}"/> subscriptions allocate <see cref="KeepKeepWitness{T}"/> and forward values, completion, errors, and disposal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepKeepSignal_LiveSource_ForwardsValuesErrorsCompletionAndCurrentThreadFlag()
    {
        using Signal<int> source = new();
        KeepKeepSignal<int> signal = new(source, static x => (x & 1) == 0, static x => x > LowerBound);
        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<int> observer = new();
        using (signal.Subscribe(observer))
        {
            source.OnNext(LowerBound);
            source.OnNext(RangeLength);
            source.OnCompleted();
        }

        await Assert.That(observer.Values.SequenceEqual([RangeLength])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);

        using Signal<int> errSource = new();
        RecordingWitness<int> errObserver = new();
        using var errSub = new KeepKeepSignal<int>(errSource, static _ => true, static _ => true).Subscribe(errObserver);
        InvalidOperationException boom = new("boom");
        errSource.OnError(boom);
        await Assert.That(errObserver.Errors.Single()).IsSameReferenceAs(boom);
    }
}
