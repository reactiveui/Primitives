// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests synchronous async operator fusions (<see cref="MapMapSyncSignal{TSource, TMiddle, TDest}"/>,
/// <see cref="KeepMapSyncSignal{TSource, TDest}"/>, <see cref="MapKeepSyncSignal{TSource, TDest}"/>, and <see cref="MapWithSyncSignal{TSource, TState, TDest}"/>).
/// </summary>
public sealed class MapMapSyncSignalTests
{
    /// <summary>Range item count.</summary>
    private const int ItemCount = 6;

    /// <summary>Projection scale factor.</summary>
    private const int ScaleFactor = 10;

    /// <summary>Filter threshold.</summary>
    private const int FilterThreshold = 25;

    /// <summary>First expected fused element.</summary>
    private const int FirstFused = 41;

    /// <summary>Second expected fused element.</summary>
    private const int SecondFused = 61;

    /// <summary>Lower bound for stateful filter.</summary>
    private const int LowerBound = 2;

    /// <summary>Expected stateful sequence.</summary>
    private static readonly int[] ExpectedStateful = [30, 40, 50, 60];

    /// <summary>Chained synchronous Keep and Map stages fuse and deliver expected values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepAndMap_ChainedSynchronousStages_DeliverExpectedValues()
    {
        var result = await SignalAsync.Range(1, ItemCount)
            .Keep(static x => (x & 1) == 0)
            .Map(static x => x * ScaleFactor)
            .Map(static x => x + 1)
            .Keep(static x => x > FilterThreshold)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FirstFused, SecondFused]);
    }

    /// <summary>KeepWith followed by MapWith projects filtered elements with state.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepWithAndMapWith_SynchronousStages_DeliverExpectedValues()
    {
        var result = await SignalAsync.Range(1, ItemCount)
            .KeepWith(LowerBound, static (min, x) => x > min)
            .MapWith(ScaleFactor, static (factor, x) => x * factor)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(ExpectedStateful);
    }

    /// <summary>Chained Keep-Keep synchronous stages fuse and forward values, resumed errors, completion, and disposal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeepKeepSyncSignal_LiveSource_ForwardsValuesErrorsAndCompletion()
    {
        var keepSource = ReactiveUI.Primitives.Async.Signals.Signal.Create<int>();
        var keepFused = keepSource.Values
            .Keep(static x => x > 0)
            .Keep(static x => (x & 1) == 0);
        await Assert.That(keepFused).IsTypeOf<KeepKeepSyncSignal<int>>();

        List<int> keepValues = [];
        List<Exception> keepErrors = [];
        var keepCompleted = false;
        await using (await keepFused.SubscribeAsync(
            (v, _) =>
            {
                keepValues.Add(v);
                return default;
            },
            (ex, _) =>
            {
                keepErrors.Add(ex);
                return default;
            },
            _ =>
            {
                keepCompleted = true;
                return default;
            }))
        {
            await keepSource.OnNextAsync(0, CancellationToken.None);
            await keepSource.OnNextAsync(1, CancellationToken.None);
            await keepSource.OnNextAsync(LowerBound, CancellationToken.None);
            await keepSource.OnErrorResumeAsync(new InvalidOperationException("keep"), CancellationToken.None);
            await keepSource.OnCompletedAsync(Result.Success);
        }

        await Assert.That(keepValues).IsCollectionEqualTo([LowerBound]);
        await Assert.That(keepErrors).Count().IsEqualTo(1);
        await Assert.That(keepCompleted).IsTrue();
    }

    /// <summary>Chained Map-Map synchronous stages fuse and forward values, resumed errors, completion, and disposal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MapMapSyncSignal_LiveSource_ForwardsValuesErrorsAndCompletion()
    {
        var mapSource = ReactiveUI.Primitives.Async.Signals.Signal.Create<int>();
        var mapFused = mapSource.Values
            .Map(static x => x * ScaleFactor)
            .Map(static x => x + 1);
        await Assert.That(mapFused).IsTypeOf<MapMapSyncSignal<int, int, int>>();

        List<int> mapValues = [];
        List<Exception> mapErrors = [];
        var mapCompleted = false;
        await using (await mapFused.SubscribeAsync(
            (v, _) =>
            {
                mapValues.Add(v);
                return default;
            },
            (ex, _) =>
            {
                mapErrors.Add(ex);
                return default;
            },
            _ =>
            {
                mapCompleted = true;
                return default;
            }))
        {
            await mapSource.OnNextAsync(LowerBound, CancellationToken.None);
            await mapSource.OnErrorResumeAsync(new InvalidOperationException("map"), CancellationToken.None);
            await mapSource.OnCompletedAsync(Result.Success);
        }

        await Assert.That(mapValues).IsCollectionEqualTo([(LowerBound * ScaleFactor) + 1]);
        await Assert.That(mapErrors).Count().IsEqualTo(1);
        await Assert.That(mapCompleted).IsTrue();
    }

    /// <summary>Verifies synchronous operator reductions on empty/never sources, double negation, Skip/Take fusions, and resumed error forwarding.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SyncOperatorReductionsAndErrorResume_BehaveAsExpected()
    {
        var empty = SignalAsync.Empty<int>();
        var never = SignalAsync.Never<int>();

        await Assert.That(KeepSyncSignal<int>.Create(empty, static _ => true)).IsSameReferenceAs(empty);
        await Assert.That(KeepSyncSignal<int>.Create(never, static _ => true)).IsSameReferenceAs(never);
        await Assert.That(MapSyncSignal<int, int>.Create(empty, static x => x)).IsSameReferenceAs(empty);
        await Assert.That(MapSyncSignal<int, int>.Create(never, static x => x)).IsSameReferenceAs(never);
        await Assert.That(MapWithSyncSignal<int, int, int>.Create(empty, 1, static (s, x) => s + x)).IsSameReferenceAs(empty);
        await Assert.That(MapWithSyncSignal<int, int, int>.Create(never, 1, static (s, x) => s + x)).IsSameReferenceAs(never);

        KeepSyncSignal<int> uninitializedKeep = new(null!, null!);
        await Assert.That(((IKeepFusableAsync<int>)uninitializedKeep).TryFuseKeep(static _ => true)).IsNull();
        await Assert.That(((IMapFusableAsync<int>)uninitializedKeep).TryFuseMap(static x => x)).IsNull();

        MapSyncSignal<int, int> uninitializedMap = new(null!, null!);
        await Assert.That(((IKeepFusableAsync<int>)uninitializedMap).TryFuseKeep(static _ => true)).IsNull();
        await Assert.That(((IMapFusableAsync<int>)uninitializedMap).TryFuseMap(static x => x)).IsNull();

        var boolLive = ReactiveUI.Primitives.Async.Signals.Signal.Create<bool>();
        var negatedOnce = NotSignal.Create(boolLive.Values);
        await Assert.That(NotSignal.Create(negatedOnce)).IsSameReferenceAs(boolLive.Values);
        await Assert.That(NotSignal.Create(SignalAsync.Empty<bool>())).IsSameReferenceAs(SignalAsync.Empty<bool>());
        await Assert.That(NotSignal.Create(SignalAsync.Never<bool>())).IsSameReferenceAs(SignalAsync.Never<bool>());

        var skipFused = SkipSignal<int>.Create(SkipSignal<int>.Create(boolLive.Values.Map(static _ => 1), 1), LowerBound);
        await Assert.That(skipFused).IsTypeOf<SkipSignal<int>>();

        var takeFused = TakeSignal<int>.Create(TakeSignal<int>.Create(boolLive.Values.Map(static _ => 1), ItemCount), LowerBound);
        await Assert.That(takeFused).IsTypeOf<TakeSignal<int>>();
        await Assert.That(TakeSignal<int>.Create(empty, 1)).IsSameReferenceAs(empty);
        await Assert.That(TakeSignal<int>.Create(never, 1)).IsSameReferenceAs(never);

        var errSource = ReactiveUI.Primitives.Async.Signals.Signal.Create<int>();
        var resumedCount = 0;
        Func<Exception, CancellationToken, ValueTask> onError = (_, _) =>
        {
            resumedCount++;
            return default;
        };

        await using var s1 = await errSource.Values.Keep(static x => x > 0).Map(static x => x).SubscribeAsync(static (_, _) => default, onError);
        await using var s2 = await errSource.Values.Map(static x => x).Keep(static x => x > 0).SubscribeAsync(static (_, _) => default, onError);
        await using var s3 = await errSource.Values.KeepWith(1, static (s, x) => x > s).SubscribeAsync(static (_, _) => default, onError);
        await using var s4 = await errSource.Values.MapWith(1, static (s, x) => x + s).SubscribeAsync(static (_, _) => default, onError);

        await errSource.OnErrorResumeAsync(new InvalidOperationException("resume"), CancellationToken.None);
        const int ExpectedResumedObservers = 4;
        await Assert.That(resumedCount).IsEqualTo(ExpectedResumedObservers);
    }
}
