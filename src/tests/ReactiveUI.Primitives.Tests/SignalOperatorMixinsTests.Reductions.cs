// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies operator factory reductions, convenience constructors, and synchronous range/list fast paths.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>Verifies empty and silent reductions across blend, chain, choose, flat-map, fold, and keep-type signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateReductions_BlendThroughMapIndexed_ReturnExpectedSignals()
    {
        var emptyInt = Signal.Empty<int>();
        var silentInt = Signal.Silent<int>();
        var emptyNested = Signal.Empty<IObservable<int>>();
        var silentNested = Signal.Silent<IObservable<int>>();

        await Assert.That(BlendSignal<int>.Create(emptyNested)).IsSameReferenceAs(emptyInt);
        await Assert.That(BlendSignal<int>.Create(silentNested)).IsSameReferenceAs(silentInt);

        RecordingWitness<int> emptyBlendObserver = new();
        new BlendUniqueSink<int>(emptyBlendObserver, EqualityComparer<int>.Default).Run([]);
        await Assert.That(emptyBlendObserver.Completed).IsEqualTo(One);

        await Assert.That(ChainSignal<int>.Create(emptyNested)).IsSameReferenceAs(emptyInt);
        var single = Signal.Return(One);
        await Assert.That(ChainSignal<int>.Create(emptyInt, single)).IsSameReferenceAs(single);
        await Assert.That(MergeSignal<int>.Create(emptyInt, single)).IsSameReferenceAs(single);

        await Assert.That(ChooseSignal<int, int>.Create(emptyInt, static x => new(true, x))).IsSameReferenceAs(emptyInt);
        await Assert.That(ChooseSignal<int, int>.Create(silentInt, static x => new(true, x))).IsSameReferenceAs(silentInt);
        using Signal<int> chooseLive = new();
        ChooseSignal<int, int>.Create(chooseLive, static x => new(true, x)).Subscribe(static _ => { }).Dispose();

        await Assert.That(DistinctSignal<int>.Create(emptyInt, null)).IsSameReferenceAs(emptyInt);
        await Assert.That(FlatMapResultSignal<int, int, int>.Create(emptyInt, Signal.Return, static (a, b) => a + b)).IsSameReferenceAs(emptyInt);
        await Assert.That(FlatMapResultSignal<int, int, int>.Create(silentInt, Signal.Return, static (a, b) => a + b)).IsSameReferenceAs(silentInt);
        await Assert.That(FlatMapSignal<int, int>.Create(emptyInt, Signal.Return)).IsSameReferenceAs(emptyInt);
        await Assert.That(FlatMapSignal<int, int>.Create(silentInt, Signal.Return)).IsSameReferenceAs(silentInt);
        await Assert.That(FlatMapSignal<int, int>.CreateBind(emptyInt, Signal.Return)).IsSameReferenceAs(emptyInt);
        await Assert.That(FlatMapSignal<int, int>.CreateBind(silentInt, Signal.Return)).IsSameReferenceAs(silentInt);
        await Assert.That(FoldSignal<int, int>.Create(emptyInt, 0, static (a, b) => a + b)).IsSameReferenceAs(emptyInt);
        await Assert.That(FoldSignal<int, int>.Create(silentInt, 0, static (a, b) => a + b)).IsSameReferenceAs(silentInt);

        await Assert.That(FusedKeepSignal<int>.Create(emptyInt, static _ => true)).IsSameReferenceAs(emptyInt);
        await Assert.That(FusedKeepSignal<int>.Create(silentInt, static _ => true)).IsSameReferenceAs(silentInt);
        await Assert.That(IgnoreValuesSignal<int>.Create(emptyInt)).IsSameReferenceAs(emptyInt);
        await Assert.That(IgnoreValuesSignal<int>.Create(Signal.Sequence(One, Two))).IsSameReferenceAs(emptyInt);
        await Assert.That(IgnoreValuesSignal<int>.Create(single)).IsSameReferenceAs(emptyInt);

        await Assert.That(KeepTypeSignal<string>.Create(Signal.Empty<object?>())).IsSameReferenceAs(Signal.Empty<string>());
        await Assert.That(KeepTypeSignal<string>.Create(Signal.Silent<object?>())).IsSameReferenceAs(Signal.Silent<string>());
        await Assert.That(MapIndexedSignal<int, int>.Create(emptyInt, static (x, i) => x + i)).IsSameReferenceAs(emptyInt);
        await Assert.That(MapIndexedSignal<int, int>.Create(silentInt, static (x, i) => x + i)).IsSameReferenceAs(silentInt);
    }

    /// <summary>Verifies empty and silent reductions across race, reduce, select-many, skip, switch, take, unique, and unspark signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateReductions_RaceThroughUnspark_ReturnExpectedSignals()
    {
        var emptyInt = Signal.Empty<int>();
        var silentInt = Signal.Silent<int>();
        var emptyNested = Signal.Empty<IObservable<int>>();
        var silentNested = Signal.Silent<IObservable<int>>();
        var single = Signal.Return(One);

        await Assert.That(RaceSignal<int>.Create(emptyNested)).IsSameReferenceAs(emptyInt);
        await Assert.That(RaceSignal<int>.Create(silentNested)).IsSameReferenceAs(silentInt);

        var emptyReduced = ReduceSignal<int, int>.Create(emptyInt, Five, static (a, b) => a + b);
        RecordingWitness<int> reducedObserver = new();
        using var reducedSub = emptyReduced.Subscribe(reducedObserver);
        await Assert.That(reducedObserver.Values.SequenceEqual([Five])).IsTrue();
        await Assert.That(ReduceSignal<int, int>.Create(silentInt, Five, static (a, b) => a + b)).IsSameReferenceAs(silentInt);

        using Signal<int> liveReduce = new();
        RecordingWitness<int> liveReduceObserver = new();
        using (ReduceSignal<int, int>.Create(liveReduce, One, static (a, b) => a + b).Subscribe(liveReduceObserver))
        {
            liveReduce.OnNext(Two);
            liveReduce.OnCompleted();
        }

        await Assert.That(liveReduceObserver.Values.SequenceEqual([Three])).IsTrue();

        await Assert.That(SelectManyEnumerableSignal<int, int>.Create(emptyInt, static x => [x])).IsSameReferenceAs(emptyInt);
        await Assert.That(SelectManyEnumerableSignal<int, int>.Create(silentInt, static x => [x])).IsSameReferenceAs(silentInt);
        await Assert.That(SelectManyResultSignal<int, int, int>.Create(emptyInt, Signal.Return, static (a, b) => a + b)).IsSameReferenceAs(emptyInt);
        await Assert.That(SelectManyResultSignal<int, int, int>.Create(silentInt, Signal.Return, static (a, b) => a + b)).IsSameReferenceAs(silentInt);
        await Assert.That(SelectManySignal<int, int>.Create(emptyInt, Signal.Return)).IsSameReferenceAs(emptyInt);
        await Assert.That(SelectManySignal<int, int>.Create(silentInt, Signal.Return)).IsSameReferenceAs(silentInt);
        await Assert.That(SelectManySignal<int, int>.Create(emptyInt, single)).IsSameReferenceAs(emptyInt);
        await Assert.That(SelectManySignal<int, int>.Create(silentInt, single)).IsSameReferenceAs(silentInt);

        await Assert.That(SkipSignal<int>.Create(emptyInt, One)).IsSameReferenceAs(emptyInt);
        await Assert.That(SkipSignal<int>.Create(silentInt, One)).IsSameReferenceAs(silentInt);
        await Assert.That(SwitchMapSignal<int, int>.Create(emptyInt, Signal.Return)).IsSameReferenceAs(emptyInt);
        await Assert.That(SwitchMapSignal<int, int>.Create(silentInt, Signal.Return)).IsSameReferenceAs(silentInt);
        await Assert.That(SwitchMapSignal<string, int>.CreateSkipNull(Signal.Empty<string?>(), static s => Signal.Return(s.Length))).IsSameReferenceAs(emptyInt);
        await Assert.That(SwitchMapSignal<string, int>.CreateSkipNull(Signal.Silent<string?>(), static s => Signal.Return(s.Length))).IsSameReferenceAs(silentInt);
        await Assert.That(SwitchSignal<int>.Create(emptyNested)).IsSameReferenceAs(emptyInt);
        await Assert.That(SwitchSignal<int>.Create(silentNested)).IsSameReferenceAs(silentInt);

        using Signal<int> switchLive = new();
        var fusedMapSwitch = SwitchSignal<int>.Create(FusedMapSignal<int, IObservable<int>>.Create(switchLive, Signal.Return));
        await Assert.That(fusedMapSwitch).IsTypeOf<SwitchMapSignal<int, int>>();

        await Assert.That(TakeSignal<int>.Create(emptyInt, One)).IsSameReferenceAs(emptyInt);
        await Assert.That(TakeSignal<int>.Create(silentInt, One)).IsSameReferenceAs(silentInt);
        var seq4 = Signal.Sequence(One, Four);
        await Assert.That(TakeSignal<int>.Create(seq4, Five)).IsSameReferenceAs(seq4);
        RecordingWitness<int> seqTakeObserver = new();
        _ = TakeSignal<int>.Create(seq4, Two).Subscribe(seqTakeObserver);
        await Assert.That(seqTakeObserver.Values.SequenceEqual([One, Two])).IsTrue();

        await Assert.That(UniqueSignal<int>.Create(emptyInt, null)).IsSameReferenceAs(emptyInt);
        await Assert.That(UniqueSignal<int>.Create(seq4, null)).IsSameReferenceAs(seq4);
        using Signal<int> uniqueLive = new();
        var uniqueOnce = UniqueSignal<int>.Create(uniqueLive, null);
        await Assert.That(UniqueSignal<int>.Create(uniqueOnce, null)).IsSameReferenceAs(uniqueOnce);

        await Assert.That(UnsparkSignal<int>.Create(Signal.Empty<Spark<int>>())).IsSameReferenceAs(emptyInt);
        await Assert.That(UnsparkSignal<int>.Create(Signal.Silent<Spark<int>>())).IsSameReferenceAs(silentInt);
    }

    /// <summary>Verifies convenience constructors, current-thread query forwarding, and FusedKeep/FusedMap/BufferCount/KeepType/Lead fast paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConvenienceConstructorsAndFirstHalfFastPaths_EmitExpectedItems()
    {
        var empty = Signal.Empty<int>();
        _ = new DefaultIfEmptySignal<int>(empty);
        _ = new DelayStartSignal<int>(empty, TimeSpan.Zero);
        _ = new DistinctBySignal<int, int>(empty, static x => x);
        _ = new DistinctSignal<int>(empty);
        _ = new ShiftSignal<int>(empty, TimeSpan.Zero);
        _ = new TimeIntervalSignal<int>(empty);
        _ = new UniqueSignal<int>(empty);
        using ExpireCoordinator<int> expire = new(empty, TimeSpan.FromSeconds(One), Sequencer.Immediate, new RecordingWitness<int>());

        AsObservableSignal<int> asObsTrue = new(new IntCurrentThreadStub(true));
        AsObservableSignal<int> asObsFalse = new(new IntCurrentThreadStub(false));
        AsObservableSignal<int> asObsPlain = new(empty);
        await Assert.That(asObsTrue.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(asObsFalse.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(asObsPlain.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        FusedKeepSignal<int> fusedKeep = new(empty, static _ => true);
        FusedMapSignal<int, int> fusedMap = new(Signal.Sequence(One, Two), static x => x * Two);
        await Assert.That(fusedKeep.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(fusedMap.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RecordingWitness<int> fusedMapObserver = new();
        await Assert.That(fusedMap.Subscribe(fusedMapObserver)).IsSameReferenceAs(EmptyDisposable.Instance);
        await Assert.That(fusedMapObserver.Values.SequenceEqual([Two, Four])).IsTrue();

        using Signal<int> fusedKeepDoneSource = new();
        RecordingWitness<int> fusedKeepDoneObserver = new();
        using (new FusedKeepSignal<int>(fusedKeepDoneSource, static _ => true).Subscribe(fusedKeepDoneObserver))
        {
            fusedKeepDoneSource.OnCompleted();
        }

        await Assert.That(fusedKeepDoneObserver.Completed).IsEqualTo(One);

        using Signal<int> fusedKeepErrorSource = new();
        RecordingWitness<int> fusedKeepErrorObserver = new();
        using var fusedKeepErrorSub = new FusedKeepSignal<int>(fusedKeepErrorSource, static _ => true).Subscribe(fusedKeepErrorObserver);
        InvalidOperationException boom = new("boom");
        fusedKeepErrorSource.OnError(boom);
        await Assert.That(fusedKeepErrorObserver.Errors.Single()).IsSameReferenceAs(boom);

        RecordingWitness<IList<int>> bufferObserver = new();
        _ = new BufferCountSignal<int>(Signal.Sequence(One, Five), Two, Two).Subscribe(bufferObserver);
        await Assert.That(bufferObserver.Values.Count).IsEqualTo(Three);

        using Signal<object?> keepTypeSource = new();
        RecordingWitness<string> keepTypeObserver = new();
        using (new KeepTypeSignal<string>(keepTypeSource).Subscribe(keepTypeObserver))
        {
            keepTypeSource.OnNext("a");
            keepTypeSource.OnCompleted();
        }

        await Assert.That(keepTypeObserver.Values.SequenceEqual(["a"])).IsTrue();
        await Assert.That(keepTypeObserver.Completed).IsEqualTo(One);

        LeadSignal<int> lead = new(Signal.Return(Two), One);
        await Assert.That(() => lead.Subscribe(null!, static _ => { }, static () => { })).Throws<ArgumentNullException>();
        List<int> leadItems = [];
        var leadDone = 0;
        _ = lead.Subscribe(leadItems.Add, static _ => { }, () => leadDone++);
        await Assert.That(leadItems.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(leadDone).IsEqualTo(One);
    }

    /// <summary>Verifies Skip, SkipWhile, Spark, and Take synchronous fast paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SkipSparkAndTakeFastPaths_EmitExpectedItems()
    {
        RecordingWitness<int> skipListObserver = new();
        _ = new SkipSignal<int>(Signal.FromEnumerable([One, Two, Three]), One).Subscribe(skipListObserver);
        await Assert.That(skipListObserver.Values.SequenceEqual([Two, Three])).IsTrue();

        RecordingWitness<int> skipWhileRangeObserver = new();
        _ = new SkipWhileSignal<int>(Signal.Sequence(One, Four), static x => x < Three).Subscribe(skipWhileRangeObserver);
        await Assert.That(skipWhileRangeObserver.Values.SequenceEqual([Three, Four])).IsTrue();

        using Signal<int> skipWhileLive = new();
        RecordingWitness<int> skipWhileLiveObserver = new();
        using (new SkipWhileSignal<int>(skipWhileLive, static x => x < Two).Subscribe(skipWhileLiveObserver))
        {
            skipWhileLive.OnCompleted();
        }

        await Assert.That(skipWhileLiveObserver.Completed).IsEqualTo(One);

        RecordingWitness<Spark<int>> sparkRangeObserver = new();
        _ = new SparkSignal<int>(Signal.Sequence(One, Two)).Subscribe(sparkRangeObserver);
        await Assert.That(sparkRangeObserver.Values.Count).IsEqualTo(Three);

        RecordingWitness<Spark<string>> sparkListObserver = new();
        _ = new SparkSignal<string>(Signal.FromEnumerable(["a", "b"])).Subscribe(sparkListObserver);
        await Assert.That(sparkListObserver.Values.Count).IsEqualTo(Three);

        RecordingWitness<int> takeZeroObserver = new();
        _ = new TakeSignal<int>(skipWhileLive, 0).Subscribe(takeZeroObserver);
        await Assert.That(takeZeroObserver.Completed).IsEqualTo(One);

        RecordingWitness<int> takeListObserver = new();
        _ = new TakeSignal<int>(Signal.FromEnumerable([One, Two, Three]), Two).Subscribe(takeListObserver);
        await Assert.That(takeListObserver.Values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies TakeWhile, Tap, UniqueBy, and Unspark synchronous fast paths and witness guards.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TakeWhileTapUniqueByAndUnsparkFastPaths_EmitExpectedItems()
    {
        InvalidOperationException boom = new("boom");
        RecordingWitness<int> takeWhileRangeObserver = new();
        _ = new TakeWhileSignal<int>(Signal.Sequence(One, Four), static x => x < Three).Subscribe(takeWhileRangeObserver);
        await Assert.That(takeWhileRangeObserver.Values.SequenceEqual([One, Two])).IsTrue();

        using Signal<int> takeWhileLive = new();
        RecordingWitness<int> takeWhileLiveObserver = new();
        using (new TakeWhileSignal<int>(takeWhileLive, static _ => true).Subscribe(takeWhileLiveObserver))
        {
            takeWhileLive.OnCompleted();
        }

        await Assert.That(takeWhileLiveObserver.Completed).IsEqualTo(One);

        RecordingWitness<int> takeWhileDuplicateObserver = new();
        using TakeWhileWitness<int> takeWhileDuplicate = new(takeWhileDuplicateObserver, static x => x > 0);
        takeWhileDuplicate.OnNext(0);
        takeWhileDuplicate.OnCompleted();
        await Assert.That(takeWhileDuplicateObserver.Completed).IsEqualTo(One);

        List<int> tappedSide = [];
        var tappedDone = 0;
        RecordingWitness<int> tapRangeObserver = new();
        _ = new TapSignal<int>(Signal.Sequence(One, Two), tappedSide.Add, static _ => { }, () => tappedDone++).Subscribe(tapRangeObserver);
        await Assert.That(tappedSide.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(tappedDone).IsEqualTo(One);

        RecordingWitness<int> uniqueByRangeObserver = new();
        _ = new UniqueBySignal<int, int>(Signal.Sequence(One, Four), static x => x / Two).Subscribe(uniqueByRangeObserver);
        await Assert.That(uniqueByRangeObserver.Values.SequenceEqual([One, Two, Four])).IsTrue();

        using Signal<int> uniqueByLive = new();
        RecordingWitness<int> uniqueByLiveObserver = new();
        using (new UniqueBySignal<int, int>(uniqueByLive, static x => x).Subscribe(uniqueByLiveObserver))
        {
            uniqueByLive.OnCompleted();
        }

        await Assert.That(uniqueByLiveObserver.Completed).IsEqualTo(One);

        RecordingWitness<int> unsparkObserver = new();
        using UnsparkWitness<int> unsparkWitness = new(unsparkObserver);
        unsparkWitness.OnError(boom);
        unsparkWitness.OnError(new InvalidOperationException("second"));
        await Assert.That(unsparkObserver.Errors.Count).IsEqualTo(One);
    }

    /// <summary>Verifies the second-empty, first-never and pass-through alternatives of the binary and fallback factories.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateReductions_BinaryAndFallbackAlternatives_ReturnExpectedSignals()
    {
        var emptyInt = Signal.Empty<int>();
        var silentInt = Signal.Silent<int>();
        var single = Signal.Return(One);
        var range = Signal.Sequence(One, Three);
        using Signal<int> live = new();

        await Assert.That(ChainSignal<int>.Create(single, emptyInt)).IsSameReferenceAs(single);
        await Assert.That(ChainSignal<int>.Create(silentInt, single)).IsSameReferenceAs(silentInt);
        await Assert.That(ChainSignal<int>.Create(single, live)).IsTypeOf<ChainSignal<int>>();
        await Assert.That(MergeSignal<int>.Create(single, emptyInt)).IsSameReferenceAs(single);
        await Assert.That(MergeSignal<int>.Create(single, live)).IsTypeOf<MergeSignal<int>>();
        await Assert.That(OnErrorResumeNextSignal<int>.Create(emptyInt, single)).IsSameReferenceAs(single);
        await Assert.That(OnErrorResumeNextSignal<int>.Create(single, live)).IsTypeOf<OnErrorResumeNextSignal<int>>();
        await Assert.That(EmitIfQuietSignal<int>.Create(live, TimeSpan.Zero)).IsSameReferenceAs(live);
        await Assert.That(EmitIfQuietSignal<int>.Create(live, TimeSpan.FromSeconds(One))).IsTypeOf<EmitIfQuietSignal<int>>();
        await Assert.That(EmitIfQuietSignal<int>.Create(live, TimeSpan.Zero, ImmediateSequencer.Instance)).IsSameReferenceAs(live);
        await Assert.That(EmitIfQuietSignal<int>.Create(live, TimeSpan.FromSeconds(One), ImmediateSequencer.Instance))
            .IsTypeOf<EmitIfQuietSignal<int>>();

        foreach (var shortCircuit in new[] { emptyInt, silentInt, range, single })
        {
            await Assert.That(RecoverSignal<int, InvalidOperationException>.Create(shortCircuit, static _ => Signal.Empty<int>()))
                .IsSameReferenceAs(shortCircuit);
            await Assert.That(ResumeSignal<int>.Create(shortCircuit, emptyInt)).IsSameReferenceAs(shortCircuit);
            await Assert.That(ReattemptSignal<int>.Create(shortCircuit, Three)).IsSameReferenceAs(shortCircuit);
            await Assert.That(ReattemptSignal<int>.CreateTotalRuns(shortCircuit, Three)).IsSameReferenceAs(shortCircuit);
        }

        await Assert.That(RecoverSignal<int, InvalidOperationException>.Create(live, static _ => Signal.Empty<int>()))
            .IsTypeOf<RecoverSignal<int, InvalidOperationException>>();
        await Assert.That(ResumeSignal<int>.Create(live, emptyInt)).IsTypeOf<ResumeSignal<int>>();
        await Assert.That(ReattemptSignal<int>.Create(live, 0)).IsSameReferenceAs(live);
        await Assert.That(ReattemptSignal<int>.Create(live, Three)).IsTypeOf<ReattemptSignal<int>>();
        await Assert.That(ReattemptSignal<int>.CreateTotalRuns(live, 0)).IsSameReferenceAs(emptyInt);
        await Assert.That(ReattemptSignal<int>.CreateTotalRuns(live, One)).IsSameReferenceAs(live);
        await Assert.That(ReattemptSignal<int>.CreateTotalRuns(live, Three)).IsTypeOf<ReattemptSignal<int>>();
    }

    /// <summary>Verifies a directly constructed reattempt subscribes non-failing and zero-retry sources without a coordinator.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReattemptSubscribe_NonFailingOrZeroRetrySources_SubscribeDirectly()
    {
        foreach (var source in new[] { Signal.Sequence(One, Two), Signal.Return(One), Signal.Empty<int>(), Signal.Repeat(One, Two) })
        {
            RecordingWitness<int> observer = new();
            using var subscription = new ReattemptSignal<int>(source, Two).Subscribe(observer);
            await Assert.That(observer.Completed).IsEqualTo(One);
        }

        using Signal<int> live = new();
        RecordingWitness<int> zeroRetry = new();
        using (new ReattemptSignal<int>(live, 0).Subscribe(zeroRetry))
        {
            live.OnNext(Five);
        }

        RecordingWitness<int> retrying = new();
        using (new ReattemptSignal<int>(live, Two).Subscribe(retrying))
        {
            live.OnNext(Six);
        }

        await Assert.That(zeroRetry.Values.SequenceEqual([Five])).IsTrue();
        await Assert.That(retrying.Values.SequenceEqual([Six])).IsTrue();
    }

    /// <summary>Verifies the empty, never and pass-through alternatives of the unary filter and side-effect factories.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateReductions_UnaryOperatorAlternatives_ReturnExpectedSignals()
    {
        foreach (var constant in new[] { Signal.Empty<int>(), Signal.Silent<int>() })
        {
            await Assert.That(DistinctBySignal<int, int>.Create(constant, static x => x)).IsSameReferenceAs(constant);
            await Assert.That(UniqueBySignal<int, int>.Create(constant, static x => x)).IsSameReferenceAs(constant);
            await Assert.That(SkipWhileSignal<int>.Create(constant, static _ => true)).IsSameReferenceAs(constant);
            await Assert.That(TakeWhileSignal<int>.Create(constant, static _ => true)).IsSameReferenceAs(constant);
            await Assert.That(TapSignal<int>.Create(constant, static _ => { })).IsSameReferenceAs(constant);
            await Assert.That(KeepWithSignal<int, int>.Create(constant, 0, static (_, _) => true)).IsSameReferenceAs(constant);
            await Assert.That(TapWithSignal<int, int>.Create(constant, 0, static (_, _) => { })).IsSameReferenceAs(constant);
        }

        using Signal<int> live = new();
        await Assert.That(DistinctBySignal<int, int>.Create(live, static x => x)).IsTypeOf<DistinctBySignal<int, int>>();
        await Assert.That(UniqueBySignal<int, int>.Create(live, static x => x)).IsTypeOf<UniqueBySignal<int, int>>();
        await Assert.That(UniqueBySignal<int, int>.Create(live, static x => x, EqualityComparer<int>.Default))
            .IsTypeOf<UniqueBySignal<int, int>>();
        await Assert.That(SkipWhileSignal<int>.Create(live, static _ => true)).IsTypeOf<SkipWhileSignal<int>>();
        await Assert.That(TakeWhileSignal<int>.Create(live, static _ => true)).IsTypeOf<TakeWhileSignal<int>>();
        await Assert.That(TapSignal<int>.Create(live, static _ => { })).IsTypeOf<TapSignal<int>>();
        await Assert.That(KeepWithSignal<int, int>.Create(live, 0, static (_, _) => true)).IsTypeOf<KeepWithSignal<int, int>>();
        await Assert.That(TapWithSignal<int, int>.Create(live, 0, static (_, _) => { })).IsTypeOf<TapWithSignal<int, int>>();
    }

    /// <summary>Verifies range shortening, saturating skip folding and take folding in the skip and take factories.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateReductions_SkipAndTakeFolding_ReturnExpectedSignals()
    {
        var range = Signal.Sequence(One, Three);
        using Signal<int> live = new();

        await Assert.That(SkipSignal<int>.Create(range, Five)).IsSameReferenceAs(Signal.Empty<int>());
        await Assert.That(SkipSignal<int>.Create(range, One)).IsTypeOf<RangeSignal>();

        var nearMax = (SkipSignal<int>)SkipSignal<int>.Create(live, int.MaxValue - One);
        await Assert.That(((SkipSignal<int>)SkipSignal<int>.Create(nearMax, Five)).Count).IsEqualTo(int.MaxValue);
        var small = SkipSignal<int>.Create(live, One);
        await Assert.That(((SkipSignal<int>)SkipSignal<int>.Create(small, Two)).Count).IsEqualTo(Three);

        var taken = TakeSignal<int>.Create(live, Three);
        await Assert.That(TakeSignal<int>.Create(taken, Five)).IsSameReferenceAs(taken);
        await Assert.That(((TakeSignal<int>)TakeSignal<int>.Create(taken, Two)).Count).IsEqualTo(Two);
        await Assert.That(nearMax.Count).IsEqualTo(int.MaxValue - One);
    }

    /// <summary>Verifies distinct keeps ranges with the default comparer and sizes its seen-set for every source and comparer shape.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DistinctCreateAndSubscribe_RangeListAndComparerShapes_EmitUniqueValues()
    {
        var range = Signal.Sequence(One, Three);
        await Assert.That(DistinctSignal<int>.Create(range, null)).IsSameReferenceAs(range);
        await Assert.That(DistinctSignal<int>.Create(range, EqualityComparer<int>.Default)).IsSameReferenceAs(range);

        RecordingWitness<int> rangeCustom = new();
        using (DistinctSignal<int>.Create(range, new PassthroughComparer()).Subscribe(rangeCustom))
        {
            await Assert.That(rangeCustom.Values.SequenceEqual(ExpectedOneToThree)).IsTrue();
        }

        int[] repeated = [One, One, Two];
        RecordingWitness<int> listDefault = new();
        using (DistinctSignal<int>.Create(Signal.FromEnumerable(repeated), null).Subscribe(listDefault))
        {
            await Assert.That(listDefault.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        }

        RecordingWitness<int> listCustom = new();
        using (DistinctSignal<int>.Create(Signal.FromEnumerable(repeated), new PassthroughComparer()).Subscribe(listCustom))
        {
            await Assert.That(listCustom.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        }

        string[] words = ["a", "a", "b"];
        RecordingWitness<string> strings = new();
        using (DistinctSignal<string>.Create(Signal.FromEnumerable(words), null).Subscribe(strings))
        {
            await Assert.That(strings.Values).Count().IsEqualTo(Two);
        }
    }

    /// <summary>Verifies the as-observable, cast and keep-not-null stages report their source's current-thread requirement.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task IsRequiredSubscribeOnCurrentThread_SourceRequiresOrNot_MatchesSource()
    {
        using Signal<int> plainInt = new();
        using Signal<object?> plainObject = new();
        using Signal<string?> plainString = new();

        await Assert.That(new AsObservableSignal<int>(new OptionalCurrentThreadObservable<int>(true)).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(new AsObservableSignal<int>(new OptionalCurrentThreadObservable<int>(false)).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new AsObservableSignal<int>(plainInt).IsRequiredSubscribeOnCurrentThread()).IsFalse();

        await Assert.That(new CastSignal<string>(new OptionalCurrentThreadObservable<object?>(true)).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(new CastSignal<string>(new OptionalCurrentThreadObservable<object?>(false)).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new CastSignal<string>(plainObject).IsRequiredSubscribeOnCurrentThread()).IsFalse();

        await Assert.That(new FusedKeepNotNullSignal<string>(new OptionalCurrentThreadObservable<string?>(true)).IsRequiredSubscribeOnCurrentThread())
            .IsTrue();
        await Assert.That(new FusedKeepNotNullSignal<string>(new OptionalCurrentThreadObservable<string?>(false)).IsRequiredSubscribeOnCurrentThread())
            .IsFalse();
        await Assert.That(new FusedKeepNotNullSignal<string>(plainString).IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>Verifies subscribe-on subscribes inline on the immediate sequencer and schedules on any other sequencer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscribeOnSubscribe_ImmediateAndScheduledSequencers_DeliverSourceValues()
    {
        RecordingWitness<int> immediate = new();
        using (new SubscribeOnSignal<int>(Signal.Return(One), ImmediateSequencer.Instance).Subscribe(immediate))
        {
            await Assert.That(immediate.Values.SequenceEqual([One])).IsTrue();
        }

        for (var attempt = 0; attempt < Two; attempt++)
        {
            RecordingWitness<int> scheduled = new();
            using (new SubscribeOnSignal<int>(Signal.Return(Two), CurrentThreadSequencer.Instance).Subscribe(scheduled))
            {
                await Assert.That(scheduled.Values.SequenceEqual([Two])).IsTrue();
            }
        }
    }

    /// <summary>Verifies the inline callback subscriptions of fused keep, map-keep and map-map stages on range and live sources.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FusedStageCallbackSubscribe_RangeAndLiveSources_DeliverValues()
    {
        for (var attempt = 0; attempt < Two; attempt++)
        {
            List<int> keep = [];
            List<int> mapKeep = [];
            List<int> mapMap = [];
            using Signal<int> live = new();
            var range = Signal.Sequence(One, Three);
            using (new FusedKeepSignal<int>(range, static x => x > One).Subscribe(keep.Add, static _ => { }, static () => { }))
            using (new MapKeepSignal<int, int>(range, static x => x * Two, static x => x > Two).Subscribe(mapKeep.Add, static _ => { }, static () => { }))
            using (new MapMapSignal<int, int, int>(range, static x => x + One, static x => x * Two).Subscribe(mapMap.Add, static _ => { }, static () => { }))
            using (new FusedKeepSignal<int>(live, static x => x > One).Subscribe(keep.Add, static _ => { }, static () => { }))
            using (new MapKeepSignal<int, int>(live, static x => x * Two, static x => x > Two).Subscribe(mapKeep.Add, static _ => { }, static () => { }))
            using (new MapMapSignal<int, int, int>(live, static x => x + One, static x => x * Two).Subscribe(mapMap.Add, static _ => { }, static () => { }))
            {
                live.OnNext(Two);
            }

            await Assert.That(keep.SequenceEqual([Two, Three, Two])).IsTrue();
            await Assert.That(mapKeep.SequenceEqual([Four, Six, Four])).IsTrue();
            await Assert.That(mapMap.SequenceEqual([Four, Six, Eight, Six])).IsTrue();
        }
    }

    /// <summary>Verifies the select-many and async-create constructors reject null arguments.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_NullArguments_ThrowArgumentNullException()
    {
        using Signal<int> live = new();
        Func<int, IObservable<int>> selector = Signal.Return;
        Func<int, int, int> resultSelector = static (a, b) => a + b;

        await Assert.That(static () => new AsyncCreateSignal<int>((Func<IObserver<int>, CancellationToken, Task<IDisposable>>)null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManySignal<int, int>(null!, selector)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManySignal<int, int>(live, (Func<int, IObservable<int>>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManySignal<int, int>(null!, live)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManySignal<int, int>(live, (IObservable<int>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManyResultSignal<int, int, int>(null!, selector, resultSelector)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManyResultSignal<int, int, int>(live, null!, resultSelector)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SelectManyResultSignal<int, int, int>(live, selector, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Stub observable implementing <see cref="IRequireCurrentThread{T}"/> for integer elements.</summary>
    /// <param name="required">Whether current-thread subscription is required.</param>
    private sealed class IntCurrentThreadStub(bool required) : IRequireCurrentThread<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRequiredSubscribeOnCurrentThread() => required;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<int> observer) => EmptyDisposable.Instance;
    }
}
