// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Data-driven parity tests proving each System.Reactive/LINQ name builds a behaviorally identical sink to its
/// Primitives-named counterpart. Each operator pair is one data-source row consumed by a single test body, so the
/// behavior is asserted once and checked for both names (and for identity between them).
/// </summary>
public class RxNamesTests
{
    /// <summary>The multiplier/state used by projection cases.</summary>
    private const int Ten = 10;

    /// <summary>The divisor used to select even values.</summary>
    private const int Two = 2;

    /// <summary>The fold/aggregate seed.</summary>
    private const int Seed = 0;

    /// <summary>The value one, pushed by the binary drive scripts.</summary>
    private const int One = 1;

    /// <summary>The value three, pushed by the binary drive scripts.</summary>
    private const int Three = 3;

    /// <summary>The value twenty, pushed by the binary drive scripts.</summary>
    private const int Twenty = 20;

    /// <summary>The value thirty, pushed by the binary drive scripts.</summary>
    private const int Thirty = 30;

    /// <summary>The value one hundred, pushed by the multi-source CombineLatest tests.</summary>
    private const int OneHundred = 100;

    /// <summary>The value two hundred, pushed by the multi-source CombineLatest tests.</summary>
    private const int TwoHundred = 200;

    /// <summary>The number of sources in the widest CombineLatest overload.</summary>
    private const int Sixteen = 16;

    /// <summary>An invalid negative count/interval used by the out-of-range tests.</summary>
    private const int NegativeOne = -1;

    /// <summary>A shared error message.</summary>
    private const string Boom = "boom";

    /// <summary>The fixed delay/timeout in ticks.</summary>
    private const long DueTicks = 1;

    /// <summary>The amount the virtual clock is advanced, comfortably past <see cref = "DueTicks"/>.</summary>
    private const long AdvanceTicks = 5;

    /// <summary>Source values 1..5.</summary>
    private static readonly int[] _oneToFive = [1, 2, 3, 4, 5];

    /// <summary>Source values 1..3.</summary>
    private static readonly int[] _oneToThree = [1, 2, 3];

    /// <summary>Each of 1..5 doubled.</summary>
    private static readonly int[] _doubled = [2, 4, 6, 8, 10];

    /// <summary>Even values of 1..5.</summary>
    private static readonly int[] _evens = [2, 4];

    /// <summary>Running sum of 1..5.</summary>
    private static readonly int[] _runningSum = [1, 3, 6, 10, 15];

    /// <summary>Final sum of 1..5.</summary>
    private static readonly int[] _finalSum = [15];

    /// <summary>Each of 1..5 plus the state value <see cref = "Ten"/>.</summary>
    private static readonly int[] _plusTen = [11, 12, 13, 14, 15];

    /// <summary>Each of 1..3 emitted twice.</summary>
    private static readonly int[] _fanned = [1, 1, 2, 2, 3, 3];

    /// <summary>A sequence containing adjacent duplicate values.</summary>
    private static readonly int[] _adjacentDuplicates = [1, 1, 2, 3, 3];

    /// <summary>The adjacent-duplicate sequence with duplicates removed.</summary>
    private static readonly int[] _deduplicated = [1, 2, 3];

    /// <summary>An empty result.</summary>
    private static readonly int[] _empty = [];

    /// <summary>Inner sequences for the higher-order family.</summary>
    private static readonly int[][] _twoInners = [[1, 2], [3, 4]];

    /// <summary>The flattened higher-order result (merge/concat/switch of synchronous inners).</summary>
    private static readonly int[] _flattened = [1, 2, 3, 4];

    /// <summary>The first inner sequence (the Amb/Race winner).</summary>
    private static readonly int[] _firstInner = [1, 2];

    /// <summary>Expected output for the Zip drive script.</summary>
    private static readonly int[] _zipped = [11, 22, 33];

    /// <summary>Expected output for the CombineLatest drive script.</summary>
    private static readonly int[] _combined = [11, 12, 22];

    /// <summary>Expected output for the WithLatestFrom drive script.</summary>
    private static readonly int[] _latched = [11, 12, 23];

    /// <summary>A single forwarded value before a terminal notification.</summary>
    private static readonly int[] _tenOnly = [10];

    /// <summary>A source value followed by the fallback sequence after Resume switches.</summary>
    private static readonly int[] _tenThenFallback = [10, 1, 2, 3];

    /// <summary>Provides the unary <c>IObservable&lt;int&gt; -&gt; IObservable&lt;int&gt;</c> parity cases.</summary>
    /// <returns>The unary parity cases.</returns>
    public static IEnumerable<UnaryCase> UnaryCases()
    {
        yield return new("Select-Map", s => s.Map(Double), s => s.Select(Double), _oneToFive, _doubled);
        yield return new("Where-Keep", s => s.Keep(IsEven), s => s.Where(IsEven), _oneToFive, _evens);
        yield return new("Scan-Fold", s => s.Fold(Seed, Add), s => s.Scan(Seed, Add), _oneToFive, _runningSum);
        yield return new(
            "Aggregate-Reduce",
            s => s.Reduce(Seed, Add),
            s => s.Aggregate(Seed, Add),
            _oneToFive,
            _finalSum);
        yield return new(
            "DistinctUntilChanged-Unique",
            s => s.Unique(),
            s => s.DistinctUntilChanged(),
            _adjacentDuplicates,
            _deduplicated);
        yield return new(
            "DistinctUntilChangedBy-UniqueBy",
            s => s.UniqueBy(Identity),
            s => s.DistinctUntilChangedBy(Identity),
            _adjacentDuplicates,
            _deduplicated);
        yield return new(
            "IgnoreElements-IgnoreValues",
            s => s.IgnoreValues(),
            s => s.IgnoreElements(),
            _oneToFive,
            _empty);
        yield return new(
            "SelectWith-MapWith",
            s => s.MapWith(Ten, AddState),
            s => s.SelectWith(Ten, AddState),
            _oneToFive,
            _plusTen);
        yield return new(
            "WhereWith-KeepWith",
            s => s.KeepWith(Two, IsMultiple),
            s => s.WhereWith(Two, IsMultiple),
            _oneToFive,
            _evens);
        yield return new("Do-Tap", s => s.Tap(Ignore), s => s.Do(Ignore), _oneToFive, _oneToFive);
        yield return new(
            "DoWith-TapWith",
            s => s.TapWith(Ten, IgnoreState),
            s => s.DoWith(Ten, IgnoreState),
            _oneToFive,
            _oneToFive);
        yield return new("SelectMany-FlatMap", s => s.FlatMap(Fan), s => s.SelectMany(Fan), _oneToThree, _fanned);
        yield return new(
            "Materialize-Spark",
            s => s.Spark().Unspark(),
            s => s.Materialize().Dematerialize(),
            _oneToFive,
            _oneToFive);
    }

    /// <summary>Provides the higher-order <c>source-of-sources</c> parity cases.</summary>
    /// <returns>The higher-order parity cases.</returns>
    public static IEnumerable<HigherOrderCase> HigherOrderCases()
    {
        yield return new("Merge-Blend", o => o.Blend(), o => o.Merge(), _twoInners, _flattened);
        yield return new("Concat-Chain", o => o.Chain(), o => o.Concat(), _twoInners, _flattened);
        yield return new("Switch-SwitchTo", o => o.SwitchTo(), o => o.Switch(), _twoInners, _flattened);
        yield return new("Amb-Race", o => o.Race(), o => o.Amb(), _twoInners, _firstInner);
    }

    /// <summary>Provides the binary <c>(left, right) -&gt; result</c> parity cases.</summary>
    /// <returns>The binary parity cases.</returns>
    public static IEnumerable<BinaryCase> BinaryCases()
    {
        yield return new("Zip-Pair", (l, r) => l.Pair(r, Add), (l, r) => l.Zip(r, Add), DriveZip, _zipped);
        yield return new(
            "CombineLatest-SyncLatest",
            (l, r) => l.SyncLatest(r, Add),
            (l, r) => l.CombineLatest(r, Add),
            DriveCombine,
            _combined);
        yield return new(
            "WithLatestFrom-Latch",
            (l, r) => l.Latch(r, Add),
            (l, r) => l.WithLatestFrom(r, Add),
            DriveLatch,
            _latched);
    }

    /// <summary>Provides the generated multi-source CombineLatest arities not covered by the dedicated edge tests.</summary>
    /// <returns>The CombineLatest arities from 4 through 15.</returns>
    public static IEnumerable<int> MultiSourceCombineLatestArities()
    {
        for (var arity = 4; arity < Sixteen; arity++)
        {
            yield return arity;
        }
    }

    /// <summary>Provides the time-based parity cases, driven by a virtual clock.</summary>
    /// <returns>The time-based parity cases.</returns>
    public static IEnumerable<TimeCase> TimeCases()
    {
        yield return new(
            "Delay-Shift",
            (s, c) => s.Shift(TimeSpan.FromTicks(DueTicks), c),
            (s, c) => s.Delay(TimeSpan.FromTicks(DueTicks), c),
            FromOneToThree,
            _oneToThree,
            false);
        yield return new(
            "Timeout-Expire",
            (s, c) => s.Expire(TimeSpan.FromTicks(DueTicks), c),
            (s, c) => s.Timeout(TimeSpan.FromTicks(DueTicks), c),
            Silent,
            _empty,
            true);
    }

    /// <summary>Verifies each unary name produces the expected sequence and is identical to its counterpart.</summary>
    /// <param name = "testCase">The parity case under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(UnaryCases))]
    public async Task UnaryNamesAreBehaviorallyIdentical(UnaryCase testCase)
    {
        var deviant = RunUnary(testCase.Deviant, testCase.Input);
        var rx = RunUnary(testCase.Rx, testCase.Input);
        await Assert.That(deviant.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx).IsEquivalentTo(deviant, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies each higher-order name produces the expected sequence and is identical to its counterpart.</summary>
    /// <param name = "testCase">The parity case under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(HigherOrderCases))]
    public async Task HigherOrderNamesAreBehaviorallyIdentical(HigherOrderCase testCase)
    {
        var deviant = RunHigherOrder(testCase.Deviant, testCase.Inners);
        var rx = RunHigherOrder(testCase.Rx, testCase.Inners);
        await Assert.That(deviant.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx).IsEquivalentTo(deviant, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies each binary name produces the expected sequence and is identical to its counterpart.</summary>
    /// <param name = "testCase">The parity case under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(BinaryCases))]
    public async Task BinaryNamesAreBehaviorallyIdentical(BinaryCase testCase)
    {
        var deviant = RunBinary(testCase.Deviant, testCase.Drive);
        var rx = RunBinary(testCase.Rx, testCase.Drive);
        await Assert.That(deviant.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rx).IsEquivalentTo(deviant, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies each time-based name produces the expected sequence/error and is identical to its counterpart.</summary>
    /// <param name = "testCase">The parity case under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(TimeCases))]
    public async Task TimeNamesAreBehaviorallyIdentical(TimeCase testCase)
    {
        (var deviantValues, var deviantError) = RunTimed(testCase.Deviant, testCase.Source);
        (var rxValues, var rxError) = RunTimed(testCase.Rx, testCase.Source);
        await Assert.That(deviantValues.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rxValues.SequenceEqual(testCase.Expected)).IsTrue();
        await Assert.That(rxValues).IsEquivalentTo(deviantValues, EqualityComparer<int>.Default);
        await Assert.That(deviantError is TimeoutException).IsEqualTo(testCase.ExpectsTimeout);
        await Assert.That(rxError is TimeoutException).IsEqualTo(testCase.ExpectsTimeout);
    }

    /// <summary>Verifies the <c>WhereNotNull</c>/<c>KeepNotNull</c> reference-type pair filters nulls identically.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhereNotNullMatchesKeepNotNull()
    {
        List<string> keep = [];
        List<string> where = [];
        Signal.FromEnumerable(["a", null, "b"]).KeepNotNull().Subscribe(keep.Add);
        Signal.FromEnumerable(["a", null, "b"]).WhereNotNull().Subscribe(where.Add);
        await Assert.That(where).IsEquivalentTo(keep, EqualityComparer<string>.Default);
        await Assert.That(where.Count).IsEqualTo(Two);
    }

    /// <summary>Verifies the binary <c>Concat</c>/<c>Chain</c> overload concatenates two sequences identically.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BinaryConcatMatchesChain()
    {
        List<int> chain = [];
        List<int> concat = [];
        Signal.FromEnumerable(_oneToThree).Chain(Signal.FromEnumerable(_oneToThree)).Subscribe(chain.Add);
        Signal.FromEnumerable(_oneToThree).Concat(Signal.FromEnumerable(_oneToThree)).Subscribe(concat.Add);
        await Assert.That(concat).IsEquivalentTo(chain, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies the three-source CombineLatest overload keeps SyncLatest latest-value semantics.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestThreeSourcesUsesLatestValues()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();
        Signal<int> source3 = new();
        List<int> values = [];
        var completed = 0;
        using var subscription = source.CombineLatest(
            source2,
            source3,
            static (first, second, third) => first + second + third)
            .Subscribe(values.Add, static _ => { }, () => completed++);

        source.OnNext(One);
        source2.OnNext(Ten);
        await Assert.That(values.Count).IsEqualTo(0);

        source3.OnNext(OneHundred);
        source.OnNext(Two);
        source2.OnNext(Twenty);
        source3.OnNext(TwoHundred);

        await Assert.That(values.SequenceEqual([
            One + Ten + OneHundred,
            Two + Ten + OneHundred,
            Two + Twenty + OneHundred,
            Two + Twenty + TwoHundred])).IsTrue();

        source.OnCompleted();
        source2.OnCompleted();
        await Assert.That(completed).IsEqualTo(0);

        source3.OnCompleted();
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies the widest CombineLatest overload preserves source ordering.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestSixteenSourcesPreservesSourceOrdering()
    {
        var sources = Enumerable.Range(0, Sixteen).Select(_ => new Signal<int>()).ToArray();
        List<int> values = [];
        var expectedInitial = Enumerable.Range(One, Sixteen).Sum();
        using var subscription = sources[0].CombineLatest(
            sources[1],
            sources[2],
            sources[3],
            sources[4],
            sources[5],
            sources[6],
            sources[7],
            sources[8],
            sources[9],
            sources[10],
            sources[11],
            sources[12],
            sources[13],
            sources[14],
            sources[15],
            SumSixteen)
            .Subscribe(values.Add);

        for (var i = 0; i < sources.Length; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.SequenceEqual([expectedInitial])).IsTrue();

        sources[0].OnNext(OneHundred);

        await Assert.That(values.SequenceEqual([expectedInitial, expectedInitial - One + OneHundred])).IsTrue();
    }

    /// <summary>Verifies every generated multi-source CombineLatest arity is wired to SyncLatest semantics.</summary>
    /// <param name="arity">The overload arity under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(MultiSourceCombineLatestArities))]
    public async Task CombineLatestMultiSourceAritiesUseLatestValues(int arity)
    {
        var sources = Enumerable.Range(0, arity).Select(_ => new Signal<int>()).ToArray();
        List<int> values = [];
        var expectedInitial = Enumerable.Range(One, arity).Sum();
        using var subscription = CreateCombineLatest(arity, sources).Subscribe(values.Add);

        for (var i = 0; i < sources.Length; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.SequenceEqual([expectedInitial])).IsTrue();

        sources[^1].OnNext(OneHundred);

        await Assert.That(values.SequenceEqual([expectedInitial, expectedInitial - arity + OneHundred])).IsTrue();
    }

    /// <summary>Verifies multi-source CombineLatest forwards source errors and stops.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestMultiSourceForwardsSourceErrors()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();
        Signal<int> source3 = new();
        Signal<int> source4 = new();
        List<int> values = [];
        InvalidOperationException expected = new(Boom);
        Exception? observed = null;
        var completed = 0;
        using var subscription = source.CombineLatest(
            source2,
            source3,
            source4,
            static (first, second, third, fourth) => first + second + third + fourth)
            .Subscribe(values.Add, error => observed = error, () => completed++);

        source.OnNext(One);
        source2.OnError(expected);
        source3.OnNext(Three);
        source4.OnNext(Ten);

        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Verifies every Rx name throws <see cref = "ArgumentNullException"/> for a null source.</summary>
    [Test]
    public void RxNamesThrowOnNullSource()
    {
        var other = Signal.FromEnumerable(_oneToThree);
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Select(Double));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.SelectWith(Ten, AddState));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Where(IsEven));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.WhereWith(Two, IsMultiple));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<string?>)!.WhereNotNull());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Do(Ignore));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.DoWith(Ten, IgnoreState));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Scan(Seed, Add));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Aggregate(Seed, Add));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.DistinctUntilChanged());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.DistinctUntilChangedBy(Identity));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.IgnoreElements());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.SelectMany(Fan));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<IObservable<int>>)!.Merge());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<IObservable<int>>)!.Concat());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Concat(other));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<IObservable<int>>)!.Amb());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<IObservable<int>>)!.Switch());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Zip(other, Add));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.CombineLatest(other, Add));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.WithLatestFrom(other, Add));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Delay(TimeSpan.FromTicks(DueTicks)));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Timeout(TimeSpan.FromTicks(DueTicks)));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Sample(TimeSpan.FromTicks(DueTicks)));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Retry(Two));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Materialize());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<Spark<int>>)!.Dematerialize());
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Resume(other));
        Assert.Throws<ArgumentNullException>(() => other.Resume(null!));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Chain(other));
        Assert.Throws<ArgumentNullException>(() => other.Chain((IObservable<int>)null!));
    }

    /// <summary>Verifies the Rx names throw <see cref = "ArgumentNullException"/> for a null projection/predicate.</summary>
    [Test]
    public void RxNamesThrowOnNullSelector()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        Assert.Throws<ArgumentNullException>(() => source.Select((Func<int, int>)null!));
        Assert.Throws<ArgumentNullException>(() => source.SelectWith<int, int, int>(Ten, null!));
        Assert.Throws<ArgumentNullException>(() => source.Where(null!));
        Assert.Throws<ArgumentNullException>(() => source.WhereWith(Two, null!));
        Assert.Throws<ArgumentNullException>(() => source.Do(null!));
        Assert.Throws<ArgumentNullException>(() => source.DoWith(Ten, null!));
        Assert.Throws<ArgumentNullException>(() => source.Scan(Seed, null!));
        Assert.Throws<ArgumentNullException>(() => source.Aggregate(Seed, null!));
        Assert.Throws<ArgumentNullException>(() => source.DistinctUntilChangedBy<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => source.Zip<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => source.CombineLatest<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => source.WithLatestFrom<int, int, int>(source, null!));
        Assert.Throws<ArgumentNullException>(() => source.Zip((IObservable<int>)null!, Add));
        Assert.Throws<ArgumentNullException>(() => source.CombineLatest((IObservable<int>)null!, Add));
        Assert.Throws<ArgumentNullException>(() => source.WithLatestFrom((IObservable<int>)null!, Add));
        Assert.Throws<ArgumentNullException>(() => source.Concat((IObservable<int>)null!));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(null!, AddPair));
        Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(Fan, null!));
    }

    /// <summary>Verifies the count/interval guards throw <see cref = "ArgumentOutOfRangeException"/>.</summary>
    [Test]
    public void RxNamesThrowOnNegativeArguments()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Retry(NegativeOne));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.Sample(TimeSpan.FromTicks(NegativeOne)));
    }

    /// <summary>Verifies the stateful sinks forward a value and then an error (covers their error path).</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSinksForwardValueThenError()
    {
        await Assert.That(RunStatefulError(s => s.SelectWith(Ten, AddState))).IsTrue();
        await Assert.That(RunStatefulError(s => s.WhereWith(Two, IsMultiple))).IsTrue();
        await Assert.That(RunStatefulError(s => s.DoWith(Ten, IgnoreState))).IsTrue();
    }

    /// <summary>Verifies the stateful projection sinks forward an exception thrown by the projection (covers their catch path).</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulProjectionForwardsThrownError()
    {
        await Assert.That(RunStatefulThrow(s => s.SelectWith(Ten, ThrowProjection))).IsTrue();
        await Assert.That(RunStatefulThrow(s => s.WhereWith(Two, ThrowPredicate))).IsTrue();
    }

    /// <summary>Verifies Resume switches to the fallback sequence after the source errors.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResumeSwitchesToFallbackOnError()
    {
        Signal<int> source = new();
        List<int> values = [];
        var completed = 0;
        using var subscription = source.Resume(Signal.FromEnumerable(_oneToThree))
            .Subscribe(values.Add, static ex => throw ex, () => completed++);
        source.OnNext(Ten);
        source.OnError(new InvalidOperationException(Boom));
        await Assert.That(values.SequenceEqual(_tenThenFallback)).IsTrue();
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies Resume forwards source completion without subscribing the fallback.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResumeForwardsCompletionWithoutFallback()
    {
        Signal<int> source = new();
        List<int> values = [];
        var completed = 0;
        using var subscription = source.Resume(Signal.FromEnumerable(_oneToThree))
            .Subscribe(values.Add, static ex => throw ex, () => completed++);
        source.OnNext(Ten);
        source.OnCompleted();
        await Assert.That(values.SequenceEqual(_tenOnly)).IsTrue();
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies disposing Resume stops forwarding from the source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResumeDisposeStopsForwarding()
    {
        Signal<int> source = new();
        List<int> values = [];
        var subscription = source.Resume(Signal.FromEnumerable(_oneToThree)).Subscribe(values.Add);
        source.OnNext(Ten);
        subscription.Dispose();
        source.OnNext(Twenty);
        await Assert.That(values.SequenceEqual(_tenOnly)).IsTrue();
    }

    /// <summary>Verifies <c>Sample</c> mirrors <c>Probe</c> when sampled against an identical virtual clock drive.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SampleMatchesProbe()
    {
        var sample = RunSampling((s, c) => s.Sample(TimeSpan.FromTicks(Two), c));
        var probe = RunSampling((s, c) => s.Probe(TimeSpan.FromTicks(Two), c));
        await Assert.That(sample).IsEquivalentTo(probe, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies the 3-arg <c>SelectMany</c> mirrors the 3-arg <c>FlatMap</c>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyWithResultSelectorMatchesFlatMap()
    {
        List<int> flatMap = [];
        List<int> selectMany = [];
        Signal.FromEnumerable(_oneToThree).FlatMap(Fan, AddPair).Subscribe(flatMap.Add);
        Signal.FromEnumerable(_oneToThree).SelectMany(Fan, AddPair).Subscribe(selectMany.Add);
        await Assert.That(selectMany).IsEquivalentTo(flatMap, EqualityComparer<int>.Default);
        await Assert.That(selectMany.Count > 0).IsTrue();
    }

    /// <summary>Verifies the int-range fast paths of the binary/higher-order names match their counterparts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RxNamesRangeFastPathsMatchCounterparts()
    {
        var zipped = Collect(Signal.Sequence(One, Three).Zip(Signal.Sequence(Ten, Three), Add));
        var paired = Collect(Signal.Sequence(One, Three).Pair(Signal.Sequence(Ten, Three), Add));
        var combined = Collect(Signal.Sequence(One, Three).CombineLatest(Signal.Sequence(Ten, Three), Add));
        var synced = Collect(Signal.Sequence(One, Three).SyncLatest(Signal.Sequence(Ten, Three), Add));
        var withLatest = Collect(Signal.Sequence(One, Three).WithLatestFrom(Signal.Sequence(Ten, Three), Add));
        var latched = Collect(Signal.Sequence(One, Three).Latch(Signal.Sequence(Ten, Three), Add));
        var switched = Collect(RangeInners().Switch());
        var switchedTo = Collect(RangeInners().SwitchTo());

        await Assert.That(zipped).IsEquivalentTo(paired, EqualityComparer<int>.Default);
        await Assert.That(combined).IsEquivalentTo(synced, EqualityComparer<int>.Default);
        await Assert.That(withLatest).IsEquivalentTo(latched, EqualityComparer<int>.Default);
        await Assert.That(switched).IsEquivalentTo(switchedTo, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies <c>Retry</c> mirrors the source when no error occurs (covers the happy path).</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RetryMirrorsSourceWhenNoError() =>
        await Assert.That(Collect(Signal.FromEnumerable(_oneToThree).Retry(Two)).SequenceEqual(_oneToThree)).IsTrue();

    /// <summary>Exercises the default-sequencer (no-scheduler) overloads of the time operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeOperatorsAcceptDefaultSequencer()
    {
        Signal.Sequence(One, Three).Delay(TimeSpan.FromTicks(DueTicks)).Subscribe(static _ => { }).Dispose();
        Signal.FromEnumerable(_oneToThree).Timeout(TimeSpan.FromSeconds(AdvanceTicks)).Subscribe(static _ => { })
            .Dispose();
        Signal.FromEnumerable(_oneToThree).Sample(TimeSpan.FromTicks(DueTicks)).Subscribe(static _ => { }).Dispose();
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <summary>Verifies the stateful sinks drop notifications that arrive after a terminal notification.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSinksDropNotificationsAfterTerminal()
    {
        await Assert.That(RunStopGuards(s => s.SelectWith(Ten, AddState))).IsTrue();
        await Assert.That(RunStopGuards(s => s.WhereWith(Two, IsMultiple))).IsTrue();
        await Assert.That(RunStopGuards(s => s.DoWith(Ten, IgnoreState))).IsTrue();
    }

    /// <summary>Verifies the stateful sinks reject a null observer.</summary>
    [Test]
    public void StatefulSinksThrowOnNullObserver()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        Assert.Throws<ArgumentNullException>(() => source.SelectWith(Ten, AddState).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => source.WhereWith(Two, IsMultiple).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => source.DoWith(Ten, IgnoreState).Subscribe((IObserver<int>)null!));
    }

    /// <summary>Verifies the stateful sinks propagate the source's current-thread subscription requirement.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSinksReportCurrentThreadRequirement()
    {
        await Assert.That(
            new MapWithSignal<int, int, int>(new CurrentThreadSource<int>(), Ten, AddState)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(
            new KeepWithSignal<int, int>(new CurrentThreadSource<int>(), Two, IsMultiple)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(
            new TapWithSignal<int, int>(new CurrentThreadSource<int>(), Ten, IgnoreState)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(
            !new MapWithSignal<int, int, int>(new ManualSource<int>(), Ten, AddState)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(
            !new KeepWithSignal<int, int>(new ManualSource<int>(), Two, IsMultiple)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(
            !new TapWithSignal<int, int>(new ManualSource<int>(), Ten, IgnoreState)
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Verifies Resume rejects a null observer.</summary>
    [Test]
    public void ResumeThrowsOnNullObserver() =>
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable(_oneToFive)
            .Resume(Signal.FromEnumerable(_oneToThree))
            .Subscribe((IObserver<int>)null!));

    /// <summary>Verifies Resume takes the scheduled subscription path when a current-thread sequencer is already active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResumeSchedulesWhenCurrentThreadSequencerActive()
    {
        List<int> values = [];
        Sequencer.CurrentThread.Schedule(() =>
            new Signal<int>().Resume(Signal.FromEnumerable(_oneToThree)).Subscribe(values.Add));
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(new ResumeSignal<int>(Signal.FromEnumerable(_oneToThree), Signal.FromEnumerable(_oneToThree))
            .IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Doubles a value.</summary>
    /// <param name = "value">The source value.</param>
    /// <returns>The doubled value.</returns>
    private static int Double(int value) => value * Two;

    /// <summary>Determines whether a value is even.</summary>
    /// <param name = "value">The source value.</param>
    /// <returns><see langword="true"/> when the value is even.</returns>
    private static bool IsEven(int value) => value % Two == 0;

    /// <summary>Adds a value to an accumulator.</summary>
    /// <param name = "accumulated">The accumulated value.</param>
    /// <param name = "value">The source value.</param>
    /// <returns>The new accumulated value.</returns>
    private static int Add(int accumulated, int value) => accumulated + value;

    /// <summary>Returns the value unchanged (key selector).</summary>
    /// <param name = "value">The source value.</param>
    /// <returns>The value.</returns>
    private static int Identity(int value) => value;

    /// <summary>Adds the state value to a source value.</summary>
    /// <param name = "state">The state value.</param>
    /// <param name = "value">The source value.</param>
    /// <returns>The sum of the state and the value.</returns>
    private static int AddState(int state, int value) => value + state;

    /// <summary>Determines whether a value is a multiple of the divisor state.</summary>
    /// <param name = "divisor">The divisor state.</param>
    /// <param name = "value">The source value.</param>
    /// <returns><see langword="true"/> when the value is a multiple of the divisor.</returns>
    private static bool IsMultiple(int divisor, int value) => value % divisor == 0;

    /// <summary>Consumes a value without effect (the side-effect under test is irrelevant to the output).</summary>
    /// <param name = "value">The source value.</param>
    private static void Ignore(int value)
    {
        // Intentionally empty: Do/Tap forward values unchanged regardless of the side effect.
    }

    /// <summary>Consumes a state and value without effect.</summary>
    /// <param name = "state">The state value.</param>
    /// <param name = "value">The source value.</param>
    private static void IgnoreState(int state, int value)
    {
        // Intentionally empty: DoWith/TapWith forward values unchanged regardless of the side effect.
    }

    /// <summary>Projects a value to an inner sequence that emits it twice.</summary>
    /// <param name = "value">The source value.</param>
    /// <returns>An inner sequence of two copies of the value.</returns>
    private static IObservable<int> Fan(int value) => Signal.FromEnumerable([value, value]);

    /// <summary>Builds the 1..3 source used by the delay case.</summary>
    /// <returns>A source emitting 1..3.</returns>
    private static IObservable<int> FromOneToThree() => Signal.FromEnumerable(_oneToThree);

    /// <summary>Builds a non-terminating source used by the timeout case.</summary>
    /// <returns>A source that never emits or completes.</returns>
    private static IObservable<int> Silent() => Signal.Silent<int>();

    /// <summary>Pushes index-paired values so Zip/Pair emits 11, 22, 33.</summary>
    /// <param name = "left">The left subject.</param>
    /// <param name = "right">The right subject.</param>
    private static void DriveZip(Signal<int> left, Signal<int> right)
    {
        left.OnNext(One);
        right.OnNext(Ten);
        left.OnNext(Two);
        right.OnNext(Twenty);
        left.OnNext(Three);
        right.OnNext(Thirty);
    }

    /// <summary>Pushes interleaved values so CombineLatest/SyncLatest emits 11, 12, 22.</summary>
    /// <param name = "left">The left subject.</param>
    /// <param name = "right">The right subject.</param>
    private static void DriveCombine(Signal<int> left, Signal<int> right)
    {
        left.OnNext(One);
        right.OnNext(Ten);
        left.OnNext(Two);
        right.OnNext(Twenty);
    }

    /// <summary>Pushes triggers and latest values so WithLatestFrom/Latch emits 11, 12, 23.</summary>
    /// <param name = "left">The triggering subject.</param>
    /// <param name = "right">The latest-value subject.</param>
    private static void DriveLatch(Signal<int> left, Signal<int> right)
    {
        right.OnNext(Ten);
        left.OnNext(One);
        left.OnNext(Two);
        right.OnNext(Twenty);
        left.OnNext(Three);
    }

    /// <summary>Runs a unary operator over a cold source and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "input">The source values.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunUnary(Func<IObservable<int>, IObservable<int>> op, int[] input)
    {
        List<int> values = [];
        op(Signal.FromEnumerable(input)).Subscribe(values.Add);
        return values;
    }

    /// <summary>Runs a higher-order operator over a source of cold inner sources and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "inners">The inner source values.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunHigherOrder(Func<IObservable<IObservable<int>>, IObservable<int>> op, int[][] inners)
    {
        var outer = Signal.FromEnumerable(Array.ConvertAll(inners, ToSource));
        List<int> values = [];
        op(outer).Subscribe(values.Add);
        return values;
    }

    /// <summary>Wraps an inner value array in a cold source.</summary>
    /// <param name = "inner">The inner values.</param>
    /// <returns>A cold source over the inner values.</returns>
    private static IObservable<int> ToSource(int[] inner) => Signal.FromEnumerable(inner);

    /// <summary>Runs a binary operator over two manual subjects driven by a script and collects the forwarded values.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "drive">The script that pushes values into the subjects.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> RunBinary(
        Func<IObservable<int>, IObservable<int>, IObservable<int>> op,
        Action<Signal<int>, Signal<int>> drive)
    {
        Signal<int> left = new();
        Signal<int> right = new();
        List<int> values = [];
        using var subscription = op(left, right).Subscribe(values.Add);
        drive(left, right);
        return values;
    }

    /// <summary>Runs a time-based operator against a virtual clock and collects the forwarded values and any error.</summary>
    /// <param name = "op">The operator under test.</param>
    /// <param name = "source">The source factory.</param>
    /// <returns>The forwarded values and any terminal error.</returns>
    private static (List<int> Values, Exception? Error) RunTimed(
        Func<IObservable<int>, ISequencer, IObservable<int>> op,
        Func<IObservable<int>> source)
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        List<int> values = [];
        Exception? error = null;
        using var subscription = op(source(), clock).Subscribe(values.Add, captured => error = captured, () => { });
        clock.AdvanceBy(TimeSpan.FromTicks(AdvanceTicks));
        return (values, error);
    }

    /// <summary>Pushes one value then an error through a stateful sink and reports whether both were forwarded.</summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when one value and the error were forwarded.</returns>
    private static bool RunStatefulError(Func<IObservable<int>, IObservable<int>> op)
    {
        Signal<int> source = new();
        List<int> values = [];
        Exception? error = null;
        using var subscription = op(source).Subscribe(values.Add, captured => error = captured, () => { });
        source.OnNext(Two);
        source.OnError(new InvalidOperationException(Boom));
        return values.Count == One && error is InvalidOperationException;
    }

    /// <summary>Pushes a value through a sink whose projection throws and reports whether the error was forwarded.</summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when the thrown error was forwarded downstream.</returns>
    private static bool RunStatefulThrow(Func<IObservable<int>, IObservable<int>> op)
    {
        Signal<int> source = new();
        Exception? error = null;
        using var subscription = op(source).Subscribe(
            static _ => { },
            captured => error = captured,
            () => { });
        source.OnNext(One);
        return error is InvalidOperationException;
    }

    /// <summary>A stateful projection that always throws (drives the sink catch path).</summary>
    /// <param name = "state">The unused state.</param>
    /// <param name = "value">The unused value.</param>
    /// <returns>Never returns; always throws.</returns>
    private static int ThrowProjection(int state, int value) => throw new InvalidOperationException(Boom);

    /// <summary>A stateful predicate that always throws (drives the sink catch path).</summary>
    /// <param name = "state">The unused state.</param>
    /// <param name = "value">The unused value.</param>
    /// <returns>Never returns; always throws.</returns>
    private static bool ThrowPredicate(int state, int value) => throw new InvalidOperationException(Boom);

    /// <summary>Runs a sampling operator against a virtual clock with a fixed drive and collects the sampled values.</summary>
    /// <param name = "op">The sampling operator under test.</param>
    /// <returns>The sampled values.</returns>
    private static List<int> RunSampling(Func<IObservable<int>, ISequencer, IObservable<int>> op)
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        using var subscription = op(source, clock).Subscribe(values.Add);
        source.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        source.OnNext(Three);
        clock.AdvanceBy(TimeSpan.FromTicks(Two));
        return values;
    }

    /// <summary>Combines a source value with an inner value (result selector for the 3-arg SelectMany/FlatMap).</summary>
    /// <param name = "source">The source value.</param>
    /// <param name = "inner">The inner value.</param>
    /// <returns>The combined value.</returns>
    private static int AddPair(int source, int inner) => source + inner;

    /// <summary>Creates the requested multi-source CombineLatest overload.</summary>
    /// <param name="arity">The arity to create.</param>
    /// <param name="sources">The source signals.</param>
    /// <returns>The combined observable.</returns>
    [SuppressMessage(
        "Style",
        "S1541:Methods and properties should not be too complex",
        Justification = "Compile-time overload coverage intentionally invokes each generated arity.")]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Compile-time overload coverage intentionally invokes high-arity selector lambdas.")]
    [SuppressMessage(
        "Major Code Smell",
        "S109:Magic numbers should not be used",
        Justification = "Compile-time overload coverage indexes each source slot by arity.")]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Functions should not have too many lines of code",
        Justification = "Compile-time overload coverage keeps the arity switch in one audited helper.")]
    private static IObservable<int> CreateCombineLatest(int arity, Signal<int>[] sources) =>
        arity switch
        {
            4 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                static (value1, value2, value3, value4) =>
                    value1 + value2 + value3 + value4),
            5 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                static (value1, value2, value3, value4, value5) =>
                    value1 + value2 + value3 + value4 + value5),
            6 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                static (value1, value2, value3, value4, value5, value6) =>
                    value1 + value2 + value3 + value4 + value5 + value6),
            7 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                static (value1, value2, value3, value4, value5, value6, value7) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7),
            8 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                static (value1, value2, value3, value4, value5, value6, value7, value8) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8),
            9 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9),
            10 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10),
            11 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                static (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11),
            12 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12),
            13 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13),
            14 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13,
                    value14) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13 + value14),
            15 => sources[0].CombineLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                sources[14],
                static (
                    value1,
                    value2,
                    value3,
                    value4,
                    value5,
                    value6,
                    value7,
                    value8,
                    value9,
                    value10,
                    value11,
                    value12,
                    value13,
                    value14,
                    value15) =>
                    value1 + value2 + value3 + value4 + value5 + value6 + value7 + value8 + value9 + value10 +
                    value11 + value12 + value13 + value14 + value15),
            _ => throw new ArgumentOutOfRangeException(nameof(arity), arity, null)
        };

    /// <summary>Sums sixteen values for the widest CombineLatest overload.</summary>
    /// <param name="value1">Value 1.</param>
    /// <param name="value2">Value 2.</param>
    /// <param name="value3">Value 3.</param>
    /// <param name="value4">Value 4.</param>
    /// <param name="value5">Value 5.</param>
    /// <param name="value6">Value 6.</param>
    /// <param name="value7">Value 7.</param>
    /// <param name="value8">Value 8.</param>
    /// <param name="value9">Value 9.</param>
    /// <param name="value10">Value 10.</param>
    /// <param name="value11">Value 11.</param>
    /// <param name="value12">Value 12.</param>
    /// <param name="value13">Value 13.</param>
    /// <param name="value14">Value 14.</param>
    /// <param name="value15">Value 15.</param>
    /// <param name="value16">Value 16.</param>
    /// <returns>The sum of all values.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "Has more than 7 parameters - required to exercise the arity-16 CombineLatest selector.")]
    private static int SumSixteen(
        int value1,
        int value2,
        int value3,
        int value4,
        int value5,
        int value6,
        int value7,
        int value8,
        int value9,
        int value10,
        int value11,
        int value12,
        int value13,
        int value14,
        int value15,
        int value16) =>
        value1 +
        value2 +
        value3 +
        value4 +
        value5 +
        value6 +
        value7 +
        value8 +
        value9 +
        value10 +
        value11 +
        value12 +
        value13 +
        value14 +
        value15 +
        value16;

    /// <summary>Subscribes to a source and collects its forwarded values.</summary>
    /// <param name = "source">The source sequence.</param>
    /// <returns>The forwarded values.</returns>
    private static List<int> Collect(IObservable<int> source)
    {
        List<int> values = [];
        source.Subscribe(values.Add);
        return values;
    }

    /// <summary>Builds a source of two int-range inner sources (exercises the synchronous Switch range fast path).</summary>
    /// <returns>An outer source of two range inners.</returns>
    private static IObservable<IObservable<int>> RangeInners() =>
        Signal.FromEnumerable([Signal.Sequence(One, Two), Signal.Sequence(Three, Two)]);

    /// <summary>
    /// Drives a stateful sink through a value, a terminal completion, and then further notifications, reporting
    /// whether the post-terminal notifications were dropped (exactly one completion, no leaked error).
    /// </summary>
    /// <param name = "op">The stateful operator under test.</param>
    /// <returns><see langword="true"/> when notifications after the terminal were dropped.</returns>
    private static bool RunStopGuards(Func<IObservable<int>, IObservable<int>> op)
    {
        ManualSource<int> source = new();
        var completed = 0;
        Exception? error = null;
        using var subscription = op(source).Subscribe(
            static _ => { },
            captured => error = captured,
            () => completed++);
        source.Next(Two);
        source.Complete();
        source.Next(Three);
        source.Error(new InvalidOperationException(Boom));
        source.Complete();
        return completed == One && error is null;
    }

    /// <summary>
    /// An observable whose subscription retains its observer and ignores disposal, letting a test push raw
    /// notifications (including ones after a terminal notification) to exercise a sink's terminal guards.
    /// </summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class ManualSource<T> : IObservable<T>
    {
        /// <summary>The observer retained from the most recent subscription.</summary>
        private IObserver<T>? _observer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return EmptyDisposable.Instance;
        }

        /// <summary>Pushes a value to the retained observer.</summary>
        /// <param name = "value">The value to push.</param>
        public void Next(T value) => _observer?.OnNext(value);

        /// <summary>Pushes an error to the retained observer.</summary>
        /// <param name = "exception">The error to push.</param>
        public void Error(Exception exception) => _observer?.OnError(exception);

        /// <summary>Pushes completion to the retained observer.</summary>
        public void Complete() => _observer?.OnCompleted();
    }

    /// <summary>A source that reports it requires current-thread subscription (drives the sink's propagation check).</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class CurrentThreadSource<T> : IRequireCurrentThread<T>
    {
        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => true;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }

    /// <summary>A unary parity case: a Primitives-named builder and its Rx-named twin over one source.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Input">The source values.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    public sealed record UnaryCase(
        string Name,
        Func<IObservable<int>, IObservable<int>> Deviant,
        Func<IObservable<int>, IObservable<int>> Rx,
        int[] Input,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A higher-order parity case operating over a source of inner sources.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Inners">The inner source values.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    [SuppressMessage(
        "Major Code Smell",
        "S2368:Public methods should not have multidimensional array parameters",
        Justification = "The jagged array is the public TUnit method-data shape for higher-order parity cases.")]
    public sealed record HigherOrderCase(
        string Name,
        Func<IObservable<IObservable<int>>, IObservable<int>> Deviant,
        Func<IObservable<IObservable<int>>, IObservable<int>> Rx,
        int[][] Inners,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A binary parity case driven by a scripted interleaving of two manual subjects.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Drive">The script that pushes values into the left and right subjects.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    public sealed record BinaryCase(
        string Name,
        Func<IObservable<int>, IObservable<int>, IObservable<int>> Deviant,
        Func<IObservable<int>, IObservable<int>, IObservable<int>> Rx,
        Action<Signal<int>, Signal<int>> Drive,
        int[] Expected)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }

    /// <summary>A time-based parity case driven by a virtual clock.</summary>
    /// <param name = "Name">The pair name.</param>
    /// <param name = "Deviant">The Primitives-named builder.</param>
    /// <param name = "Rx">The Rx/LINQ-named builder.</param>
    /// <param name = "Source">The source factory.</param>
    /// <param name = "Expected">The expected forwarded values.</param>
    /// <param name = "ExpectsTimeout">Whether a <see cref = "TimeoutException"/> is expected.</param>
    public sealed record TimeCase(
        string Name,
        Func<IObservable<int>, ISequencer, IObservable<int>> Deviant,
        Func<IObservable<int>, ISequencer, IObservable<int>> Rx,
        Func<IObservable<int>> Source,
        int[] Expected,
        bool ExpectsTimeout)
    {
        /// <inheritdoc/>
        public override string ToString() => Name;
    }
}
