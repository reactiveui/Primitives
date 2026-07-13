// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Data-driven parity tests proving each System.Reactive/LINQ name builds a behaviorally identical sink to its
/// Primitives-named counterpart. Each operator pair is one data-source row consumed by a single test body, so the
/// behavior is asserted once and checked for both names (and for identity between them).
/// </summary>
public partial class RxNamesTests
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

    /// <summary>The timeout in seconds used while waiting for ThreadPool-scheduled coverage branches.</summary>
    private const int PollTimeoutSeconds = 2;

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
        yield return new("Select-Map", static s => s.Map(Double), static s => s.Select(Double), _oneToFive, _doubled);
        yield return new("Where-Keep", static s => s.Keep(IsEven), static s => s.Where(IsEven), _oneToFive, _evens);
        yield return new("Scan-Fold", static s => s.Fold(Seed, Add), static s => s.Scan(Seed, Add), _oneToFive, _runningSum);
        yield return new(
            "Aggregate-Reduce",
            static s => s.Reduce(Seed, Add),
            static s => s.Aggregate(Seed, Add),
            _oneToFive,
            _finalSum);
        yield return new(
            "DistinctUntilChanged-Unique",
            static s => s.Unique(),
            static s => s.DistinctUntilChanged(),
            _adjacentDuplicates,
            _deduplicated);
        yield return new(
            "DistinctUntilChangedBy-UniqueBy",
            static s => s.UniqueBy(Identity),
            static s => s.DistinctUntilChangedBy(Identity),
            _adjacentDuplicates,
            _deduplicated);
        yield return new(
            "IgnoreElements-IgnoreValues",
            static s => s.IgnoreValues(),
            static s => s.IgnoreElements(),
            _oneToFive,
            _empty);
        yield return new(
            "SelectWith-MapWith",
            static s => s.MapWith(Ten, AddState),
            static s => s.SelectWith(Ten, AddState),
            _oneToFive,
            _plusTen);
        yield return new(
            "WhereWith-KeepWith",
            static s => s.KeepWith(Two, IsMultiple),
            static s => s.WhereWith(Two, IsMultiple),
            _oneToFive,
            _evens);
        yield return new("Do-Tap", static s => s.Tap(Ignore), static s => s.Do(Ignore), _oneToFive, _oneToFive);
        yield return new(
            "DoWith-TapWith",
            static s => s.TapWith(Ten, IgnoreState),
            static s => s.DoWith(Ten, IgnoreState),
            _oneToFive,
            _oneToFive);
        yield return new("SelectMany-FlatMap", static s => s.FlatMap(Fan), static s => s.SelectMany(Fan), _oneToThree, _fanned);
        yield return new(
            "Materialize-Spark",
            static s => s.Spark().Unspark(),
            static s => s.Materialize().Dematerialize(),
            _oneToFive,
            _oneToFive);
    }

    /// <summary>Provides the higher-order <c>source-of-sources</c> parity cases.</summary>
    /// <returns>The higher-order parity cases.</returns>
    public static IEnumerable<HigherOrderCase> HigherOrderCases()
    {
        yield return new("Merge-Blend", static o => o.Blend(), static o => o.Merge(), _twoInners, _flattened);
        yield return new("Concat-Chain", static o => o.Chain(), static o => o.Concat(), _twoInners, _flattened);
        yield return new("Switch-SwitchTo", static o => o.SwitchTo(), static o => o.Switch(), _twoInners, _flattened);
        yield return new("Amb-Race", static o => o.Race(), static o => o.Amb(), _twoInners, _firstInner);
    }

    /// <summary>Provides the binary <c>(left, right) -&gt; result</c> parity cases.</summary>
    /// <returns>The binary parity cases.</returns>
    public static IEnumerable<BinaryCase> BinaryCases()
    {
        yield return new("Zip-Pair", static (l, r) => l.Pair(r, Add), static (l, r) => l.Zip(r, Add), DriveZip, _zipped);
        yield return new(
            "CombineLatest-SyncLatest",
            static (l, r) => l.SyncLatest(r, Add),
            static (l, r) => l.CombineLatest(r, Add),
            DriveCombine,
            _combined);
        yield return new(
            "WithLatestFrom-Latch",
            static (l, r) => l.Latch(r, Add),
            static (l, r) => l.WithLatestFrom(r, Add),
            DriveLatch,
            _latched);
    }

    /// <summary>Provides the generated multi-source CombineLatest arities not covered by the dedicated edge tests.</summary>
    /// <returns>The CombineLatest arities from 4 through 15.</returns>
    public static IEnumerable<int> MultiSourceCombineLatestArities()
    {
        for (var arity = MinMultiSourceArity; arity < Sixteen; arity++)
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
            static (s, c) => s.Shift(TimeSpan.FromTicks(DueTicks), c),
            static (s, c) => s.Delay(TimeSpan.FromTicks(DueTicks), c),
            FromOneToThree,
            _oneToThree,
            false);
        yield return new(
            "Timeout-Expire",
            static (s, c) => s.Expire(TimeSpan.FromTicks(DueTicks), c),
            static (s, c) => s.Timeout(TimeSpan.FromTicks(DueTicks), c),
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
        var (deviantValues, deviantError) = RunTimed(testCase.Deviant, testCase.Source);
        var (rxValues, rxError) = RunTimed(testCase.Rx, testCase.Source);
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
        _ = Signal.FromEnumerable(["a", null, "b"]).KeepNotNull().Subscribe(keep.Add);
        _ = Signal.FromEnumerable(["a", null, "b"]).WhereNotNull().Subscribe(where.Add);
        await Assert.That(where).IsEquivalentTo(keep, EqualityComparer<string>.Default);
        await Assert.That(where.Count).IsEqualTo(Two);
    }

    /// <summary>Verifies the Rx type-filtering names match the existing KeepType and CastTo operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CastAndOfTypeMatchTypeFilteringAliases()
    {
        List<string> keepType = [];
        List<string> ofType = [];
        _ = Signal.FromEnumerable<object?>(["a", null, Two, "b"]).KeepType<string>().Subscribe(keepType.Add);
        _ = Signal.FromEnumerable<object?>(["a", null, Two, "b"]).OfType<string>().Subscribe(ofType.Add);

        List<string> castValues = [];
        Exception? castError = null;
        _ = Signal.FromEnumerable<object?>(["a", Two]).Cast<string>()
            .Subscribe(castValues.Add, error => castError = error);

        await Assert.That(ofType).IsEquivalentTo(keepType, EqualityComparer<string>.Default);
        await Assert.That(ofType.SequenceEqual(["a", "b"])).IsTrue();
        await Assert.That(castValues.SequenceEqual(["a"])).IsTrue();
        await Assert.That(castError).IsTypeOf<InvalidCastException>();
    }

    /// <summary>Verifies OnErrorResumeNext continues after both normal completion and errors.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextContinuesAfterCompletionAndError()
    {
        List<int> binary = [];
        var binaryCompleted = 0;
        _ = Signal.Emit(Ten)
            .OnErrorResumeNext(Signal.FromEnumerable(_oneToThree))
            .Subscribe(binary.Add, static ex => throw ex, () => binaryCompleted++);

        List<int> staticValues = [];
        var staticCompleted = 0;
        _ = Signal.OnErrorResumeNext(
                Signal.FromEnumerable([One]),
                Signal.Fail<int>(new InvalidOperationException(Boom)),
                Signal.FromEnumerable([Two, Three]))
            .Subscribe(staticValues.Add, static ex => throw ex, () => staticCompleted++);

        await Assert.That(binary.SequenceEqual(_tenThenFallback)).IsTrue();
        await Assert.That(binaryCompleted).IsEqualTo(1);
        await Assert.That(staticValues.SequenceEqual(_oneToThree)).IsTrue();
        await Assert.That(staticCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies absolute-time Delay, DelaySubscription, and Timeout overloads use scheduler time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AbsoluteTimeOperatorsUseSchedulerNow()
    {
        VirtualClock delayClock = new(DateTimeOffset.UnixEpoch);
        List<int> delayed = [];
        _ = Signal.Emit(One).Delay(delayClock.Now.AddTicks(DueTicks), delayClock).Subscribe(delayed.Add);
        await Assert.That(delayed.Count).IsEqualTo(0);
        delayClock.AdvanceBy(TimeSpan.FromTicks(DueTicks));
        await Assert.That(delayed.SequenceEqual([One])).IsTrue();

        VirtualClock subscriptionClock = new(DateTimeOffset.UnixEpoch);
        List<int> delayedSubscription = [];
        _ = Signal.Emit(Two)
            .DelaySubscription(subscriptionClock.Now.AddTicks(DueTicks), subscriptionClock)
            .Subscribe(delayedSubscription.Add);
        await Assert.That(delayedSubscription.Count).IsEqualTo(0);
        subscriptionClock.AdvanceBy(TimeSpan.FromTicks(DueTicks));
        await Assert.That(delayedSubscription.SequenceEqual([Two])).IsTrue();

        VirtualClock timeoutClock = new(DateTimeOffset.UnixEpoch);
        Exception? timeout = null;
        _ = Signal.Silent<int>()
            .Timeout(timeoutClock.Now.AddTicks(DueTicks), timeoutClock)
            .Subscribe(static _ => { }, error => timeout = error);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(DueTicks));
        await Assert.That(timeout).IsTypeOf<TimeoutException>();
    }

    /// <summary>Verifies absolute-time operators resolve scheduler time when subscribed, not when constructed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AbsoluteTimeOperatorsUseSchedulerNowAtSubscription()
    {
        const long RelativeTicks = 5;
        const long ConstructionAdvanceTicks = 3;
        const long RemainingTicks = RelativeTicks - ConstructionAdvanceTicks;

        VirtualClock delayClock = new(DateTimeOffset.UnixEpoch);
        var delayDueTime = delayClock.Now.AddTicks(RelativeTicks);
        var delayedSignal = Signal.Emit(One).Delay(delayDueTime, delayClock);
        delayClock.AdvanceBy(TimeSpan.FromTicks(ConstructionAdvanceTicks));
        List<int> delayed = [];
        _ = delayedSignal.Subscribe(delayed.Add);
        delayClock.AdvanceBy(TimeSpan.FromTicks(RemainingTicks));
        await Assert.That(delayed.SequenceEqual([One])).IsTrue();

        VirtualClock subscriptionClock = new(DateTimeOffset.UnixEpoch);
        var subscriptionDueTime = subscriptionClock.Now.AddTicks(RelativeTicks);
        var delayedSubscriptionSignal = Signal.Emit(Two).DelaySubscription(subscriptionDueTime, subscriptionClock);
        subscriptionClock.AdvanceBy(TimeSpan.FromTicks(ConstructionAdvanceTicks));
        List<int> delayedSubscription = [];
        _ = delayedSubscriptionSignal.Subscribe(delayedSubscription.Add);
        subscriptionClock.AdvanceBy(TimeSpan.FromTicks(RemainingTicks));
        await Assert.That(delayedSubscription.SequenceEqual([Two])).IsTrue();

        VirtualClock timeoutClock = new(DateTimeOffset.UnixEpoch);
        var timeoutDueTime = timeoutClock.Now.AddTicks(RelativeTicks);
        var timeoutSignal = Signal.Silent<int>().Timeout(timeoutDueTime, timeoutClock);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(ConstructionAdvanceTicks));
        Exception? timeout = null;
        _ = timeoutSignal.Subscribe(static _ => { }, error => timeout = error);
        timeoutClock.AdvanceBy(TimeSpan.FromTicks(RemainingTicks));
        await Assert.That(timeout).IsTypeOf<TimeoutException>();

        await Assert.That(((IRequireCurrentThread<int>)Signal.Emit(One).Delay(delayDueTime, Sequencer.CurrentThread))
            .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert
            .That(((IRequireCurrentThread<int>)Signal.Silent<int>().Timeout(timeoutDueTime, Sequencer.CurrentThread))
                .IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert
            .That(((IRequireCurrentThread<int>)Signal.OnErrorResumeNext(Signal.Silent<int>())
                .Timeout(timeoutDueTime, Sequencer.Immediate)).IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert
            .That(((IRequireCurrentThread<int>)new ManualSource<int>().Timeout(timeoutDueTime, Sequencer.Immediate))
                .IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>Verifies absolute-time overloads use the default scheduler when no scheduler is supplied.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AbsoluteTimeOperatorsUseDefaultScheduler()
    {
        var dueTime = ThreadPoolSequencer.Instance.Now.AddSeconds(-PollTimeoutSeconds);
        List<int> delayedScalar = [];
        List<int> delayedRange = [];
        List<int> delayedSubscriptionScalar = [];
        List<int> delayedSubscriptionRange = [];
        List<int> delayedExplicitRange = [];
        List<int> delayedSubscriptionExplicitRange = [];
        Exception? timeout = null;
        Exception? explicitTimeout = null;
        const ISequencer? defaultScheduler = null;

        using var delayScalarSubscription = Signal.Emit(One)
            .Delay(dueTime)
            .Subscribe(delayedScalar.Add);
        using var delayRangeSubscription = Signal.Sequence(Two, Two)
            .Delay(dueTime)
            .Subscribe(delayedRange.Add);
        using var delayExplicitRangeSubscription = Signal.Sequence(Two, Two)
            .Delay(dueTime, defaultScheduler)
            .Subscribe(delayedExplicitRange.Add);
        using var subscriptionScalarSubscription = Signal.Emit(One)
            .DelaySubscription(dueTime)
            .Subscribe(delayedSubscriptionScalar.Add);
        using var subscriptionRangeSubscription = Signal.Sequence(Two, Two)
            .DelaySubscription(dueTime)
            .Subscribe(delayedSubscriptionRange.Add);
        using var subscriptionExplicitRangeSubscription = Signal.Sequence(Two, Two)
            .DelaySubscription(dueTime, defaultScheduler)
            .Subscribe(delayedSubscriptionExplicitRange.Add);
        using var timeoutSubscription = Signal.Silent<int>()
            .Timeout(dueTime)
            .Subscribe(static _ => { }, captured => timeout = captured);
        using var explicitTimeoutSubscription = Signal.Silent<int>()
            .Timeout(dueTime, defaultScheduler)
            .Subscribe(static _ => { }, captured => explicitTimeout = captured);

        await TestPolling.SpinUntil(
            () =>
                delayedScalar.Count == One &&
                delayedRange.Count == Two &&
                delayedExplicitRange.Count == Two &&
                delayedSubscriptionScalar.Count == One &&
                delayedSubscriptionRange.Count == Two &&
                delayedSubscriptionExplicitRange.Count == Two &&
                timeout is not null &&
                explicitTimeout is not null,
            TimeSpan.FromSeconds(PollTimeoutSeconds));

        await Assert.That(delayedScalar.SequenceEqual([One])).IsTrue();
        await Assert.That(delayedRange.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedExplicitRange.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedSubscriptionScalar.SequenceEqual([One])).IsTrue();
        await Assert.That(delayedSubscriptionRange.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(delayedSubscriptionExplicitRange.SequenceEqual([Two, Three])).IsTrue();
        await Assert.That(timeout).IsTypeOf<TimeoutException>();
        await Assert.That(explicitTimeout).IsTypeOf<TimeoutException>();
    }

    /// <summary>Verifies the binary <c>Concat</c>/<c>Chain</c> overload concatenates two sequences identically.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BinaryConcatMatchesChain()
    {
        List<int> chain = [];
        List<int> concat = [];
        _ = Signal.FromEnumerable(_oneToThree).Chain(Signal.FromEnumerable(_oneToThree)).Subscribe(chain.Add);
        _ = Signal.FromEnumerable(_oneToThree).Concat(Signal.FromEnumerable(_oneToThree)).Subscribe(concat.Add);
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
            Two + Twenty + TwoHundred
        ])).IsTrue();

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
        CombineLatestSources signals = new();
        var sources = signals.InOrder;
        List<int> values = [];
        var expectedInitial = Enumerable.Range(One, Sixteen).Sum();
        using var subscription = signals.First.CombineLatest(
                signals.Second,
                signals.Third,
                signals.Fourth,
                signals.Fifth,
                signals.Sixth,
                signals.Seventh,
                signals.Eighth,
                signals.Ninth,
                signals.Tenth,
                signals.Eleventh,
                signals.Twelfth,
                signals.Thirteenth,
                signals.Fourteenth,
                signals.Fifteenth,
                signals.Sixteenth,
                SumSixteen)
            .Subscribe(values.Add);

        for (var i = 0; i < sources.Length; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.SequenceEqual([expectedInitial])).IsTrue();

        signals.First.OnNext(OneHundred);

        await Assert.That(values.SequenceEqual([expectedInitial, expectedInitial - One + OneHundred])).IsTrue();
    }

    /// <summary>Verifies every generated multi-source CombineLatest arity is wired to SyncLatest semantics.</summary>
    /// <param name="arity">The overload arity under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(MultiSourceCombineLatestArities))]
    public async Task CombineLatestMultiSourceAritiesUseLatestValues(int arity)
    {
        CombineLatestSources signals = new();
        var sources = signals.InOrder;
        List<int> values = [];
        var expectedInitial = Enumerable.Range(One, arity).Sum();
        using var subscription = CreateCombineLatest(arity, signals).Subscribe(values.Add);

        for (var i = 0; i < arity; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.SequenceEqual([expectedInitial])).IsTrue();

        sources[arity - One].OnNext(OneHundred);

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
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Select(Double));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.SelectWith(Ten, AddState));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Where(IsEven));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.WhereWith(Two, IsMultiple));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<string?>)!.WhereNotNull());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Do(Ignore));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.DoWith(Ten, IgnoreState));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Scan(Seed, Add));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Aggregate(Seed, Add));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.DistinctUntilChanged());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.DistinctUntilChangedBy(Identity));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.IgnoreElements());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.SelectMany(Fan));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<IObservable<int>>)!.Merge());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<IObservable<int>>)!.Concat());
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Concat(other));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<IObservable<int>>)!.Amb());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<IObservable<int>>)!.Switch());
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Zip(other, Add));
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.CombineLatest(other, Add));
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.WithLatestFrom(other, Add));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Delay(TimeSpan.FromTicks(DueTicks)));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Delay(DateTimeOffset.UnixEpoch));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            default(IObservable<int>)!.Timeout(TimeSpan.FromTicks(DueTicks)));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Timeout(DateTimeOffset.UnixEpoch));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Sample(TimeSpan.FromTicks(DueTicks)));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Retry(Two));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Materialize());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<Spark<int>>)!.Dematerialize());
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Resume(other));
        _ = Assert.Throws<ArgumentNullException>(() => other.Resume(null!));
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.OnErrorResumeNext(other));
        _ = Assert.Throws<ArgumentNullException>(() => other.OnErrorResumeNext(null!));
        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Chain(other));
        _ = Assert.Throws<ArgumentNullException>(() => other.Chain((IObservable<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<object?>)!.OfType<string>());
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<object?>)!.Cast<string>());
        _ = Assert.Throws<ArgumentNullException>(static () =>
            default(IObservable<int>)!.DelaySubscription(DateTimeOffset.UnixEpoch));
    }

    /// <summary>Verifies the Rx names throw <see cref = "ArgumentNullException"/> for a null projection/predicate.</summary>
    [Test]
    public void RxNamesThrowOnNullSelector()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        _ = Assert.Throws<ArgumentNullException>(() => source.Select((Func<int, int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.SelectWith<int, int, int>(Ten, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Where(null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.WhereWith(Two, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Do(null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.DoWith(Ten, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Scan(Seed, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Aggregate(Seed, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.DistinctUntilChangedBy<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.SelectMany((Func<int, IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Zip<int, int, int>(source, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.CombineLatest<int, int, int>(source, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.WithLatestFrom<int, int, int>(source, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.Zip((IObservable<int>)null!, Add));
        _ = Assert.Throws<ArgumentNullException>(() => source.CombineLatest((IObservable<int>)null!, Add));
        _ = Assert.Throws<ArgumentNullException>(() => source.WithLatestFrom((IObservable<int>)null!, Add));
        _ = Assert.Throws<ArgumentNullException>(() => source.Concat((IObservable<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(null!, AddPair));
        _ = Assert.Throws<ArgumentNullException>(() => source.SelectMany<int, int, int>(Fan, null!));
    }

    /// <summary>Verifies the count/interval guards throw <see cref = "ArgumentOutOfRangeException"/>.</summary>
    [Test]
    public void RxNamesThrowOnNegativeArguments()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Retry(NegativeOne));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => source.Sample(TimeSpan.FromTicks(NegativeOne)));
    }

    /// <summary>Verifies the stateful sinks forward a value and then an error (covers their error path).</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSinksForwardValueThenError()
    {
        await Assert.That(RunStatefulError(static s => s.SelectWith(Ten, AddState))).IsTrue();
        await Assert.That(RunStatefulError(static s => s.WhereWith(Two, IsMultiple))).IsTrue();
        await Assert.That(RunStatefulError(static s => s.DoWith(Ten, IgnoreState))).IsTrue();
    }

    /// <summary>Verifies the stateful projection sinks forward an exception thrown by the projection (covers their catch path).</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulProjectionForwardsThrownError()
    {
        await Assert.That(RunStatefulThrow(static s => s.SelectWith(Ten, ThrowProjection))).IsTrue();
        await Assert.That(RunStatefulThrow(static s => s.WhereWith(Two, ThrowPredicate))).IsTrue();
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
        var sample = RunSampling(static (s, c) => s.Sample(TimeSpan.FromTicks(Two), c));
        var probe = RunSampling(static (s, c) => s.Probe(TimeSpan.FromTicks(Two), c));
        await Assert.That(sample).IsEquivalentTo(probe, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies the 3-arg <c>SelectMany</c> mirrors the 3-arg <c>FlatMap</c>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SelectManyWithResultSelectorMatchesFlatMap()
    {
        List<int> flatMap = [];
        List<int> selectMany = [];
        _ = Signal.FromEnumerable(_oneToThree).FlatMap(Fan, AddPair).Subscribe(flatMap.Add);
        _ = Signal.FromEnumerable(_oneToThree).SelectMany(Fan, AddPair).Subscribe(selectMany.Add);
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
        await Assert.That(RunStopGuards(static s => s.SelectWith(Ten, AddState))).IsTrue();
        await Assert.That(RunStopGuards(static s => s.WhereWith(Two, IsMultiple))).IsTrue();
        await Assert.That(RunStopGuards(static s => s.DoWith(Ten, IgnoreState))).IsTrue();
    }

    /// <summary>Verifies the stateful sinks reject a null observer.</summary>
    [Test]
    public void StatefulSinksThrowOnNullObserver()
    {
        var source = Signal.FromEnumerable(_oneToFive);
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.SelectWith(Ten, AddState).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.WhereWith(Two, IsMultiple).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            source.DoWith(Ten, IgnoreState).Subscribe((IObserver<int>)null!));
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
        Assert.Throws<ArgumentNullException>(static () => Signal.FromEnumerable(_oneToFive)
            .Resume(Signal.FromEnumerable(_oneToThree))
            .Subscribe((IObserver<int>)null!));

    /// <summary>Verifies Resume takes the scheduled subscription path when a current-thread sequencer is already active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ResumeSchedulesWhenCurrentThreadSequencerActive()
    {
        List<int> values = [];
        _ = Sequencer.CurrentThread.Schedule(() =>
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
    /// <param name = "_">The source value, which the side effect deliberately ignores.</param>
    private static void Ignore(int _)
    {
        // Intentionally empty: Do/Tap forward values unchanged regardless of the side effect.
    }

    /// <summary>Consumes a state and value without effect.</summary>
    /// <param name = "state">The state value.</param>
    /// <param name = "value">The source value.</param>
    [SuppressMessage("Maintainability", "SST1461:Remove unread private parameters", Justification = "The signature is fixed by the delegate this method is passed to as a method group.")]
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
}
