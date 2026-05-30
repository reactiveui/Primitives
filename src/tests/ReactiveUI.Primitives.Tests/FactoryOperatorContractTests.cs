// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies factory and operator contract behavior for the primitives surface.
/// </summary>
public class FactoryOperatorContractTests
{
    /// <summary>
    /// The first integer used by parity sequences.
    /// </summary>
    private const int FirstValue = 1;

    /// <summary>
    /// The second integer used by parity sequences.
    /// </summary>
    private const int SecondValue = 2;

    /// <summary>
    /// The third integer used by parity sequences.
    /// </summary>
    private const int RetrySuccessAttempt = 3;

    /// <summary>
    /// The fourth integer used by parity sequences.
    /// </summary>
    private const int FourthValue = 4;

    /// <summary>
    /// A representative even value used by predicate tests.
    /// </summary>
    private const int SixthValue = 6;

    /// <summary>
    /// A resource-scoped sequence value.
    /// </summary>
    private const int ResourceFirstValue = 7;

    /// <summary>
    /// A resource-scoped sequence value.
    /// </summary>
    private const int ResourceSecondValue = 8;

    /// <summary>
    /// The repeated value used by finite factory tests.
    /// </summary>
    private const int RepeatValue = 9;

    /// <summary>
    /// The multiplier used by unfold and projection tests.
    /// </summary>
    private const int ProjectionMultiplier = 10;

    /// <summary>
    /// The first projected value after applying the projection multiplier.
    /// </summary>
    private const int ProjectedFirstValue = 10;

    /// <summary>
    /// The second projected value after applying the projection multiplier.
    /// </summary>
    private const int ProjectedSecondValue = 11;

    /// <summary>
    /// The third projected value after applying the projection multiplier.
    /// </summary>
    private const int ProjectedThirdValue = 20;

    /// <summary>
    /// The fourth projected value after applying the projection multiplier.
    /// </summary>
    private const int ProjectedFourthValue = 21;

    /// <summary>
    /// A peer value used to verify distinct-by bucketing.
    /// </summary>
    private const int ProjectedSecondBucketPeerValue = 12;

    /// <summary>
    /// The zip result expected from the first pair.
    /// </summary>
    private const int FirstZipResult = 11;

    /// <summary>
    /// The zip or fork-join result expected from the second pair.
    /// </summary>
    private const int SecondZipResult = 22;

    /// <summary>
    /// The second result expected from the shorter range zip test.
    /// </summary>
    private const int RangeZipShorterSecondResult = 13;

    /// <summary>
    /// The third unfolded value.
    /// </summary>
    private const int ThirdUnfoldedValue = 30;

    /// <summary>
    /// The terminal value used by default and recovery tests.
    /// </summary>
    private const int RetryResult = 42;

    /// <summary>
    /// Delay used by the async enumerable cancellation test.
    /// </summary>
    private const int AsyncEnumeratorDelayMilliseconds = 5000;

    /// <summary>
    /// Settle delay used by the async enumerable cancellation test.
    /// </summary>
    private const int AsyncEnumeratorSettleMilliseconds = 50;

    /// <summary>
    /// Virtual clock due time for one-shot timers.
    /// </summary>
    private const int AfterTicks = 5;

    /// <summary>
    /// Virtual clock period for recurring timers.
    /// </summary>
    private const int EveryTicks = 3;

    /// <summary>
    /// Virtual clock advance used before a boundary tick.
    /// </summary>
    private const int InitialAdvanceTicks = 4;

    /// <summary>
    /// Virtual clock advance used after disposing recurring work.
    /// </summary>
    private const int FinalAdvanceTicks = 10;

    /// <summary>
    /// Index of the third interval captured in the interval test.
    /// </summary>
    private const int ThirdIntervalIndex = 2;

    /// <summary>
    /// Expected values for finite factory composition.
    /// </summary>
    private static readonly int[] FiniteFactoryExpected =
    [
        SecondValue,
        RetrySuccessAttempt,
        FourthValue,
        RepeatValue,
        RepeatValue,
        ProjectedFirstValue,
        ProjectedThirdValue,
        ThirdUnfoldedValue,
        ResourceFirstValue,
        ResourceSecondValue,
    ];

    /// <summary>
    /// Expected values from the unary materialization test.
    /// </summary>
    private static readonly int[] UnaryExpected = [FourthValue, ProjectedFirstValue, 18];

    /// <summary>
    /// Expected source values from a four-item sequence.
    /// </summary>
    private static readonly int[] FourItemExpected = [FirstValue, SecondValue, RetrySuccessAttempt, FourthValue];

    /// <summary>
    /// Expected selected values after source disposal.
    /// </summary>
    private static readonly int[] SelectedAfterDisposeExpected = [SecondValue, RetrySuccessAttempt];

    /// <summary>
    /// Expected values from a single-filter pass.
    /// </summary>
    private static readonly int[] SingleSecondValueExpected = [SecondValue];

    /// <summary>
    /// Expected values from the zip test.
    /// </summary>
    private static readonly int[] ZippedExpected = [FirstZipResult, SecondZipResult];

    /// <summary>
    /// Expected values from the shorter range zip test.
    /// </summary>
    private static readonly int[] RangeZipShorterExpected = [FirstZipResult, RangeZipShorterSecondResult];

    /// <summary>
    /// Expected values from combine-latest style operators.
    /// </summary>
    private static readonly string[] LatestExpected = ["2a", "2b"];

    /// <summary>
    /// Expected values from virtual recurring timers.
    /// </summary>
    private static readonly long[] EveryExpected = [0L, 1L, 2L];

    /// <summary>
    /// Expected values from lead, append, and prepend.
    /// </summary>
    private static readonly int[] LeadAppendExpected = [0, FirstValue, SecondValue, RetrySuccessAttempt, FourthValue];

    /// <summary>
    /// Expected values from the System.Reactive named alias migration test.
    /// </summary>
    private static readonly int[] SystemReactiveNamedAliasExpected = [0, FirstValue, SecondValue, RetrySuccessAttempt];

    /// <summary>
    /// Expected values after distinct-by bucketing.
    /// </summary>
    private static readonly int[] DistinctByExpected = [ProjectedSecondValue, ProjectedFourthValue];

    /// <summary>
    /// Expected values from a take-while sequence.
    /// </summary>
    private static readonly int[] TakeWhileExpected = [FirstValue, SecondValue];

    /// <summary>
    /// Expected values from a skip-while sequence.
    /// </summary>
    private static readonly int[] SkipWhileExpected = [RetrySuccessAttempt, FirstValue];

    /// <summary>
    /// Expected values from bind selection.
    /// </summary>
    private static readonly int[] SelectedProjectionExpected =
    [
        ProjectedFirstValue,
        ProjectedSecondValue,
        ProjectedThirdValue,
        ProjectedFourthValue,
    ];

    /// <summary>
    /// Expected true result for boolean terminal operators.
    /// </summary>
    private static readonly bool[] TrueExpected = [true];

    /// <summary>
    /// Expected one-shot timer result before repeated timer advancement.
    /// </summary>
    private static readonly long[] OneShotTimerExpected = [0L];

    /// <summary>
    /// Expected retry recovery value.
    /// </summary>
    private static readonly int[] RetryResultExpected = [RetryResult];

    /// <summary>
    /// Expected async enumerable value before disposal.
    /// </summary>
    private static readonly int[] AsyncEnumerableBeforeDisposeExpected = [FirstValue];

    /// <summary>
    /// Expected observed value after virtual clock processing.
    /// </summary>
    private static readonly int[] ObservedResourceExpected = [ResourceFirstValue];

    /// <summary>
    /// Expected throttle output after the quiet period.
    /// </summary>
    private static readonly int[] ThrottleExpected = [RetrySuccessAttempt];

    /// <summary>
    /// Expected sample output over the virtual clock ticks.
    /// </summary>
    private static readonly int[] SampleExpected = [SecondValue, RetrySuccessAttempt];

    /// <summary>
    /// Expected fork-join output.
    /// </summary>
    private static readonly int[] ForkJoinExpected = [SecondZipResult];

    /// <summary>
    /// Expected collected task output.
    /// </summary>
    private static readonly int[] CollectedExpected = [FirstValue, SecondValue, RetrySuccessAttempt];

    /// <summary>
    /// Verifies finite factory composition and resource disposal.
    /// </summary>
    [Test]
    public void FactoriesEmitExpectedFiniteSequencesAndDisposeResources()
    {
        var values = new List<int>();
        var completed = 0;
        var disposed = 0;

        Signal.Sequence(SecondValue, RetrySuccessAttempt)
            .Chain(Signal.Loop(RepeatValue, SecondValue))
            .Chain(Signal.Unfold(FirstValue, state => state <= RetrySuccessAttempt, state => state + FirstValue, state => state * ProjectionMultiplier))
            .Chain(Signal.Use(
                () => Disposable.Create(() => disposed++),
                _ => Signal.FromEnumerable([ResourceFirstValue, ResourceSecondValue])))
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        Assert.Equal(FiniteFactoryExpected, values);
        Assert.Equal(1, completed);
        Assert.Equal(1, disposed);
    }

    /// <summary>
    /// Verifies unary transformation, filtering, aggregation, and materialization operators.
    /// </summary>
    [Test]
    public void UnaryOperatorsTransformFilterAggregateAndMaterialize()
    {
        var sparks = new List<Spark<int>>();
        var values = new List<int>();
        var terminal = new List<int>();
        var taps = 0;

        Signal.FromEnumerable([FirstValue, SecondValue, SecondValue, RetrySuccessAttempt, FourthValue])
            .Map(value => value * SecondValue)
            .Keep(value => value >= FourthValue)
            .Unique()
            .Tap(_ => taps++)
            .Fold(0, (sum, value) => sum + value)
            .Take(RetrySuccessAttempt)
            .Spark()
            .Subscribe(sparks.Add);

        Signal.FromEnumerable(sparks).Unspark().Subscribe(values.Add);
        Signal.FromEnumerable(FourItemExpected).Reduce(0, (sum, value) => sum + value).Subscribe(terminal.Add);

        Assert.Equal(UnaryExpected, values);
        Assert.Equal(new[] { ProjectedFirstValue }, terminal);
        Assert.Equal(RetrySuccessAttempt, taps);
        Assert.Equal(SparkKind.OnCompleted, sparks[^1].Kind);
    }

    /// <summary>
    /// Verifies cold map and keep operators detach from their source when disposed.
    /// </summary>
    [Test]
    public void MapAndKeepStayColdUntilSubscribedAndDetachOnDispose()
    {
        var source = new Signal<int>();
        var selected = source.Map(static value => value + 1);
        var filtered = source.Keep(static value => value > 1);

        Assert.False(source.HasObservers);

        var selectedValues = new List<int>();
        var filteredValues = new List<int>();
        var selectedSubscription = selected.Subscribe(selectedValues.Add);
        var filteredSubscription = filtered.Subscribe(filteredValues.Add);

        Assert.True(source.HasObservers);
        source.OnNext(FirstValue);
        source.OnNext(SecondValue);
        selectedSubscription.Dispose();
        filteredSubscription.Dispose();

        Assert.False(source.HasObservers);
        source.OnNext(RetrySuccessAttempt);

        Assert.Equal(SelectedAfterDisposeExpected, selectedValues);
        Assert.Equal(SingleSecondValueExpected, filteredValues);
    }

    /// <summary>
    /// Verifies merge, concat, zip, and combine-latest ordering semantics.
    /// </summary>
    [Test]
    public void CombiningOperatorsPreserveCoreOrderingSemantics()
    {
        var merged = new List<int>();
        var concatenated = new List<int>();
        var zipped = new List<int>();
        var latest = new List<string>();
        var rangeConcatenated = new List<int>();
        var rangeMerged = new List<int>();
        var rangeRace = new List<int>();
        var rangeAmb = new List<int>();
        var rangeLatest = new List<int>();
        var rangeWithLatest = new List<int>();
        var rangeForkJoin = new List<int>();
        var rangeObserver = new RecordingObserver<int>();

        var rangeConcatSignal = Signal.Chain(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue));
        Signal.Blend(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(merged.Add);
        Signal.Chain(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(concatenated.Add);
        Signal.Pair(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([ProjectedFirstValue, ProjectedThirdValue]), (left, right) => left + right).Subscribe(zipped.Add);
        Signal.SyncLatest(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable(["a", "b"]), (left, right) => left + right).Subscribe(latest.Add);
        rangeConcatSignal.Subscribe(rangeConcatenated.Add);
        rangeConcatSignal.Subscribe(rangeObserver);
        Signal.Blend(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue)).Subscribe(rangeMerged.Add);
        Signal.Race(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue)).Subscribe(rangeRace.Add);
        Signal.Race(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue)).Subscribe(rangeAmb.Add);
        Signal.SyncLatest(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(ProjectionMultiplier, SecondValue), static (left, right) => left + right).Subscribe(rangeLatest.Add);
        Signal.Sequence(FirstValue, SecondValue).Latch(Signal.Sequence(ProjectionMultiplier, SecondValue), static (left, right) => left + right).Subscribe(rangeWithLatest.Add);
        Signal.ForkJoin(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(ProjectionMultiplier, SecondValue), static (left, right) => left + right).Subscribe(rangeForkJoin.Add);
        Assert.Throws<ArgumentNullException>(() => rangeConcatSignal.Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)rangeConcatSignal).Subscribe(null!, _ => { }, () => { }));
        Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)rangeConcatSignal).Subscribe(_ => { }, _ => { }, null!));

        Assert.Equal(FourItemExpected, merged);
        Assert.Equal(FourItemExpected, concatenated);
        Assert.Equal(ZippedExpected, zipped);
        Assert.Equal(LatestExpected, latest);
        Assert.Equal(FourItemExpected, rangeConcatenated);
        Assert.Equal(FourItemExpected, rangeObserver.Values);
        Assert.Equal(1, rangeObserver.Completed);
        Assert.Equal(FourItemExpected, rangeMerged);
        Assert.Equal(TakeWhileExpected, rangeRace);
        Assert.Equal(TakeWhileExpected, rangeAmb);
        Assert.Equal(new[] { ProjectedSecondBucketPeerValue, RangeZipShorterSecondResult }, rangeLatest);
        Assert.Equal(new[] { ProjectedSecondBucketPeerValue, RangeZipShorterSecondResult }, rangeWithLatest);
        Assert.Equal(new[] { RangeZipShorterSecondResult }, rangeForkJoin);
    }

    /// <summary>
    /// Verifies the range-specialized zip path preserves shorter-source completion semantics.
    /// </summary>
    [Test]
    public void RangeZipCompletesAtShorterRange()
    {
        var values = new List<int>();
        var completed = 0;

        Signal.Pair(Signal.Sequence(FirstValue, FourthValue), Signal.Sequence(ProjectionMultiplier, SecondValue), static (left, right) => left + right)
            .Subscribe(values.Add, _ => { }, () => completed++);

        Assert.Equal(RangeZipShorterExpected, values);
        Assert.Equal(1, completed);
    }

    /// <summary>
    /// Verifies retry resubscribes until a deferred source succeeds.
    /// </summary>
    [Test]
    public void RetryResubscribesUntilSuccess()
    {
        var attempts = 0;
        var values = new List<int>();

        Signal.Lazy(() =>
            {
                attempts++;
                return attempts < RetrySuccessAttempt
                    ? Signal.Fail<int>(new InvalidOperationException("try again"))
                    : Signal.Emit(RetryResult);
            })
            .Reattempt(RetrySuccessAttempt)
            .Subscribe(values.Add);

        Assert.Equal(RetrySuccessAttempt, attempts);
        Assert.Equal(RetryResultExpected, values);
    }

    /// <summary>
    /// Verifies async enumerable subscriptions cancel and dispose the enumerator.
    /// </summary>
    /// <returns>A task that completes when the asynchronous assertions have run.</returns>
    [Test]
    public async Task AsyncEnumerableFactoryCancelsEnumeratorOnDispose()
    {
        var disposed = false;
        var values = new List<int>();

        async IAsyncEnumerable<int> Values([EnumeratorCancellation] CancellationToken token = default)
        {
            try
            {
                yield return FirstValue;
                await Task.Delay(AsyncEnumeratorDelayMilliseconds, token);
                yield return SecondValue;
            }
            finally
            {
                disposed = true;
            }
        }

        var subscription = Signal.FromAsyncEnumerable(Values()).Subscribe(values.Add, _ => { }, () => { });
        await Task.Delay(AsyncEnumeratorSettleMilliseconds);
        subscription.Dispose();
        await Task.Delay(AsyncEnumeratorSettleMilliseconds);

        Assert.Equal(AsyncEnumerableBeforeDisposeExpected, values);
        Assert.True(disposed);
    }

    /// <summary>
    /// Verifies completed async enumerable subscriptions can be disposed without racing a disposed token source.
    /// </summary>
    /// <returns>A task that completes when asynchronous assertions have run.</returns>
    [Test]
    public async Task AsyncEnumerableFactoryCanDisposeAfterCompletion()
    {
        static async IAsyncEnumerable<int> Values()
        {
            yield return FirstValue;
            await Task.Yield();
            yield return SecondValue;
        }

        var values = await Signal.FromAsyncEnumerable(Values()).CollectArrayAsync();

        Assert.Equal(TakeWhileExpected, (IEnumerable<int>)values);
    }

    /// <summary>
    /// Verifies timer factories use an injected virtual sequencer.
    /// </summary>
    [Test]
    public void TimeFactoriesUseInjectedScheduler()
    {
        var clock = new TestClock();
        var after = new List<long>();
        var absoluteTimer = new List<long>();
        var every = new List<long>();

        Signal.After(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(after.Add);
        Signal.After(clock.Now.AddTicks(AfterTicks), clock).Subscribe(absoluteTimer.Add);
        var subscription = Signal.Every(TimeSpan.FromTicks(EveryTicks), clock).Subscribe(every.Add);

        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        Assert.Equal(0, after.Count);
        Assert.Equal(OneShotTimerExpected, every);

        clock.AdvanceBy(TimeSpan.FromTicks(FirstValue));
        Assert.Equal(OneShotTimerExpected, after);
        Assert.Equal(OneShotTimerExpected, absoluteTimer);

        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        subscription.Dispose();
        clock.AdvanceBy(TimeSpan.FromTicks(FinalAdvanceTicks));
        Assert.Equal(EveryExpected, every);
    }

    /// <summary>
    /// Verifies additional factory and unary operator parity helpers.
    /// </summary>
    [Test]
    public void AdditionalFactoriesAndUnaryOperatorsCoverCommonParitySurface()
    {
        VerifySequenceBoundaryOperators();
        VerifyBooleanTerminalOperators();
        VerifySelectionAndProjectionOperators();
    }

    /// <summary>
    /// Verifies System.Reactive-style aliases intended to ease migration.
    /// </summary>
    /// <returns>A task that completes when the asynchronous assertions have run.</returns>
    [Test]
    public async Task SystemReactiveNamedAliasesCoverMigrationConvenienceSurface()
    {
        var values = new List<int>();
        var sideEffects = new List<int>();
        var recovered = new List<int>();
        var observed = new List<int>();
        var clock = new TestClock();
        var source = new Signal<int>();

        Signal.FromEnumerable([SecondValue, RetrySuccessAttempt])
            .Prepend(0, FirstValue)
            .Tap(sideEffects.Add)
            .AsObservable()
            .Subscribe(values.Add);

        Signal.Fail<int>(new InvalidOperationException("recover"))
            .Recover(_ => Signal.Emit(RetryResult))
            .Subscribe(recovered.Add);

        source.ObserveOn(clock).Subscribe(observed.Add);
        source.OnNext(ResourceFirstValue);

        Assert.Equal(SystemReactiveNamedAliasExpected, values);
        Assert.Equal((IEnumerable<int>)values, sideEffects);
        Assert.Equal(RetryResultExpected, recovered);
        Assert.Equal(0, observed.Count);

        clock.Start();

        Assert.Equal(ObservedResourceExpected, observed);

        await VerifyTaskAliasOperators();
    }

    /// <summary>
    /// Verifies boundary and latest-value operators with virtual time.
    /// </summary>
    [Test]
    public void BoundaryAndLatestOperatorsUseVirtualTimeAndCompletionSemantics()
    {
        var clock = new TestClock();
        var source = new Signal<int>();
        var throttled = new List<int>();
        var sampled = new List<int>();
        var intervals = new List<TimeInterval<int>>();
        var latest = new List<string>();
        var forkJoined = new List<int>();

        source.Calm(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(throttled.Add);
        source.Probe(TimeSpan.FromTicks(InitialAdvanceTicks), clock).Subscribe(sampled.Add);
        source.TimeInterval(clock).Subscribe(intervals.Add);

        source.OnNext(FirstValue);
        clock.AdvanceBy(TimeSpan.FromTicks(SecondValue));
        source.OnNext(SecondValue);
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        source.OnNext(RetrySuccessAttempt);
        clock.AdvanceBy(TimeSpan.FromTicks(AfterTicks));
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));

        Signal.FromEnumerable(TakeWhileExpected).PairLatest(Signal.FromEnumerable(["a", "b"]), (left, right) => left + right).Subscribe(latest.Add);
        Signal.ForkJoin(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([ProjectedFirstValue, ProjectedThirdValue]), (left, right) => left + right).Subscribe(forkJoined.Add);

        Assert.Equal(ThrottleExpected, throttled);
        Assert.Equal(SampleExpected, sampled);
        Assert.Equal(TimeSpan.Zero, intervals[0].Interval);
        Assert.Equal(TimeSpan.FromTicks(SecondValue), intervals[1].Interval);
        Assert.Equal(TimeSpan.FromTicks(InitialAdvanceTicks), intervals[ThirdIntervalIndex].Interval);
        Assert.Equal(LatestExpected, latest);
        Assert.Equal(ForkJoinExpected, forkJoined);
    }

    /// <summary>
    /// Verifies terminal task operators complete with their expected values.
    /// </summary>
    /// <returns>A task that completes when the asynchronous assertions have run.</returns>
    [Test]
    public async Task TerminalTaskOperatorsCompleteWithExpectedSemantics()
    {
        var first = await Signal.FromEnumerable([RetrySuccessAttempt, FourthValue]).FirstAsync();
        var collected = await Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).CollectArrayAsync();
        var none = await Signal.None<int>().FirstOrDefaultAsync(RetryResult);
        var rangeFirst = await Signal.Sequence(FirstValue, FourthValue).FirstAsync();
        var rangeLast = await Signal.Sequence(FirstValue, FourthValue).ToTask();
        var rangeCollected = await Signal.Sequence(FirstValue, RetrySuccessAttempt).CollectListAsync();
        var count = await Signal.Sequence(FirstValue, FourthValue).CountAsync();
        var countEven = await Signal.Sequence(FirstValue, FourthValue).CountAsync(static value => value % 2 == 0);
        var any = await Signal.Sequence(FirstValue, FourthValue).AnyAsync(static value => value == FourthValue);

        Assert.Equal(RetrySuccessAttempt, first);
        Assert.Equal(CollectedExpected, (IEnumerable<int>)collected);
        Assert.Equal(RetryResult, none);
        Assert.Equal(FirstValue, rangeFirst);
        Assert.Equal(FourthValue, rangeLast);
        Assert.Equal(CollectedExpected, (IEnumerable<int>)rangeCollected);
        Assert.Equal(FourthValue, count);
        Assert.Equal(SecondValue, countEven);
        Assert.True(any);
    }

    /// <summary>
    /// Verifies factory guards, async aliases, and cancellation-aware enumerable conversion.
    /// </summary>
    /// <returns>A task that completes when asynchronous assertions finish.</returns>
    [Test]
    public async Task FactoryAliasesAndGuardsCoverParityBranches()
    {
        var values = new List<int>();
        var errors = new List<Exception>();
        var completed = 0;
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Sequence(FirstValue, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.Sequence(FirstValue, SecondValue, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Loop(FirstValue, -1));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold(0, null!, static state => state, static state => state));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold(0, static _ => true, null!, static state => state));
        Assert.Throws<ArgumentNullException>(() => Signal.Unfold<int, int>(0, static _ => true, static state => state, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern(_ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventArgs>(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEventPattern<EventArgs>(_ => { }, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start((Func<int>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start(static () => FirstValue, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Start((Action)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.After(DateTimeOffset.UnixEpoch, null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => Signal.Every(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentNullException>(() => Signal.After(TimeSpan.Zero, TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).SubscribeOn(Sequencer.Immediate));
        Assert.Throws<ArgumentNullException>(() => Signal.None<int>().SubscribeOn(null!));

        Signal.Sequence(FirstValue, 0).Subscribe(values.Add, errors.Add, () => completed++);
        Signal.Loop(FirstValue, 0).Subscribe(values.Add, errors.Add, () => completed++);
        Signal.Iterate(FirstValue, value => value <= SecondValue, value => value + 1, value => value).Subscribe(values.Add);
        Signal.Sequence(FirstValue, SecondValue).SubscribeOn(Sequencer.Immediate).Subscribe(values.Add);
        new[] { FirstValue, SecondValue }.ToObservable(cancelled.Token).Subscribe(values.Add, errors.Add, () => completed++);
        Signal.Start<int>(() => throw new InvalidOperationException("start failed"), Sequencer.Immediate).Subscribe(values.Add, errors.Add, () => completed++);

        var eventSource = new EventSource();
        var eventValues = new List<EventPattern<EventArgs>>();
        using (Signal.FromEventPattern(handler => eventSource.Raised += handler, handler => eventSource.Raised -= handler).Subscribe(eventValues.Add))
        {
            eventSource.Raise();
        }

        var fromAsync = await Signal.FromAsync(() => Task.FromResult(RetryResult)).ToTask();
        var fromAsyncWithToken = await Signal.FromAsync(static token => Task.FromResult(token.IsCancellationRequested ? -1 : RetrySuccessAttempt)).ToTask();

        Assert.Equal(RetryResult, fromAsync);
        Assert.Equal(RetrySuccessAttempt, fromAsyncWithToken);
        Assert.Equal(new[] { FirstValue, SecondValue, FirstValue, SecondValue }, values);
        Assert.Equal(SecondValue, completed);
        Assert.Equal(1, errors.Count);
        Assert.Equal(1, eventValues.Count);
        Assert.Same(eventSource, eventValues[0].Sender!);
        Assert.Same(EventArgs.Empty, eventValues[0].EventArgs);
    }

    /// <summary>
    /// Verifies sequence boundary operators.
    /// </summary>
    private static void VerifySequenceBoundaryOperators()
    {
        var leadAppend = new List<int>();
        var ignored = new List<int>();
        var distinctBy = new List<int>();
        var takeWhile = new List<int>();
        var skipWhile = new List<int>();
        var defaulted = new List<int>();

        Signal.FromEnumerable([SecondValue, RetrySuccessAttempt]).Lead(FirstValue).Append(FourthValue).Prepend(0).Subscribe(leadAppend.Add);
        Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).IgnoreValues().Subscribe(ignored.Add);
        Signal.FromEnumerable([ProjectedSecondValue, ProjectedSecondBucketPeerValue, ProjectedFourthValue, SecondZipResult])
            .DistinctBy(value => value / ProjectionMultiplier)
            .Subscribe(distinctBy.Add);
        Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt, FirstValue]).TakeWhile(value => value < RetrySuccessAttempt).Subscribe(takeWhile.Add);
        Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt, FirstValue]).SkipWhile(value => value < RetrySuccessAttempt).Subscribe(skipWhile.Add);
        Signal.None<int>().DefaultIfEmpty(RetryResult).Subscribe(defaulted.Add);

        Assert.Equal(LeadAppendExpected, leadAppend);
        Assert.Equal(0, ignored.Count);
        Assert.Equal(DistinctByExpected, distinctBy);
        Assert.Equal(TakeWhileExpected, takeWhile);
        Assert.Equal(SkipWhileExpected, skipWhile);
        Assert.Equal(RetryResultExpected, defaulted);
    }

    /// <summary>
    /// Verifies boolean terminal operators.
    /// </summary>
    private static void VerifyBooleanTerminalOperators()
    {
        var count = new List<int>();
        var any = new List<bool>();
        var all = new List<bool>();
        var contains = new List<bool>();
        var isEmpty = new List<bool>();

        Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).Count().Subscribe(count.Add);
        Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).Any(value => value == SecondValue).Subscribe(any.Add);
        Signal.FromEnumerable([SecondValue, FourthValue, SixthValue]).All(value => value % SecondValue == 0).Subscribe(all.Add);
        Signal.FromEnumerable([SecondValue, FourthValue, SixthValue]).Contains(FourthValue).Subscribe(contains.Add);
        Signal.None<int>().IsEmpty().Subscribe(isEmpty.Add);

        Assert.Equal(new[] { RetrySuccessAttempt }, count);
        Assert.Equal(TrueExpected, any);
        Assert.Equal(TrueExpected, all);
        Assert.Equal(TrueExpected, contains);
        Assert.Equal(TrueExpected, isEmpty);
    }

    /// <summary>
    /// Verifies selection and projection operators.
    /// </summary>
    private static void VerifySelectionAndProjectionOperators()
    {
        var selected = new List<int>();

        Signal.FromEnumerable(TakeWhileExpected).Bind(value => Signal.Sequence(value * ProjectionMultiplier, SecondValue)).Subscribe(selected.Add);

        Assert.Equal(SelectedProjectionExpected, selected);
    }

    /// <summary>
    /// Verifies task-based alias operators.
    /// </summary>
    /// <returns>A task that completes when assertions have run.</returns>
    [SuppressMessage("Major Code Smell", "S6966:Awaitable method should be used", Justification = "Synchronous ToArray/ToList operators are deliberately covered alongside async variants.")]
    private static async Task VerifyTaskAliasOperators()
    {
        var converted = new[] { 4, AfterTicks }.ToObservable();
        var last = await converted.ToTask();
        var lastAlias = await converted.LastAsync();
        var lastDefault = await Signal.None<int>().LastOrDefaultAsync(RetryResult);
        var array = await Signal.Sequence(FirstValue, FourthValue).ToArrayAsync();
        var list = await Signal.Sequence(FirstValue, FourthValue).ToListAsync();
        int[] observedArray = [];
        Signal.Sequence(FirstValue, SecondValue).ToArray().Subscribe(value => observedArray = value).Dispose();
        List<int> observedList = [];
        Signal.Sequence(FirstValue, SecondValue).ToList().Subscribe(value => observedList = [.. value]).Dispose();
        var first = await Signal.FromEnumerable([RepeatValue, ProjectionMultiplier]).FirstAsync().ToTask();
        var started = await Signal.Start(() => ProjectedSecondValue, Sequencer.CurrentThread).ToTask();

        Assert.Equal(AfterTicks, last);
        Assert.Equal(AfterTicks, lastAlias);
        Assert.Equal(RetryResult, lastDefault);
        Assert.Equal(FourItemExpected, (IEnumerable<int>)array);
        Assert.Equal(FourItemExpected, (IEnumerable<int>)list);
        Assert.Equal((IEnumerable<int>)[FirstValue, SecondValue], observedArray);
        Assert.Equal([FirstValue, SecondValue], (IEnumerable<int>)observedList);
        Assert.Equal(RepeatValue, first);
        Assert.Equal(ProjectedSecondValue, started);
    }

    /// <summary>
    /// Test event source.
    /// </summary>
    private sealed class EventSource
    {
        /// <summary>
        /// Raised when <see cref="Raise"/> is called.
        /// </summary>
        public event EventHandler? Raised;

        /// <summary>
        /// Raises the event.
        /// </summary>
        public void Raise() => Raised?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>
    /// Records observer values and terminal signals.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>
        /// Gets observed values.
        /// </summary>
        public List<T> Values { get; } = [];

        /// <summary>
        /// Gets completion count.
        /// </summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => throw error;

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
