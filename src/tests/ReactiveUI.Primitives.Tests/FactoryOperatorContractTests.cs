// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

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

        Signal.Range(SecondValue, RetrySuccessAttempt)
            .Concat(Signal.Repeat(RepeatValue, SecondValue))
            .Concat(Signal.Unfold(FirstValue, state => state <= RetrySuccessAttempt, state => state + FirstValue, state => state * ProjectionMultiplier))
            .Concat(Signal.Use(
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
            .DistinctUntilChanged()
            .Tap(_ => taps++)
            .Scan(0, (sum, value) => sum + value)
            .Take(RetrySuccessAttempt)
            .Sparkify()
            .Subscribe(sparks.Add);

        Signal.FromEnumerable(sparks).Unspark().Subscribe(values.Add);
        Signal.FromEnumerable(FourItemExpected).Fold(0, (sum, value) => sum + value).Subscribe(terminal.Add);

        Assert.Equal(UnaryExpected, values);
        Assert.Equal(new[] { ProjectedFirstValue }, terminal);
        Assert.Equal(RetrySuccessAttempt, taps);
        Assert.Equal(SparkKind.OnCompleted, sparks[^1].Kind);
    }

    /// <summary>
    /// Verifies cold select and where operators detach from their source when disposed.
    /// </summary>
    [Test]
    public void SelectAndWhereStayColdUntilSubscribedAndDetachOnDispose()
    {
        var source = new Signal<int>();
        var selected = source.Select(static value => value + 1);
        var filtered = source.Where(static value => value > 1);

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

        Signal.Merge(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(merged.Add);
        Signal.Concat(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(concatenated.Add);
        Signal.Zip(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable([ProjectedFirstValue, ProjectedThirdValue]), (left, right) => left + right).Subscribe(zipped.Add);
        Signal.CombineLatest(Signal.FromEnumerable(TakeWhileExpected), Signal.FromEnumerable(["a", "b"]), (left, right) => left + right).Subscribe(latest.Add);

        Assert.Equal(FourItemExpected, merged);
        Assert.Equal(FourItemExpected, concatenated);
        Assert.Equal(ZippedExpected, zipped);
        Assert.Equal(LatestExpected, latest);
    }

    /// <summary>
    /// Verifies the range-specialized zip path preserves shorter-source completion semantics.
    /// </summary>
    [Test]
    public void RangeZipCompletesAtShorterRange()
    {
        var values = new List<int>();
        var completed = 0;

        Signal.Zip(Signal.Range(FirstValue, FourthValue), Signal.Range(ProjectionMultiplier, SecondValue), static (left, right) => left + right)
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

        Signal.Defer(() =>
            {
                attempts++;
                return attempts < RetrySuccessAttempt
                    ? Signal.Throw<int>(new InvalidOperationException("try again"))
                    : Signal.Return(RetryResult);
            })
            .Retry(RetrySuccessAttempt)
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
    /// Verifies timer factories use an injected virtual sequencer.
    /// </summary>
    [Test]
    public void TimeFactoriesUseInjectedScheduler()
    {
        var clock = new TestClock();
        var after = new List<long>();
        var every = new List<long>();

        Signal.After(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(after.Add);
        var subscription = Signal.Every(TimeSpan.FromTicks(EveryTicks), clock).Subscribe(every.Add);

        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        Assert.Equal(0, after.Count);
        Assert.Equal(OneShotTimerExpected, every);

        clock.AdvanceBy(TimeSpan.FromTicks(FirstValue));
        Assert.Equal(OneShotTimerExpected, after);

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
            .StartWith(0, FirstValue)
            .Do(sideEffects.Add)
            .AsObservable()
            .Subscribe(values.Add);

        Signal.Throw<int>(new InvalidOperationException("recover"))
            .Catch(_ => Signal.Return(RetryResult))
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

        source.Throttle(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(throttled.Add);
        source.Sample(TimeSpan.FromTicks(InitialAdvanceTicks), clock).Subscribe(sampled.Add);
        source.TimeInterval(clock).Subscribe(intervals.Add);

        source.OnNext(FirstValue);
        clock.AdvanceBy(TimeSpan.FromTicks(SecondValue));
        source.OnNext(SecondValue);
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        source.OnNext(RetrySuccessAttempt);
        clock.AdvanceBy(TimeSpan.FromTicks(AfterTicks));
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));

        Signal.FromEnumerable(TakeWhileExpected).ZipLatest(Signal.FromEnumerable(["a", "b"]), (left, right) => left + right).Subscribe(latest.Add);
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
        var none = await Signal.Empty<int>().FirstOrDefaultAsync(RetryResult);

        Assert.Equal(RetrySuccessAttempt, first);
        Assert.Equal(CollectedExpected, (IEnumerable<int>)collected);
        Assert.Equal(RetryResult, none);
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
        Signal.Empty<int>().DefaultIfEmpty(RetryResult).Subscribe(defaulted.Add);

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
        Signal.Empty<int>().IsEmpty().Subscribe(isEmpty.Add);

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

        Signal.FromEnumerable(TakeWhileExpected).Bind(value => Signal.Range(value * ProjectionMultiplier, SecondValue)).Subscribe(selected.Add);

        Assert.Equal(SelectedProjectionExpected, selected);
    }

    /// <summary>
    /// Verifies task-based alias operators.
    /// </summary>
    /// <returns>A task that completes when assertions have run.</returns>
    private static async Task VerifyTaskAliasOperators()
    {
        var converted = new[] { 4, AfterTicks }.ToObservable();
        var last = await converted.ToTask();
        var first = await Signal.FromEnumerable([RepeatValue, ProjectionMultiplier]).FirstAsync().ToTask();
        var started = await Signal.Start(() => ProjectedSecondValue, Sequencer.CurrentThread).ToTask();

        Assert.Equal(AfterTicks, last);
        Assert.Equal(RepeatValue, first);
        Assert.Equal(ProjectedSecondValue, started);
    }
}
