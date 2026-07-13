// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies factory and operator composition behavior across the <see cref="Signal"/> surface.</summary>
public partial class SignalFactoriesTests
{
    /// <summary>The first integer used by parity sequences.</summary>
    private const int FirstValue = 1;

    /// <summary>The second integer used by parity sequences.</summary>
    private const int SecondValue = 2;

    /// <summary>The third integer used by parity sequences.</summary>
    private const int RetrySuccessAttempt = 3;

    /// <summary>The fourth integer used by parity sequences.</summary>
    private const int FourthValue = 4;

    /// <summary>A representative even value used by predicate tests.</summary>
    private const int SixthValue = 6;

    /// <summary>A resource-scoped sequence value.</summary>
    private const int ResourceFirstValue = 7;

    /// <summary>A resource-scoped sequence value.</summary>
    private const int ResourceSecondValue = 8;

    /// <summary>The repeated value used by finite factory tests.</summary>
    private const int RepeatValue = 9;

    /// <summary>The multiplier used by unfold and projection tests.</summary>
    private const int ProjectionMultiplier = 10;

    /// <summary>The first projected value after applying the projection multiplier.</summary>
    private const int ProjectedFirstValue = 10;

    /// <summary>The second projected value after applying the projection multiplier.</summary>
    private const int ProjectedSecondValue = 11;

    /// <summary>The third projected value after applying the projection multiplier.</summary>
    private const int ProjectedThirdValue = 20;

    /// <summary>The fourth projected value after applying the projection multiplier.</summary>
    private const int ProjectedFourthValue = 21;

    /// <summary>A peer value used to verify distinct-by bucketing.</summary>
    private const int ProjectedSecondBucketPeerValue = 12;

    /// <summary>The zip result expected from the first pair.</summary>
    private const int FirstZipResult = 11;

    /// <summary>The zip or fork-join result expected from the second pair.</summary>
    private const int SecondZipResult = 22;

    /// <summary>The second result expected from the shorter range zip test.</summary>
    private const int RangeZipShorterSecondResult = 13;

    /// <summary>The third unfolded value.</summary>
    private const int ThirdUnfoldedValue = 30;

    /// <summary>The terminal value used by default and recovery tests.</summary>
    private const int RetryResult = 42;

    /// <summary>The divisor that selects the even values of a sequence.</summary>
    private const int EvenDivisor = 2;

    /// <summary>Delay used by the async enumerable cancellation test.</summary>
    private const int AsyncEnumeratorDelayMilliseconds = 5000;

    /// <summary>Timeout used while waiting for async enumerable disposal.</summary>
    private const int AsyncEnumeratorDisposeTimeoutSeconds = 5;

    /// <summary>Virtual clock due time for one-shot timers.</summary>
    private const int AfterTicks = 5;

    /// <summary>Virtual clock period for recurring timers.</summary>
    private const int EveryTicks = 3;

    /// <summary>Virtual clock advance used before a boundary tick.</summary>
    private const int InitialAdvanceTicks = 4;

    /// <summary>Virtual clock advance used after disposing recurring work.</summary>
    private const int FinalAdvanceTicks = 10;

    /// <summary>Index of the third interval captured in the interval test.</summary>
    private const int ThirdIntervalIndex = 2;

    /// <summary>Expected values for finite factory composition.</summary>
    private static readonly int[] FiniteFactoryExpected =
    [
        SecondValue, RetrySuccessAttempt, FourthValue, RepeatValue, RepeatValue,
        ProjectedFirstValue, ProjectedThirdValue, ThirdUnfoldedValue, ResourceFirstValue, ResourceSecondValue
    ];

    /// <summary>Expected values from the unary materialization test.</summary>
    private static readonly int[] UnaryExpected = [FourthValue, ProjectedFirstValue, 18];

    /// <summary>Expected source values from a four-item sequence.</summary>
    private static readonly int[] FourItemExpected = [FirstValue, SecondValue, RetrySuccessAttempt, FourthValue];

    /// <summary>Expected selected values after source disposal.</summary>
    private static readonly int[] SelectedAfterDisposeExpected = [SecondValue, RetrySuccessAttempt];

    /// <summary>Expected values from a single-filter pass.</summary>
    private static readonly int[] SingleSecondValueExpected = [SecondValue];

    /// <summary>Expected values from the zip test.</summary>
    private static readonly int[] ZippedExpected = [FirstZipResult, SecondZipResult];

    /// <summary>Expected values from the shorter range zip test.</summary>
    private static readonly int[] RangeZipShorterExpected = [FirstZipResult, RangeZipShorterSecondResult];

    /// <summary>Expected values from combine-latest style operators.</summary>
    private static readonly string[] LatestExpected = ["2a", "2b"];

    /// <summary>Expected values from virtual recurring timers.</summary>
    private static readonly long[] EveryExpected = [0L, 1L, 2L];

    /// <summary>Expected values from lead, append, and prepend.</summary>
    private static readonly int[] LeadAppendExpected = [0, FirstValue, SecondValue, RetrySuccessAttempt, FourthValue];

    /// <summary>Expected values from the System.Reactive named alias migration test.</summary>
    private static readonly int[] SystemReactiveNamedAliasExpected = [0, FirstValue, SecondValue, RetrySuccessAttempt];

    /// <summary>Expected values after distinct-by bucketing.</summary>
    private static readonly int[] DistinctByExpected = [ProjectedSecondValue, ProjectedFourthValue];

    /// <summary>Expected values from a take-while sequence.</summary>
    private static readonly int[] TakeWhileExpected = [FirstValue, SecondValue];

    /// <summary>Expected values from a skip-while sequence.</summary>
    private static readonly int[] SkipWhileExpected = [RetrySuccessAttempt, FirstValue];

    /// <summary>Expected values from bind selection.</summary>
    private static readonly int[] SelectedProjectionExpected =
        [ProjectedFirstValue, ProjectedSecondValue, ProjectedThirdValue, ProjectedFourthValue];

    /// <summary>Expected true result for boolean terminal operators.</summary>
    private static readonly bool[] TrueExpected = [true];

    /// <summary>Expected one-shot timer result before repeated timer advancement.</summary>
    private static readonly long[] OneShotTimerExpected = [0L];

    /// <summary>Expected retry recovery value.</summary>
    private static readonly int[] RetryResultExpected = [RetryResult];

    /// <summary>Expected async enumerable value before disposal.</summary>
    private static readonly int[] AsyncEnumerableBeforeDisposeExpected = [FirstValue];

    /// <summary>Expected observed value after virtual clock processing.</summary>
    private static readonly int[] ObservedResourceExpected = [ResourceFirstValue];

    /// <summary>Expected throttle output after the quiet period.</summary>
    private static readonly int[] ThrottleExpected = [RetrySuccessAttempt];

    /// <summary>Expected sample output over the virtual clock ticks.</summary>
    private static readonly int[] SampleExpected = [SecondValue, RetrySuccessAttempt];

    /// <summary>Expected fork-join output.</summary>
    private static readonly int[] ForkJoinExpected = [SecondZipResult];

    /// <summary>Expected collected task output.</summary>
    private static readonly int[] CollectedExpected = [FirstValue, SecondValue, RetrySuccessAttempt];

    /// <summary>Verifies finite factory composition and resource disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FactoriesEmitExpectedFiniteSequencesAndDisposeResources()
    {
        List<int> values = [];
        var completed = 0;
        var disposed = 0;
        _ = Signal.Sequence(
                SecondValue,
                RetrySuccessAttempt)
            .Chain(
                Signal.Loop(RepeatValue, SecondValue))
            .Chain(Signal.Unfold(
                FirstValue,
                static state => state <= RetrySuccessAttempt,
                static state => state + FirstValue,
                static state => state * ProjectionMultiplier))
            .Chain(Signal.Use(
                () => new ActionDisposable(() => disposed++),
                static _ => Signal.FromEnumerable([ResourceFirstValue, ResourceSecondValue])))
            .Subscribe(values.Add, static ex => throw ex, () => completed++);
        await Assert.That(values.SequenceEqual(FiniteFactoryExpected)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies unary transformation, filtering, aggregation, and materialization operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnaryOperatorsTransformFilterAggregateAndMaterialize()
    {
        List<Spark<int>> sparks = [];
        List<int> values = [];
        List<int> terminal = [];
        var taps = 0;
        _ = Signal.FromEnumerable([FirstValue, SecondValue, SecondValue, RetrySuccessAttempt, FourthValue])
            .Map(static value => value * SecondValue).Keep(static value => value >= FourthValue).Unique().Tap(_ => taps++)
            .Fold(0, static (sum, value) => sum + value).Take(RetrySuccessAttempt).Spark().Subscribe(sparks.Add);
        _ = Signal.FromEnumerable(sparks).Unspark().Subscribe(values.Add);
        _ = Signal.FromEnumerable(FourItemExpected).Reduce(0, static (sum, value) => sum + value).Subscribe(terminal.Add);
        await Assert.That(values.SequenceEqual(UnaryExpected)).IsTrue();
        int[] expectedTerminal = [ProjectedFirstValue];
        await Assert.That(terminal.SequenceEqual(expectedTerminal)).IsTrue();
        await Assert.That(taps).IsEqualTo(RetrySuccessAttempt);
        await Assert.That(sparks[^1].Kind).IsEqualTo(SparkKind.OnCompleted);
    }

    /// <summary>Verifies cold map and keep operators detach from their source when disposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MapAndKeepStayColdUntilSubscribedAndDetachOnDispose()
    {
        Signal<int> source = new();
        var selected = source.Map(static value => value + 1);
        var filtered = source.Keep(static value => value > 1);
        await Assert.That(source.HasObservers).IsFalse();
        List<int> selectedValues = [];
        List<int> filteredValues = [];
        var selectedSubscription = selected.Subscribe(selectedValues.Add);
        var filteredSubscription = filtered.Subscribe(filteredValues.Add);
        await Assert.That(source.HasObservers).IsTrue();
        source.OnNext(FirstValue);
        source.OnNext(SecondValue);
        selectedSubscription.Dispose();
        filteredSubscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        source.OnNext(RetrySuccessAttempt);
        await Assert.That(selectedValues.SequenceEqual(SelectedAfterDisposeExpected)).IsTrue();
        await Assert.That(filteredValues.SequenceEqual(SingleSecondValueExpected)).IsTrue();
    }

    /// <summary>Verifies merge, concat, zip, and combine-latest ordering semantics.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombiningOperatorsPreserveCoreOrderingSemantics()
    {
        await AssertEnumerableSourceCombinatorsPreserveOrdering();
        await AssertRangeSourceCombinatorsPreserveOrdering();
    }

    /// <summary>Verifies the range-specialized zip path preserves shorter-source completion semantics.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RangeZipCompletesAtShorterRange()
    {
        List<int> values = [];
        var completed = 0;
        _ = Signal.Pair(
            Signal.Sequence(FirstValue, FourthValue),
            Signal.Sequence(ProjectionMultiplier, SecondValue),
            static (left, right) => left + right).Subscribe(
            values.Add,
            static _ => { },
            () => completed++);
        await Assert.That(values.SequenceEqual(RangeZipShorterExpected)).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
    }

    /// <summary>Verifies retry resubscribes until a deferred source succeeds.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RetryResubscribesUntilSuccess()
    {
        var attempts = 0;
        List<int> values = [];
        _ = Signal.Lazy(() =>
        {
            attempts++;
            return attempts < RetrySuccessAttempt
                ? Signal.Fail<int>(new InvalidOperationException("try again"))
                : Signal.Emit(RetryResult);
        }).Reattempt(RetrySuccessAttempt).Subscribe(values.Add);
        await Assert.That(attempts).IsEqualTo(RetrySuccessAttempt);
        await Assert.That(values.SequenceEqual(RetryResultExpected)).IsTrue();
    }

    /// <summary>Verifies async enumerable subscriptions cancel and dispose the enumerator.</summary>
    /// <returns>A task that completes when the asynchronous assertions have run.</returns>
    [Test]
    public async Task AsyncEnumerableFactoryCancelsEnumeratorOnDispose()
    {
        var disposed = false;
        List<int> values = [];
        TaskCompletionSource disposedSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

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
                _ = disposedSignal.TrySetResult();
            }
        }

        var subscription = Signal.FromAsyncEnumerable(Values()).Subscribe(
            values.Add,
            static _ => { },
            static () => { });
        await Task.Yield();
        subscription.Dispose();
        await disposedSignal.Task.WaitAsync(TimeSpan.FromSeconds(AsyncEnumeratorDisposeTimeoutSeconds))
            .ConfigureAwait(false);
        await Assert.That(values.SequenceEqual(AsyncEnumerableBeforeDisposeExpected)).IsTrue();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies completed async enumerable subscriptions can be disposed without racing a disposed token source.</summary>
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
        await Assert.That(values.SequenceEqual(TakeWhileExpected)).IsTrue();
    }

    /// <summary>Verifies timer factories use an injected virtual sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeFactoriesUseInjectedScheduler()
    {
        VirtualClock clock = new();
        List<long> after = [];
        List<long> absoluteTimer = [];
        List<long> every = [];
        _ = Signal.After(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(after.Add);
        _ = Signal.After(clock.Now.AddTicks(AfterTicks), clock).Subscribe(absoluteTimer.Add);
        var subscription = Signal.Every(TimeSpan.FromTicks(EveryTicks), clock).Subscribe(every.Add);
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        await Assert.That(after.Count).IsEqualTo(0);
        await Assert.That(every.SequenceEqual(OneShotTimerExpected)).IsTrue();
        clock.AdvanceBy(TimeSpan.FromTicks(FirstValue));
        await Assert.That(after.SequenceEqual(OneShotTimerExpected)).IsTrue();
        await Assert.That(absoluteTimer.SequenceEqual(OneShotTimerExpected)).IsTrue();
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        subscription.Dispose();
        clock.AdvanceBy(TimeSpan.FromTicks(FinalAdvanceTicks));
        await Assert.That(every.SequenceEqual(EveryExpected)).IsTrue();
    }

    /// <summary>Verifies additional factory and unary operator parity helpers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AdditionalFactoriesAndUnaryOperatorsCoverCommonParitySurface()
    {
        await VerifySequenceBoundaryOperators();
        await VerifyBooleanTerminalOperators();
        await VerifySelectionAndProjectionOperators();
    }

    /// <summary>Verifies System.Reactive-style aliases intended to ease migration.</summary>
    /// <returns>A task that completes when the asynchronous assertions have run.</returns>
    [Test]
    public async Task SystemReactiveNamedAliasesCoverMigrationConvenienceSurface()
    {
        List<int> values = [];
        List<int> sideEffects = [];
        List<int> recovered = [];
        List<int> observed = [];
        VirtualClock clock = new();
        Signal<int> source = new();
        _ = Signal.FromEnumerable([SecondValue, RetrySuccessAttempt]).Prepend(0, FirstValue).Tap(sideEffects.Add)
            .AsObservable().Subscribe(values.Add);
        _ = Signal.Fail<int>(new InvalidOperationException("recover")).Recover(static _ => Signal.Emit(RetryResult))
            .Subscribe(recovered.Add);
        _ = source.ObserveOn(clock).Subscribe(observed.Add);
        source.OnNext(ResourceFirstValue);
        await Assert.That(values.SequenceEqual(SystemReactiveNamedAliasExpected)).IsTrue();
        await Assert.That(sideEffects.SequenceEqual(values)).IsTrue();
        await Assert.That(recovered.SequenceEqual(RetryResultExpected)).IsTrue();
        await Assert.That(observed.Count).IsEqualTo(0);
        clock.Start();
        await Assert.That(observed.SequenceEqual(ObservedResourceExpected)).IsTrue();
        await VerifyTaskAliasOperators();
    }

    /// <summary>Verifies boundary and latest-value operators with virtual time.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BoundaryAndLatestOperatorsUseVirtualTimeAndCompletionSemantics()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        List<int> throttled = [];
        List<int> sampled = [];
        List<TimeInterval<int>> intervals = [];
        List<string> latest = [];
        List<int> forkJoined = [];
        _ = source.Calm(TimeSpan.FromTicks(AfterTicks), clock).Subscribe(throttled.Add);
        _ = source.Probe(TimeSpan.FromTicks(InitialAdvanceTicks), clock).Subscribe(sampled.Add);
        _ = source.TimeInterval(clock).Subscribe(intervals.Add);
        source.OnNext(FirstValue);
        clock.AdvanceBy(TimeSpan.FromTicks(SecondValue));
        source.OnNext(SecondValue);
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        source.OnNext(RetrySuccessAttempt);
        clock.AdvanceBy(TimeSpan.FromTicks(AfterTicks));
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(InitialAdvanceTicks));
        _ = Signal.FromEnumerable(TakeWhileExpected)
            .PairLatest(Signal.FromEnumerable(["a", "b"]), static (left, right) => left + right).Subscribe(latest.Add);
        _ = Signal.ForkJoin(
            Signal.FromEnumerable(TakeWhileExpected),
            Signal.FromEnumerable([ProjectedFirstValue, ProjectedThirdValue]),
            static (left, right) => left + right).Subscribe(forkJoined.Add);
        await Assert.That(throttled.SequenceEqual(ThrottleExpected)).IsTrue();
        await Assert.That(sampled.SequenceEqual(SampleExpected)).IsTrue();
        await Assert.That(intervals[0].Interval).IsEqualTo(TimeSpan.Zero);
        await Assert.That(intervals[1].Interval).IsEqualTo(TimeSpan.FromTicks(SecondValue));
        await Assert.That(intervals[ThirdIntervalIndex].Interval).IsEqualTo(TimeSpan.FromTicks(InitialAdvanceTicks));
        await Assert.That(latest.SequenceEqual(LatestExpected)).IsTrue();
        await Assert.That(forkJoined.SequenceEqual(ForkJoinExpected)).IsTrue();
    }

    /// <summary>Verifies terminal task operators complete with their expected values.</summary>
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
        var countEven = await Signal.Sequence(FirstValue, FourthValue)
            .CountAsync(static value => value % EvenDivisor == 0);
        var any = await Signal.Sequence(FirstValue, FourthValue).AnyAsync(static value => value == FourthValue);
        await Assert.That(first).IsEqualTo(RetrySuccessAttempt);
        await Assert.That(collected.SequenceEqual(CollectedExpected)).IsTrue();
        await Assert.That(none).IsEqualTo(RetryResult);
        await Assert.That(rangeFirst).IsEqualTo(FirstValue);
        await Assert.That(rangeLast).IsEqualTo(FourthValue);
        await Assert.That(rangeCollected.SequenceEqual(CollectedExpected)).IsTrue();
        await Assert.That(count).IsEqualTo(FourthValue);
        await Assert.That(countEven).IsEqualTo(SecondValue);
        await Assert.That(any).IsTrue();
    }

    /// <summary>Verifies factory guards, async aliases, and cancellation-aware enumerable conversion.</summary>
    /// <returns>A task that completes when asynchronous assertions finish.</returns>
    [Test]
    public async Task FactoryAliasesAndGuardsCoverParityBranches()
    {
        List<int> values = [];
        List<Exception> errors = [];
        var completed = 0;
        using CancellationTokenSource cancelled = new();
        await cancelled.CancelAsync();
        AssertFactoryGuardsRejectInvalidArguments();
        _ = Signal.Sequence(FirstValue, 0).Subscribe(values.Add, errors.Add, () => completed++);
        _ = Signal.Loop(FirstValue, 0).Subscribe(values.Add, errors.Add, () => completed++);
        _ = Signal.Iterate(FirstValue, static value => value <= SecondValue, static value => value + 1, static value => value)
            .Subscribe(values.Add);
        _ = Signal.Sequence(FirstValue, SecondValue).SubscribeOn(Sequencer.Immediate).Subscribe(values.Add);
        _ = new[] { FirstValue, SecondValue }.ToObservable(cancelled.Token)
            .Subscribe(values.Add, errors.Add, () => completed++);
        _ = Signal.Start<int>(static () => throw new InvalidOperationException("start failed"), Sequencer.Immediate)
            .Subscribe(values.Add, errors.Add, () => completed++);
        EventSource eventSource = new();
        List<EventPattern<EventArgs>> eventValues = [];
        using (Signal.FromEventPattern(
                   handler => eventSource.Raised += handler,
                   handler => eventSource.Raised -= handler).Subscribe(eventValues.Add))
        {
            eventSource.Raise();
        }

        var fromAsync = await Signal.FromAsync(static () => Task.FromResult(RetryResult)).ToTask();
        var fromAsyncWithToken = await Signal.FromAsync(static token =>
            Task.FromResult(token.IsCancellationRequested ? -1 : RetrySuccessAttempt)).ToTask();
        await Assert.That(fromAsync).IsEqualTo(RetryResult);
        await Assert.That(fromAsyncWithToken).IsEqualTo(RetrySuccessAttempt);
        int[] expectedValues = [FirstValue, SecondValue, FirstValue, SecondValue];
        await Assert.That(values.SequenceEqual(expectedValues)).IsTrue();
        await Assert.That(completed).IsEqualTo(SecondValue);
        await Assert.That(errors.Count).IsEqualTo(1);
        await Assert.That(eventValues.Count).IsEqualTo(1);
        await Assert.That(eventValues[0].Sender!).IsSameReferenceAs(eventSource);
        await Assert.That(eventValues[0].EventArgs).IsSameReferenceAs(EventArgs.Empty);
    }

    /// <summary>Asserts every factory rejects a null callback or an out-of-range count before it produces a signal.</summary>
    private static void AssertFactoryGuardsRejectInvalidArguments()
    {
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Sequence(FirstValue, -1));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Sequence(FirstValue, SecondValue, null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Loop(FirstValue, -1));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Unfold(0, null!, static state => state, static state => state));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Unfold(0, static _ => true, null!, static state => state));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.Unfold<int, int>(0, static _ => true, static state => state, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEventPattern(null!, static _ => { }));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEventPattern(
            static _ => { },
            null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEventPattern<EventArgs>(null!, static _ => { }));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEventPattern<EventArgs>(
            static _ => { },
            null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start(static () => FirstValue, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Start((Action)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(TimeSpan.Zero, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(DateTimeOffset.UnixEpoch, null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(static () => Signal.Every(TimeSpan.FromTicks(-1)));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.After(TimeSpan.Zero, TimeSpan.Zero, null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsync((Func<Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromAsync((Func<CancellationToken, Task<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).SubscribeOn(Sequencer.Immediate));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.None<int>().SubscribeOn(null!));
    }

    /// <summary>Asserts blend, chain, pair, and sync-latest preserve ordering over enumerable-backed sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertEnumerableSourceCombinatorsPreserveOrdering()
    {
        List<int> merged = [];
        List<int> concatenated = [];
        List<int> zipped = [];
        List<string> latest = [];
        _ = Signal.Blend(
            Signal.FromEnumerable(TakeWhileExpected),
            Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(merged.Add);
        _ = Signal.Chain(
            Signal.FromEnumerable(TakeWhileExpected),
            Signal.FromEnumerable([RetrySuccessAttempt, FourthValue])).Subscribe(concatenated.Add);
        _ = Signal.Pair(
            Signal.FromEnumerable(TakeWhileExpected),
            Signal.FromEnumerable([ProjectedFirstValue, ProjectedThirdValue]),
            static (left, right) => left + right).Subscribe(zipped.Add);
        _ = Signal.SyncLatest(
            Signal.FromEnumerable(TakeWhileExpected),
            Signal.FromEnumerable(["a", "b"]),
            static (left, right) => left + right).Subscribe(latest.Add);
        await Assert.That(merged.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(concatenated.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(zipped.SequenceEqual(ZippedExpected)).IsTrue();
        await Assert.That(latest.SequenceEqual(LatestExpected)).IsTrue();
    }

    /// <summary>Asserts the range-specialized combinators preserve ordering and validate their observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertRangeSourceCombinatorsPreserveOrdering()
    {
        List<int> rangeConcatenated = [];
        List<int> rangeMerged = [];
        List<int> rangeRace = [];
        List<int> rangeAmb = [];
        List<int> rangeLatest = [];
        List<int> rangeWithLatest = [];
        List<int> rangeForkJoin = [];
        RecordingWitness<int> rangeObserver = new();
        var rangeConcatSignal = Signal.Chain(
            Signal.Sequence(FirstValue, SecondValue),
            Signal.Sequence(RetrySuccessAttempt, SecondValue));
        _ = rangeConcatSignal.Subscribe(rangeConcatenated.Add);
        _ = rangeConcatSignal.Subscribe(rangeObserver);
        _ = Signal.Blend(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue))
            .Subscribe(rangeMerged.Add);
        _ = Signal.Race(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue))
            .Subscribe(rangeRace.Add);
        _ = Signal.Race(Signal.Sequence(FirstValue, SecondValue), Signal.Sequence(RetrySuccessAttempt, SecondValue))
            .Subscribe(rangeAmb.Add);
        _ = Signal.SyncLatest(
            Signal.Sequence(FirstValue, SecondValue),
            Signal.Sequence(ProjectionMultiplier, SecondValue),
            static (left, right) => left + right).Subscribe(rangeLatest.Add);
        _ = Signal.Sequence(FirstValue, SecondValue)
            .Latch(Signal.Sequence(ProjectionMultiplier, SecondValue), static (left, right) => left + right)
            .Subscribe(rangeWithLatest.Add);
        _ = Signal.ForkJoin(
            Signal.Sequence(FirstValue, SecondValue),
            Signal.Sequence(ProjectionMultiplier, SecondValue),
            static (left, right) => left + right).Subscribe(rangeForkJoin.Add);
        _ = Assert.Throws<ArgumentNullException>(() => rangeConcatSignal.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)rangeConcatSignal).Subscribe(
            null!,
            static _ => { },
            static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => ((IInlineSignal<int>)rangeConcatSignal).Subscribe(
            static _ => { },
            static _ => { },
            null!));
        await Assert.That(rangeConcatenated.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(rangeObserver.Values.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(rangeObserver.Completed).IsEqualTo(1);
        await Assert.That(rangeMerged.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(rangeRace.SequenceEqual(TakeWhileExpected)).IsTrue();
        await Assert.That(rangeAmb.SequenceEqual(TakeWhileExpected)).IsTrue();
        int[] expectedRangeLatest = [ProjectedSecondBucketPeerValue, RangeZipShorterSecondResult];
        await Assert.That(rangeLatest.SequenceEqual(expectedRangeLatest)).IsTrue();
        int[] expectedRangeWithLatest = [ProjectedSecondBucketPeerValue, RangeZipShorterSecondResult];
        await Assert.That(rangeWithLatest.SequenceEqual(expectedRangeWithLatest)).IsTrue();
        int[] expectedRangeForkJoin = [RangeZipShorterSecondResult];
        await Assert.That(rangeForkJoin.SequenceEqual(expectedRangeForkJoin)).IsTrue();
    }

    /// <summary>Verifies sequence boundary operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySequenceBoundaryOperators()
    {
        List<int> leadAppend = [];
        List<int> ignored = [];
        List<int> distinctBy = [];
        List<int> takeWhile = [];
        List<int> skipWhile = [];
        List<int> defaulted = [];
        _ = Signal.FromEnumerable([SecondValue, RetrySuccessAttempt]).Lead(FirstValue).Append(FourthValue).Prepend(0)
            .Subscribe(leadAppend.Add);
        _ = Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).IgnoreValues().Subscribe(ignored.Add);
        _ = Signal.FromEnumerable([
            ProjectedSecondValue, ProjectedSecondBucketPeerValue, ProjectedFourthValue, SecondZipResult
        ]).DistinctBy(static value => value / ProjectionMultiplier).Subscribe(distinctBy.Add);
        _ = Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt, FirstValue])
            .TakeWhile(static value => value < RetrySuccessAttempt).Subscribe(takeWhile.Add);
        _ = Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt, FirstValue])
            .SkipWhile(static value => value < RetrySuccessAttempt).Subscribe(skipWhile.Add);
        _ = Signal.None<int>().DefaultIfEmpty(RetryResult).Subscribe(defaulted.Add);
        await Assert.That(leadAppend.SequenceEqual(LeadAppendExpected)).IsTrue();
        await Assert.That(ignored.Count).IsEqualTo(0);
        await Assert.That(distinctBy.SequenceEqual(DistinctByExpected)).IsTrue();
        await Assert.That(takeWhile.SequenceEqual(TakeWhileExpected)).IsTrue();
        await Assert.That(skipWhile.SequenceEqual(SkipWhileExpected)).IsTrue();
        await Assert.That(defaulted.SequenceEqual(RetryResultExpected)).IsTrue();
    }

    /// <summary>Verifies boolean terminal operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    private static async Task VerifyBooleanTerminalOperators()
    {
        List<int> count = [];
        List<bool> any = [];
        List<bool> all = [];
        List<bool> contains = [];
        List<bool> isEmpty = [];
        _ = Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).Count().Subscribe(count.Add);
        _ = Signal.FromEnumerable([FirstValue, SecondValue, RetrySuccessAttempt]).Any(static value => value == SecondValue)
            .Subscribe(any.Add);
        _ = Signal.FromEnumerable([SecondValue, FourthValue, SixthValue]).All(static value => value % SecondValue == 0)
            .Subscribe(all.Add);
        _ = Signal.FromEnumerable([SecondValue, FourthValue, SixthValue]).Contains(FourthValue).Subscribe(contains.Add);
        _ = Signal.None<int>().IsEmpty().Subscribe(isEmpty.Add);
        int[] expectedCount = [RetrySuccessAttempt];
        await Assert.That(count.SequenceEqual(expectedCount)).IsTrue();
        await Assert.That(any.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(all.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(contains.SequenceEqual(TrueExpected)).IsTrue();
        await Assert.That(isEmpty.SequenceEqual(TrueExpected)).IsTrue();
    }

    /// <summary>Verifies selection and projection operators.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task VerifySelectionAndProjectionOperators()
    {
        List<int> selected = [];
        _ = Signal.FromEnumerable(TakeWhileExpected)
            .Bind(static value => Signal.Sequence(value * ProjectionMultiplier, SecondValue)).Subscribe(selected.Add);
        await Assert.That(selected.SequenceEqual(SelectedProjectionExpected)).IsTrue();
    }

    /// <summary>Verifies task-based alias operators.</summary>
    /// <returns>A task that completes when assertions have run.</returns>
    private static async Task VerifyTaskAliasOperators()
    {
        var converted = new[] { FourthValue, AfterTicks }.ToObservable();
        var last = await converted.ToTask();
        var lastAlias = await converted.LastAsync();
        var lastDefault = await Signal.None<int>().LastOrDefaultAsync(RetryResult);
        var array = await Signal.Sequence(FirstValue, FourthValue).ToArrayAsync();
        var list = await Signal.Sequence(FirstValue, FourthValue).ToListAsync();
        CaptureSynchronousMaterialization(out var observedArray, out var observedList);
        var first = await Signal.FromEnumerable([RepeatValue, ProjectionMultiplier]).FirstAsync().ToTask();
        var started = await Signal.Start(static () => ProjectedSecondValue, Sequencer.CurrentThread).ToTask();
        await Assert.That(last).IsEqualTo(AfterTicks);
        await Assert.That(lastAlias).IsEqualTo(AfterTicks);
        await Assert.That(lastDefault).IsEqualTo(RetryResult);
        await Assert.That(array.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(list.SequenceEqual(FourItemExpected)).IsTrue();
        await Assert.That(observedArray.SequenceEqual((IEnumerable<int>)[FirstValue, SecondValue])).IsTrue();
        await Assert.That(observedList.SequenceEqual([FirstValue, SecondValue])).IsTrue();
        await Assert.That(first).IsEqualTo(RepeatValue);
        await Assert.That(started).IsEqualTo(ProjectedSecondValue);
    }

    /// <summary>Captures output of synchronous materialization operators that emit observable results.</summary>
    /// <param name="observedArray">Receives the array emitted by the synchronous to-array operator.</param>
    /// <param name="observedList">Receives the list emitted by the synchronous to-list operator.</param>
    private static void CaptureSynchronousMaterialization(out int[] observedArray, out List<int> observedList)
    {
        int[] capturedArray = [];
        Signal.Sequence(FirstValue, SecondValue).ToArray().Subscribe(value => capturedArray = value).Dispose();
        List<int> capturedList = [];
        Signal.Sequence(FirstValue, SecondValue).ToList().Subscribe(value => capturedList = [.. value]).Dispose();
        observedArray = capturedArray;
        observedList = capturedList;
    }

    /// <summary>Test event source.</summary>
    private sealed class EventSource
    {
        /// <summary>Raised when <see cref = "Raise"/> is called.</summary>
        public event EventHandler? Raised;

        /// <summary>Raises the event.</summary>
        public void Raise() => Raised?.Invoke(this, EventArgs.Empty);
    }
}
