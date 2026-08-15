// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Concurrency;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Contract tests for async primitive aliases and async runtime integration points.</summary>
public sealed class AsyncPrimitiveContractTests
{
    /// <summary>A reused value of two for projections and counts.</summary>
    private const int Two = 2;

    /// <summary>Expected element count from the blend operator.</summary>
    private const int BlendedCount = 2;

    /// <summary>Expected sequence one, two, three.</summary>
    private static readonly int[] OneTwoThree = [1, 2, 3];

    /// <summary>Expected sequence one, two.</summary>
    private static readonly int[] OneTwo = [1, 2];

    /// <summary>Expected sequence zero, one, two, three.</summary>
    private static readonly int[] ZeroOneTwoThree = [0, 1, 2, 3];

    /// <summary>Expected sequence one, one, two.</summary>
    private static readonly int[] OneOneTwo = [1, 1, 2];

    /// <summary>Expected sequence three, four, five.</summary>
    private static readonly int[] ThreeFourFive = [3, 4, 5];

    /// <summary>Expected single-element sequence containing nine.</summary>
    private static readonly int[] NineOnly = [9];

    /// <summary>Expected single-element sequence containing one.</summary>
    private static readonly int[] OneOnly = [1];

    /// <summary>Expected single-element sequence containing two.</summary>
    private static readonly int[] TwoOnly = [2];

    /// <summary>Expected values produced by the tap projection.</summary>
    private static readonly int[] TappedExpected = [6, 8, 10, 12];

    /// <summary>Expected values produced by the async map projection.</summary>
    private static readonly int[] TwoFourSix = [2, 4, 6];

    /// <summary>Expected values produced by the stateful map projection.</summary>
    private static readonly int[] ElevenTwelveThirteen = [11, 12, 13];

    /// <summary>Expected running-fold accumulation values.</summary>
    private static readonly int[] FoldedExpected = [6, 14, 24, 36];

    /// <summary>Expected running-fold values for one, two, three.</summary>
    private static readonly int[] OneThreeSix = [1, 3, 6];

    /// <summary>Expected string sequence one, three.</summary>
    private static readonly string[] OneThree = ["one", "three"];

    /// <summary>Expected string sequence one, two.</summary>
    private static readonly string[] OneTwoStrings = ["one", "two"];

    /// <summary>Expected single-element string sequence one.</summary>
    private static readonly string[] OneStringOnly = ["one"];

    /// <summary>Expected string sequence a, b.</summary>
    private static readonly string[] AAndB = ["a", "b"];

    /// <summary>Expected string sequence aa, bbb.</summary>
    private static readonly string[] AaAndBbb = ["aa", "bbb"];

    /// <summary>Case-insensitive duplicate input.</summary>
    private static readonly string[] CaseInsensitiveInput = ["a", "A", "b"];

    /// <summary>Length-duplicate input.</summary>
    private static readonly string[] LengthDuplicateInput = ["aa", "ab", "bbb"];

    /// <summary>Mixed-type input used by the type-filtering tests.</summary>
    private static readonly object?[] MixedTypeInput = ["one", 2, "three", null];

    /// <summary>Single boxed string input.</summary>
    private static readonly object?[] BoxedOne = ["one"];

    /// <summary>Nullable string input with a null gap.</summary>
    private static readonly string?[] NullableOneNullTwo = ["one", null, "two"];

    /// <summary>Expected single-element sequence containing "2a".</summary>
    private static readonly string[] TwoA = ["2a"];

    /// <summary>Expected single-element sequence containing seven.</summary>
    private static readonly int[] SevenOnly = [7];

    /// <summary>Expected single-element sequence containing forty-two.</summary>
    private static readonly int[] FortyTwoOnly = [42];

    /// <summary>Expected single-element sequence containing twenty-four.</summary>
    private static readonly int[] TwentyFourOnly = [24];

    /// <summary>Expected single-element sequence containing five.</summary>
    private static readonly int[] FiveOnly = [5];

    /// <summary>Expected single-element sequence containing eleven.</summary>
    private static readonly int[] ElevenOnly = [11];

    /// <summary>Expected single-element sequence containing three.</summary>
    private static readonly int[] ThreeOnly = [3];

    /// <summary>Expected single-element sequence containing four.</summary>
    private static readonly int[] FourOnly = [4];

    /// <summary>Expected sequence four, five.</summary>
    private static readonly int[] FourFive = [4, 5];

    /// <summary>Expected sequence zero, one.</summary>
    private static readonly long[] ZeroOne = [0, 1];

    /// <summary>Expected single-element sequence containing zero.</summary>
    private static readonly long[] ZeroOnly = [0];

    /// <summary>Verifies async primitive factory aliases emit the same values as their observable async counterparts.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesFactoryAliasesMatchObservableAsyncSemantics()
    {
        const int SequenceStart = 3;
        const int SequenceCount = 3;
        const int EmittedValue = 9;
        var sequence = await AsyncObs.Sequence(SequenceStart, SequenceCount).ToListAsync();
        var emitted = await AsyncObs.Emit(EmittedValue).ToListAsync();
        var none = await AsyncObs.None<int>().ToListAsync();
        var enumerable = await OneTwoThree.ToAsyncSignal().ToListAsync();

        // Typed local forces the IEnumerable<T> sequence overload, not the scalar overload.
        int[] empty = [];
        await Assert.That(sequence.SequenceEqual(ThreeFourFive)).IsTrue();
        await Assert.That(emitted.SequenceEqual(NineOnly)).IsTrue();
        await Assert.That(none.SequenceEqual(empty)).IsTrue();
        await Assert.That(enumerable.SequenceEqual(OneTwoThree)).IsTrue();
        InvalidOperationException error = new("failure");
        InvalidOperationException? observed = null;
        try
        {
            await AsyncObs.Fail<int>(error).ToListAsync();
        }
        catch (InvalidOperationException exception)
        {
            observed = exception;
        }

        await Assert.That(observed!).IsSameReferenceAs(error);
    }

    /// <summary>Verifies remaining async primitive factory aliases forward to their canonical factories.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesFactoryAliasesCoverRemainingWrappers()
    {
        const int FirstValue = 1;
        const int SecondValue = 2;
        const int ThirdValue = 3;
        const int FourthValue = 4;
        var period = TimeSpan.FromMilliseconds(1);
        var unit = await SignalAsyncReactiveExtensions.EmitRxVoid().FirstAsync();
        var enumerable = await AsyncObs.FromEnumerable(FourFive).ToListAsync();
        var asyncEnumerable = await AsyncObs.FromAsyncEnumerable(ReadValuesAsync()).ToListAsync();
        var after = await AsyncObs.After(TimeSpan.Zero).ToListAsync();
        var periodicAfter = await AsyncObs.After(TimeSpan.Zero, period).Take(Two).ToListAsync();
        var every = await AsyncObs.Every(period).Take(1).ToListAsync();
        var pulse = await AsyncObs.Pulse(period).Take(1).ToListAsync();
        var chained = await AsyncObs.Chain(AsyncObs.Emit(FirstValue), AsyncObs.Emit(SecondValue)).ToListAsync();
        var blended = await AsyncObs.Blend(AsyncObs.Emit(ThirdValue), AsyncObs.Emit(FourthValue)).ToListAsync();
        List<int> subscribed = [];
        await using var subscription = await AsyncObs.Emit(FirstValue).SubscribeAsync(subscribed.Add);
        await Assert.That(unit).IsEqualTo(RxVoid.Default);
        await Assert.That(enumerable.SequenceEqual(FourFive)).IsTrue();
        await Assert.That(asyncEnumerable.SequenceEqual(FourFive)).IsTrue();
        await Assert.That(after.SequenceEqual(ZeroOnly)).IsTrue();
        await Assert.That(periodicAfter.SequenceEqual(ZeroOne)).IsTrue();
        await Assert.That(every.SequenceEqual(ZeroOnly)).IsTrue();
        await Assert.That(pulse.SequenceEqual(ZeroOnly)).IsTrue();
        await Assert.That(chained.SequenceEqual(OneTwo)).IsTrue();
        await Assert.That(blended.Count).IsEqualTo(BlendedCount);
        await Assert.That(blended).Contains(ThirdValue);
        await Assert.That(blended).Contains(FourthValue);
        await Assert.That(subscribed.SequenceEqual(OneOnly)).IsTrue();
    }

    /// <summary>Verifies async primitive transformation aliases compose using the core naming surface.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesTransformationAliasesComposeLikeCoreNaming()
    {
        const int SequenceStart = 1;
        const int SequenceCount = 6;
        const int KeepThreshold = 4;
        const int Seed = 0;
        List<int> tapped = [];
        var values = await AsyncObs.Sequence(SequenceStart, SequenceCount).Map(static value => value * Two)
            .Keep(static value => value > KeepThreshold).Tap(tapped.Add)
            .Fold(Seed, static (acc, value) => acc + value).ToListAsync();
        await Assert.That(tapped.SequenceEqual(TappedExpected)).IsTrue();
        await Assert.That(values.SequenceEqual(FoldedExpected)).IsTrue();
        var typed = await MixedTypeInput.ToAsyncSignal().KeepType<string>().ToListAsync();
        await Assert.That(typed.SequenceEqual(OneThree)).IsTrue();
    }

    /// <summary>Verifies remaining async primitive transformation aliases forward to their canonical operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesTransformationAliasesCoverRemainingWrappers()
    {
        const int FirstValue = 1;
        const int ThirdValue = 3;
        var source = AsyncObs.Sequence(FirstValue, ThirdValue);
        await Assert.That(source.ToAsyncSignal()).IsSameReferenceAs(source);
        await AssertMapAndKeepAliasesForwardAsync(source);
        await AssertTapAliasesForwardAsync(source);
        await AssertFoldAndBindAliasesForwardAsync(source);
        await AssertUniqueAndCastAliasesForwardAsync();
        AssertStatefulAliasesRejectNullSelectors(source);
    }

    /// <summary>Verifies async primitive combination aliases forward to the expected async operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesCombinationAliasesForwardToAsyncOperators()
    {
        const int ChainStart = 2;
        const int ChainCount = 2;
        const int PairLeft = 2;
        const int LatestLeft = 2;
        const int LatestRight = 5;
        const int BlendLeft = 10;
        const int BlendRight = 20;
        var chained = await AsyncObs.Emit(1).Chain(AsyncObs.Sequence(ChainStart, ChainCount)).ToListAsync();
        var paired = await AsyncObs.Emit(PairLeft).Pair(AsyncObs.Emit("a"), static (left, right) => $"{left}{right}")
            .ToListAsync();
        var latest = await AsyncObs.Emit(LatestLeft)
            .SyncLatest(AsyncObs.Emit(LatestRight), static (left, right) => left + right).ToListAsync();
        var blended = await AsyncObs.Emit(BlendLeft).Blend(AsyncObs.Emit(BlendRight)).ToListAsync();
        await Assert.That(chained.SequenceEqual(OneTwoThree)).IsTrue();
        await Assert.That(paired.SequenceEqual(TwoA)).IsTrue();
        await Assert.That(latest.SequenceEqual(SevenOnly)).IsTrue();
        await Assert.That(blended.Count).IsEqualTo(BlendedCount);
        await Assert.That(blended).Contains(BlendLeft);
        await Assert.That(blended).Contains(BlendRight);
    }

    /// <summary>Verifies remaining async primitive combination aliases forward to the expected async operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesCombinationAliasesCoverRemainingWrappers()
    {
        const int FirstValue = 1;
        const int SecondValue = 2;
        const int ThirdValue = 3;
        const int FourthValue = 4;
        var latest = await AsyncObs.Emit(FirstValue)
            .PairLatest(AsyncObs.Emit(SecondValue), static (left, right) => left + right).ToListAsync();
        var chained =
            await ((IEnumerable<IObservableAsync<int>>)[AsyncObs.Emit(FirstValue), AsyncObs.Emit(SecondValue)])
                .ToAsyncSignal().Chain().ToListAsync();
        var blended =
            await ((IEnumerable<IObservableAsync<int>>)[AsyncObs.Emit(ThirdValue), AsyncObs.Emit(FourthValue)])
                .ToAsyncSignal().Blend().ToListAsync();
        var switched = await ((IEnumerable<IObservableAsync<int>>)[AsyncObs.Never<int>(), AsyncObs.Emit(FourthValue)])
            .ToAsyncSignal().SwitchTo().Take(1).ToListAsync();
        await Assert.That(latest.SequenceEqual(ThreeOnly)).IsTrue();
        await Assert.That(chained.SequenceEqual(OneTwo)).IsTrue();
        await Assert.That(blended.Count).IsEqualTo(BlendedCount);
        await Assert.That(blended).Contains(ThirdValue);
        await Assert.That(blended).Contains(FourthValue);
        await Assert.That(switched.SequenceEqual(FourOnly)).IsTrue();
    }

    /// <summary>Verifies async primitive error handling and terminal aliases match expected behavior.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesErrorAndTerminalAliasesMatchExpectedBehavior()
    {
        const int RecoveredValue = 42;
        const int ResumedValue = 24;
        const int ReattemptValue = 7;
        const int ReattemptCount = 1;
        const int SequenceStart = 1;
        const int SequenceCount = 3;
        var recovered = await AsyncObs.Fail<int>(new InvalidOperationException())
            .Recover(static _ => AsyncObs.Emit(RecoveredValue)).ToListAsync();
        var resumed = await AsyncObs.Fail<int>(new InvalidOperationException()).Resume(AsyncObs.Emit(ResumedValue))
            .ToListAsync();
        var attempt = 0;
        var reattempted = await AsyncObs
            .Defer(() =>
            {
                attempt++;
                return attempt == 1
                    ? AsyncObs.Fail<int>(new InvalidOperationException())
                    : AsyncObs.Emit(ReattemptValue);
            })
            .Reattempt(ReattemptCount).ToListAsync();
        var collected = await AsyncObs.Sequence(SequenceStart, SequenceCount).CollectArrayAsync();
        await Assert.That(recovered.SequenceEqual(FortyTwoOnly)).IsTrue();
        await Assert.That(resumed.SequenceEqual(TwentyFourOnly)).IsTrue();
        await Assert.That(reattempted.SequenceEqual(SevenOnly)).IsTrue();
        await Assert.That(collected.SequenceEqual(OneTwoThree)).IsTrue();
    }

    /// <summary>Verifies remaining async primitive error and terminal aliases forward to their canonical operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesErrorAndTerminalAliasesCoverRemainingWrappers()
    {
        const int RescuedValue = 42;
        const int LeadValue = 0;
        const int SequenceStart = 1;
        const int SequenceCount = 3;
        const int Seed = 0;
        var rescued = await AsyncObs.Fail<int>(new InvalidOperationException())
            .Rescue(static _ => AsyncObs.Emit(RescuedValue)).ToListAsync();
        var led = await AsyncObs.Sequence(SequenceStart, SequenceCount).Lead(LeadValue).ToListAsync();
        var collected = await AsyncObs.Sequence(SequenceStart, SequenceCount).CollectListAsync();
        var reduced = await AsyncObs.Sequence(SequenceStart, SequenceCount)
            .ReduceAsync(Seed, static (accumulator, value) => accumulator + value);
        await Assert.That(rescued.SequenceEqual(FortyTwoOnly)).IsTrue();
        await Assert.That(led.SequenceEqual(ZeroOneTwoThree)).IsTrue();
        await Assert.That(collected.SequenceEqual(OneTwoThree)).IsTrue();
        await Assert.That(reduced).IsEqualTo(OneThreeSix[^1]);
    }

    /// <summary>Verifies <c>Use</c> disposes its async resource after completion.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task UseDisposesAsyncResourceAfterCompletion()
    {
        const int EmittedValue = 5;
        var disposed = false;
        var values = await AsyncObs
            .Use(
                _ => new ValueTask<TestAsyncResource>(new TestAsyncResource(() => disposed = true)),
                static _ => AsyncObs.Emit(EmittedValue)).ToListAsync();
        await Assert.That(values.SequenceEqual(FiveOnly)).IsTrue();
        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies <c>ObserveOn</c> schedules direct work through the supplied sequencer.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task ObserveOnSequencerSchedulesDirectWorkItems()
    {
        const int EmittedValue = 11;
        QueuedSequencer sequencer = new();
        var task = AsyncObs.Emit(EmittedValue).WitnessOn(sequencer, true).ToListAsync().AsTask();
        var values = await DrainUntilComplete(task, sequencer);
        await Assert.That(values.SequenceEqual(ElevenOnly)).IsTrue();
        await Assert.That(sequencer.ScheduleCount > 0).IsTrue();
    }

    /// <summary>Verifies shift and expire aliases use the time-based async operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task ShiftAndExpireAliasesUseTimeBasedOperators()
    {
        const int EmittedValue = 3;
        const int DelayMilliseconds = 1;
        var shifted = await AsyncObs.Emit(EmittedValue).Shift(TimeSpan.FromMilliseconds(DelayMilliseconds))
            .ToListAsync();
        await Assert.That(shifted.SequenceEqual(ThreeOnly)).IsTrue();
        TimeoutException? timeout = null;
        try
        {
            await AsyncObs.Never<int>().Expire(TimeSpan.FromMilliseconds(DelayMilliseconds)).ToListAsync();
        }
        catch (TimeoutException exception)
        {
            timeout = exception;
        }

        await Assert.That(timeout).IsNotNull();
    }

    /// <summary>Drains queued sequencer work until the supplied task completes.</summary>
    /// <typeparam name = "T">The task result type.</typeparam>
    /// <param name = "task">The task to observe for completion.</param>
    /// <param name = "sequencer">The queued sequencer to drain.</param>
    /// <returns>The completed task result.</returns>
    private static async Task<T> DrainUntilComplete<T>(Task<T> task, QueuedSequencer sequencer)
    {
        const int MaxIterations = 1_000;
        const int PollDelayMilliseconds = 1;
        const int TimeoutSeconds = 5;
        for (var i = 0; i < MaxIterations; i++)
        {
            sequencer.DrainAll();
            if (task.IsCompleted)
            {
                return await task.ConfigureAwait(false);
            }

            await Task.Delay(PollDelayMilliseconds).ConfigureAwait(false);
        }

        return await task.WaitAsync(TimeSpan.FromSeconds(TimeoutSeconds)).ConfigureAwait(false);
    }

    /// <summary>Reads a short async enumerable sequence for factory alias coverage.</summary>
    /// <returns>The async enumerable values.</returns>
    private static async IAsyncEnumerable<int> ReadValuesAsync()
    {
        await Task.Yield();
        yield return FourFive[0];
        yield return FourFive[1];
    }

    /// <summary>Test sequencer that queues scheduled work until drained explicitly.</summary>
    /// <summary>Verifies the asynchronous and stateful map/keep aliases forward to their canonical operators.</summary>
    /// <param name = "source">The 1..3 sequence under test.</param>
    /// <returns>A task to monitor completion.</returns>
    private static async Task AssertMapAndKeepAliasesForwardAsync(IObservableAsync<int> source)
    {
        const int State = 10;
        const int KeepWithThreshold = 12;
        var mappedAsync = await source.Map(static (value, _) => new ValueTask<int>(value * Two)).ToListAsync();
        var mappedWith = await source.MapWith(State, static (state, value) => state + value).ToListAsync();
        var keptAsync = await source.Keep(static (value, _) => new(value % Two == 0)).ToListAsync();
        var keptWith = await source.KeepWith(State, static (state, value) => state + value > KeepWithThreshold)
            .ToListAsync();
        await Assert.That(mappedAsync.SequenceEqual(TwoFourSix)).IsTrue();
        await Assert.That(mappedWith.SequenceEqual(ElevenTwelveThirteen)).IsTrue();
        await Assert.That(keptAsync.SequenceEqual(TwoOnly)).IsTrue();
        await Assert.That(keptWith.SequenceEqual(ThreeOnly)).IsTrue();
    }

    /// <summary>Verifies the asynchronous and synchronous tap aliases observe values, errors and completion.</summary>
    /// <param name = "source">The 1..3 sequence under test.</param>
    /// <returns>A task to monitor completion.</returns>
    private static async Task AssertTapAliasesForwardAsync(IObservableAsync<int> source)
    {
        const int FirstValue = 1;
        List<int> asyncTapped = [];
        var asyncCompleted = false;
        var asyncTapValues = await source.Tap(
            (value, _) =>
            {
                asyncTapped.Add(value);
                return default;
            },
            null,
            _ =>
            {
                asyncCompleted = true;
                return default;
            }).ToListAsync();
        List<Exception> syncTapErrors = [];
        var syncCompleted = false;
        var syncTapValues = await AsyncObs.Emit(FirstValue).Tap(
            static _ => { },
            syncTapErrors.Add,
            () => syncCompleted = true).ToListAsync();
        await Assert.That(asyncTapped.SequenceEqual(OneTwoThree)).IsTrue();
        await Assert.That(asyncTapValues.SequenceEqual(OneTwoThree)).IsTrue();
        await Assert.That(asyncCompleted).IsTrue();
        await Assert.That(syncTapValues.SequenceEqual(OneOnly)).IsTrue();
        await Assert.That(syncTapErrors.Count).IsEqualTo(0);
        await Assert.That(syncCompleted).IsTrue();
    }

    /// <summary>Verifies the fold, bind and flat-map aliases forward to their canonical operators.</summary>
    /// <param name = "source">The 1..3 sequence under test.</param>
    /// <returns>A task to monitor completion.</returns>
    private static async Task AssertFoldAndBindAliasesForwardAsync(IObservableAsync<int> source)
    {
        const int FirstValue = 1;
        const int State = 10;
        var foldedAsync =
            await source.Fold(0, static (accumulator, value, _) => new(accumulator + value)).ToListAsync();
        var bound = await AsyncObs.Emit(FirstValue).Bind(static value => AsyncObs.Emit(value + State)).ToListAsync();
        var flatMapped = await AsyncObs.Emit(FirstValue).FlatMap(static value => AsyncObs.Emit(value + State))
            .ToListAsync();
        var flatMappedAsync = await AsyncObs.Emit(FirstValue)
            .FlatMap(static (value, _) => new ValueTask<IObservableAsync<int>>(AsyncObs.Emit(value + State)))
            .ToListAsync();
        await Assert.That(foldedAsync.SequenceEqual(OneThreeSix)).IsTrue();
        await Assert.That(bound.SequenceEqual(ElevenOnly)).IsTrue();
        await Assert.That(flatMapped.SequenceEqual(ElevenOnly)).IsTrue();
        await Assert.That(flatMappedAsync.SequenceEqual(ElevenOnly)).IsTrue();
    }

    /// <summary>Verifies the de-duplication, cast and null-filtering aliases forward to their canonical operators.</summary>
    /// <returns>A task to monitor completion.</returns>
    private static async Task AssertUniqueAndCastAliasesForwardAsync()
    {
        var unique = await OneOneTwo.ToAsyncSignal().Unique().ToListAsync();
        var uniqueComparer = await CaseInsensitiveInput.ToAsyncSignal().Unique(StringComparer.OrdinalIgnoreCase)
            .ToListAsync();
        var uniqueBy = await LengthDuplicateInput.ToAsyncSignal().UniqueBy(static value => value.Length).ToListAsync();
        var uniqueByComparer = await CaseInsensitiveInput.ToAsyncSignal()
            .UniqueBy(static value => value, StringComparer.OrdinalIgnoreCase).ToListAsync();
        var casted = await BoxedOne.ToAsyncSignal().CastTo<string>().ToListAsync();
        var notNull = await NullableOneNullTwo.ToAsyncSignal().KeepNotNull().ToListAsync();
        await Assert.That(unique.SequenceEqual(OneTwo)).IsTrue();
        await Assert.That(uniqueComparer.SequenceEqual(AAndB)).IsTrue();
        await Assert.That(uniqueBy.SequenceEqual(AaAndBbb)).IsTrue();
        await Assert.That(uniqueByComparer.SequenceEqual(AAndB)).IsTrue();
        await Assert.That(casted.SequenceEqual(OneStringOnly)).IsTrue();
        await Assert.That(notNull.SequenceEqual(OneTwoStrings)).IsTrue();
    }

    /// <summary>Verifies the stateful map/keep aliases reject a null selector.</summary>
    /// <param name = "source">The 1..3 sequence under test.</param>
    private static void AssertStatefulAliasesRejectNullSelectors(IObservableAsync<int> source)
    {
        const int State = 10;
        _ = Assert.Throws<ArgumentNullException>(() => source.MapWith<int, int, int>(State, null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.KeepWith(State, (Func<int, int, bool>)null!));
    }

    /// <summary>A sequencer that queues work items so a test can drain them deterministically.</summary>
    private sealed class QueuedSequencer : ISequencer
    {
        /// <summary>A fixed deterministic clock value for the test sequencer.</summary>
        private static readonly DateTimeOffset FixedNow = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>The queue of scheduled work items awaiting drain.</summary>
        private readonly ConcurrentQueue<IWorkItem> _items = new();

        /// <inheritdoc/>
        public DateTimeOffset Now => FixedNow;

        /// <inheritdoc/>
        public long Timestamp => FixedNow.Ticks;

        /// <summary>Gets the number of scheduled work items.</summary>
        public int ScheduleCount { get; private set; }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            ScheduleCount++;
            _items.Enqueue(item);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Executes all queued work items.</summary>
        public void DrainAll()
        {
            while (_items.TryDequeue(out var item))
            {
                item.Execute();
            }
        }
    }

    /// <summary>Async disposable test resource that invokes a callback when disposed.</summary>
    /// <param name = "onDispose">The callback invoked during disposal.</param>
    private sealed class TestAsyncResource(Action onDispose) : IAsyncDisposable
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync()
        {
            onDispose();
            return default;
        }
    }
}
