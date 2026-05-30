// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Concurrency;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Contract tests for async primitive aliases and async runtime integration points.
/// </summary>
public sealed class AsyncPrimitiveContractTests
{
    /// <summary>A reused value of two for projections and counts.</summary>
    private const int Two = 2;

    /// <summary>Expected element count from the blend operator.</summary>
    private const int BlendedCount = 2;

    /// <summary>Expected sequence one, two, three.</summary>
    private static readonly int[] OneTwoThree = [1, 2, 3];

    /// <summary>Expected sequence three, four, five.</summary>
    private static readonly int[] ThreeFourFive = [3, 4, 5];

    /// <summary>Expected single-element sequence containing nine.</summary>
    private static readonly int[] NineOnly = [9];

    /// <summary>Expected values produced by the tap projection.</summary>
    private static readonly int[] TappedExpected = [6, 8, 10, 12];

    /// <summary>Expected running-fold accumulation values.</summary>
    private static readonly int[] FoldedExpected = [6, 14, 24, 36];

    /// <summary>Expected string sequence one, three.</summary>
    private static readonly string[] OneThree = ["one", "three"];

    /// <summary>Mixed-type input used by the type-filtering tests.</summary>
    private static readonly object?[] MixedTypeInput = ["one", 2, "three", null];

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

    /// <summary>
    /// Verifies async primitive factory aliases emit the same values as their observable async counterparts.
    /// </summary>
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

        Assert.Equal(ThreeFourFive, sequence);
        Assert.Equal(NineOnly, emitted);
        Assert.Equal(Array.Empty<int>(), none);
        Assert.Equal(OneTwoThree, enumerable);

        var error = new InvalidOperationException("failure");
        InvalidOperationException? observed = null;
        try
        {
            await AsyncObs.Fail<int>(error).ToListAsync();
        }
        catch (InvalidOperationException exception)
        {
            observed = exception;
        }

        Assert.Same(error, observed!);
    }

    /// <summary>
    /// Verifies async primitive transformation aliases compose using the core naming surface.
    /// </summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task PrimitivesTransformationAliasesComposeLikeCoreNaming()
    {
        const int SequenceStart = 1;
        const int SequenceCount = 6;
        const int KeepThreshold = 4;
        const int Seed = 0;

        var tapped = new List<int>();
        var values = await AsyncObs.Sequence(SequenceStart, SequenceCount)
            .Map(value => value * Two)
            .Keep(value => value > KeepThreshold)
            .Tap(tapped.Add)
            .Fold(Seed, (acc, value) => acc + value)
            .ToListAsync();

        Assert.Equal(TappedExpected, tapped);
        Assert.Equal(FoldedExpected, values);

        var typed = await MixedTypeInput
            .ToAsyncSignal()
            .KeepType<string>()
            .ToListAsync();
        Assert.Equal(OneThree, typed);
    }

    /// <summary>
    /// Verifies async primitive combination aliases forward to the expected async operators.
    /// </summary>
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

        var chained = await AsyncObs.Emit(1).Chain(AsyncObs.Sequence(ChainStart, ChainCount))
            .ToListAsync();
        var paired = await AsyncObs.Emit(PairLeft).Pair(AsyncObs.Emit("a"), (left, right) => $"{left}{right}").ToListAsync();
        var latest = await AsyncObs.Emit(LatestLeft).SyncLatest(AsyncObs.Emit(LatestRight), (left, right) => left + right).ToListAsync();
        var blended = await AsyncObs.Emit(BlendLeft).Blend(AsyncObs.Emit(BlendRight)).ToListAsync();

        Assert.Equal(OneTwoThree, chained);
        Assert.Equal(TwoA, paired);
        Assert.Equal(SevenOnly, latest);
        Assert.Equal(BlendedCount, blended.Count);
        Assert.Contains(BlendLeft, blended);
        Assert.Contains(BlendRight, blended);
    }

    /// <summary>
    /// Verifies async primitive error handling and terminal aliases match expected behavior.
    /// </summary>
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
            .Recover(_ => AsyncObs.Emit(RecoveredValue))
            .ToListAsync();
        var resumed = await AsyncObs.Fail<int>(new InvalidOperationException())
            .Resume(AsyncObs.Emit(ResumedValue))
            .ToListAsync();
        var attempt = 0;
        var reattempted = await AsyncObs.Defer(() =>
            ++attempt == 1 ? AsyncObs.Fail<int>(new InvalidOperationException()) : AsyncObs.Emit(ReattemptValue))
            .Reattempt(ReattemptCount)
            .ToListAsync();
        var collected = await AsyncObs.Sequence(SequenceStart, SequenceCount).CollectArrayAsync();

        Assert.Equal(FortyTwoOnly, recovered);
        Assert.Equal(TwentyFourOnly, resumed);
        Assert.Equal(SevenOnly, reattempted);
        Assert.Equal((IEnumerable<int>)OneTwoThree, collected);
    }

    /// <summary>
    /// Verifies <c>Use</c> disposes its async resource after completion.
    /// </summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task UseDisposesAsyncResourceAfterCompletion()
    {
        const int EmittedValue = 5;

        var disposed = false;
        var values = await AsyncObs.Use(
            _ => new ValueTask<TestAsyncResource>(new TestAsyncResource(() => disposed = true)),
            _ => AsyncObs.Emit(EmittedValue))
            .ToListAsync();

        Assert.Equal(FiveOnly, values);
        Assert.True(disposed);
    }

    /// <summary>
    /// Verifies <c>ObserveOn</c> schedules direct work through the supplied sequencer.
    /// </summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task ObserveOnSequencerSchedulesDirectWorkItems()
    {
        const int EmittedValue = 11;

        var sequencer = new QueuedSequencer();
        var task = AsyncObs.Emit(EmittedValue)
            .ObserveOn(sequencer, forceYielding: true)
            .ToListAsync()
            .AsTask();

        var values = await DrainUntilComplete(task, sequencer);

        Assert.Equal(ElevenOnly, values);
        Assert.True(sequencer.ScheduleCount > 0);
    }

    /// <summary>
    /// Verifies shift and expire aliases use the time-based async operators.
    /// </summary>
    /// <returns>A task to monitor completion.</returns>
    [Test]
    public async Task ShiftAndExpireAliasesUseTimeBasedOperators()
    {
        const int EmittedValue = 3;
        const int DelayMilliseconds = 1;

        var shifted = await AsyncObs.Emit(EmittedValue).Shift(TimeSpan.FromMilliseconds(DelayMilliseconds)).ToListAsync();
        Assert.Equal(ThreeOnly, shifted);

        TimeoutException? timeout = null;
        try
        {
            await AsyncObs.Never<int>().Expire(TimeSpan.FromMilliseconds(DelayMilliseconds)).ToListAsync();
        }
        catch (TimeoutException exception)
        {
            timeout = exception;
        }

        Assert.NotNull(timeout);
    }

    /// <summary>
    /// Drains queued sequencer work until the supplied task completes.
    /// </summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to observe for completion.</param>
    /// <param name="sequencer">The queued sequencer to drain.</param>
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

    /// <summary>
    /// Test sequencer that queues scheduled work until drained explicitly.
    /// </summary>
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

        /// <summary>
        /// Gets the number of scheduled work items.
        /// </summary>
        public int ScheduleCount { get; private set; }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            ScheduleCount++;
            _items.Enqueue(item);
        }

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>
        /// Executes all queued work items.
        /// </summary>
        public void DrainAll()
        {
            while (_items.TryDequeue(out var item))
            {
                item.Execute();
            }
        }
    }

    /// <summary>
    /// Async disposable test resource that invokes a callback when disposed.
    /// </summary>
    /// <param name="onDispose">The callback invoked during disposal.</param>
    private sealed class TestAsyncResource(Action onDispose) : IAsyncDisposable
    {
        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            onDispose();
            return default;
        }
    }
}
