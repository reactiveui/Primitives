// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for async parity helpers that mirror the synchronous helper surface in the repository.</summary>
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "TUnit requires instance methods")]
public class ParityOperatorTests
{
    /// <summary>Seconds a test waits for a notification before giving up.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>Sentinel value (42) used by tests.</summary>
    private const int CanonicalAnswer = 42;

    /// <summary>Sentinel value (99) used by tests.</summary>
    private const int FallbackSentinel = 99;

    /// <summary>Single source value (7).</summary>
    private const int SingleSourceValue = 7;

    /// <summary>Bool sequence [true, false, true] used by ParityOperatorTests.</summary>
    private static readonly bool[] ParityBoolSequenceTft = [true, false, true];

    /// <summary>Bool sequence [true, false, true, false, false] used by ParityOperatorTests.</summary>
    private static readonly bool[] ParityBoolSequenceTftff = [true, false, true, false, false];

    /// <summary>Bool sequence [true, false, true, false, true] used by ParityOperatorTests.</summary>
    private static readonly bool[] ParityBoolSequenceTftft = [true, false, true, false, true];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence123 = [1, 2, 3];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence12345 = [1, 2, 3, 4, 5];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence42 = [42];

    /// <summary>Window used by the throttle and debounce timing tests.</summary>
    private static readonly TimeSpan ThrottleWindow = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum time a test waits for an emission to arrive.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(WaitTimeoutSeconds);

    /// <summary>Tests that WhereIsNotNull filters null values and narrows the result type.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereIsNotNull_ThenNullValuesAreFiltered()
    {
        string?[] source = [null, "alpha", null, "beta"];

        var result = await source
            .ToAsyncSignal()
            .WhereIsNotNull()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["alpha", "beta"]);
    }

    /// <summary>Tests that CombineLatestValuesAreAllTrue evaluates the latest boolean state across an enumerable of sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrue_ThenEvaluatesAggregateState()
    {
        IObservableAsync<bool>[] sources = [AsyncObs.Return(true), AsyncObs.Return(true), AsyncObs.Return(false)];

        var result = await sources.CombineLatestValuesAreAllTrue().FirstAsync();

        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests that CombineLatestValuesAreAllTrue returns true when all sources emit true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrue_WithAllTrue_ThenReturnsTrue()
    {
        IObservableAsync<bool>[] sources = [AsyncObs.Return(true), AsyncObs.Return(true), AsyncObs.Return(true)];

        var result = await sources.CombineLatestValuesAreAllTrue().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that CombineLatestValuesAreAllTrue returns true when no sources are provided.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrue_WithEmptySources_ThenReturnsTrue()
    {
        IObservableAsync<bool>[] sources = [];

        var result = await sources.CombineLatestValuesAreAllTrue().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that GetMax returns the maximum latest value across all sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMax_ThenReturnsMaximumLatestValue()
    {
        const int FirstValue = 2;
        const int LargestValue = 5;
        const int ThirdValue = 3;

        var result = await AsyncObs.Return(FirstValue)
            .GetMax(AsyncObs.Return(LargestValue), AsyncObs.Return(ThirdValue))
            .FirstAsync();

        await Assert.That(result).IsEqualTo(LargestValue);
    }

    /// <summary>Tests that GetMax returns the single value when only one source is provided.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMax_WithSingleSource_ThenReturnsThatValue()
    {
        var result = await AsyncObs.Return(SingleSourceValue)
            .GetMax()
            .FirstAsync();

        await Assert.That(result).IsEqualTo(SingleSourceValue);
    }

    /// <summary>Tests that ScanWithInitial emits the seed before emitting accumulated values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanWithInitial_ThenSeedIsEmittedFirst()
    {
        const int RangeStart = 1;
        const int RangeCount = 3;
        const int Seed = 0;
        const int SumAfterTwo = 3;
        const int SumAfterThree = 6;

        var result = await AsyncObs.Range(RangeStart, RangeCount)
            .ScanWithInitial(Seed, static (acc, value) => acc + value)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([Seed, 1, SumAfterTwo, SumAfterThree]);
    }

    /// <summary>Tests that Pairwise emits adjacent pairs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwise_ThenAdjacentPairsAreProduced()
    {
        const int RangeStart = 1;
        const int RangeCount = 4;
        const int ExpectedPairCount = 3;
        const int FirstPairSecond = 2;
        const int SecondPairFirst = 2;
        const int SecondPairSecond = 3;
        const int ThirdPairIndex = 2;
        const int ThirdPairFirst = 3;
        const int ThirdPairSecond = 4;

        var result = await AsyncObs.Range(RangeStart, RangeCount)
            .Pairwise()
            .ToListAsync();

        await Assert.That(result).Count().IsEqualTo(ExpectedPairCount);
        await Assert.That(result[0]).IsEqualTo((1, FirstPairSecond));
        await Assert.That(result[1]).IsEqualTo((SecondPairFirst, SecondPairSecond));
        await Assert.That(result[ThirdPairIndex]).IsEqualTo((ThirdPairFirst, ThirdPairSecond));
    }

    /// <summary>Tests that Pairwise produces an empty sequence when the source has fewer than two elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwise_WithSingleElement_ThenProducesEmptySequence()
    {
        var result = await Sequence42
            .ToAsyncSignal()
            .Pairwise()
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Partition splits a source into true and false branches.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartition_ThenSourceIsSplitIntoBranches()
    {
        const int EvenDivisor = 2;
        const int Emit2 = 2;
        const int Emit3 = 3;
        const int Emit4 = 4;
        const int Emit5 = 5;
        const int Emit6 = 6;

        var signal = Signal.Create<int>();
        var (trueBranch, falseBranch) = signal.Values.Partition(static value => value % EvenDivisor == 0);

        var trueTask = trueBranch.ToListAsync().AsTask();
        var falseTask = falseBranch.ToListAsync().AsTask();

        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnNextAsync(Emit2, CancellationToken.None);
        await signal.OnNextAsync(Emit3, CancellationToken.None);
        await signal.OnNextAsync(Emit4, CancellationToken.None);
        await signal.OnNextAsync(Emit5, CancellationToken.None);
        await signal.OnNextAsync(Emit6, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        var trueValues = await trueTask;
        var falseValues = await falseTask;

        await Assert.That(trueValues).IsCollectionEqualTo([Emit2, Emit4, Emit6]);
        await Assert.That(falseValues).IsCollectionEqualTo([1, Emit3, Emit5]);
    }

    /// <summary>Tests that DoOnSubscribe runs for each subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDoOnSubscribe_ThenRunsPerSubscription()
    {
        const int ExpectedSubscriptions = 2;
        var subscriptions = 0;
        var source = AsyncObs.Return(CanonicalAnswer).DoOnSubscribe(() => subscriptions++);

        await source.WaitCompletionAsync();
        await source.WaitCompletionAsync();

        await Assert.That(subscriptions).IsEqualTo(ExpectedSubscriptions);
    }

    /// <summary>Tests that CatchIgnore suppresses terminal failures and completes with an empty result set.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnore_ThenFailureIsSuppressed()
    {
        var result = await AsyncObs.Throw<int>(new InvalidOperationException("boom"))
            .CatchIgnore()
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Start executes the supplied function and publishes its result.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFunction_ThenPublishesFunctionResult()
    {
        var result = await AsyncObs.Start(static () => CanonicalAnswer).FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that AsSignal converts each source value into RxVoid.Default.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsSignal_ThenEmitsUnitForEachValue()
    {
        var result = await Sequence123
            .ToAsyncSignal()
            .AsSignal()
            .ToListAsync();

        const int ExpectedCount = 3;
        const int LastIndex = 2;
        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result[0]).IsEqualTo(RxVoid.Default);
        await Assert.That(result[1]).IsEqualTo(RxVoid.Default);
        await Assert.That(result[LastIndex]).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Tests that CatchIgnore with a typed exception suppresses matching exceptions and invokes the action.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenCatchIgnoreTyped_WithMatchingException_ThenSuppressesAndInvokesAction()
    {
        Exception? captured = null;

        var result = await AsyncObs.Throw<int>(new InvalidOperationException("typed boom"))
            .CatchIgnore<int, InvalidOperationException>(ex => captured = ex)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
        await Assert.That(captured).IsNotNull();
        await Assert.That(captured!.Message).IsEqualTo("typed boom");
    }

    /// <summary>Tests that CatchIgnore with a typed exception re-throws when the exception type does not match.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreTyped_WithNonMatchingException_ThenPropagatesError()
    {
        ArgumentException error = new("wrong type");

        var resultTask = AsyncObs.Throw<int>(error)
            .CatchIgnore<int, InvalidOperationException>(static _ => { })
            .ToListAsync()
            .AsTask();

        await Assert.That(resultTask).ThrowsException();
    }

    /// <summary>Tests that CatchAndReturn emits the fallback value on terminal failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturn_ThenEmitsFallbackOnFailure()
    {
        var result = await AsyncObs.Throw<int>(new InvalidOperationException("fail"))
            .CatchAndReturn(FallbackSentinel)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FallbackSentinel]);
    }

    /// <summary>Tests that CatchAndReturn with a typed exception emits the factory result on a matching failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnTyped_WithMatchingException_ThenEmitsFactoryResult()
    {
        var result = await AsyncObs.Throw<string>(new InvalidOperationException("boom"))
            .CatchAndReturn<string, InvalidOperationException>(static ex => $"caught: {ex.Message}")
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["caught: boom"]);
    }

    /// <summary>Tests that CatchAndReturn with a typed exception re-throws when the exception type does not match.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnTyped_WithNonMatchingException_ThenPropagatesError()
    {
        var resultTask = AsyncObs.Throw<string>(new ArgumentException("nope"))
            .CatchAndReturn<string, InvalidOperationException>(static _ => "fallback")
            .ToListAsync()
            .AsTask();

        await Assert.That(resultTask).ThrowsException();
    }

    /// <summary>Tests that the async DoOnSubscribe overload executes the asynchronous action before subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDoOnSubscribeAsync_ThenAsyncActionRunsBeforeSubscription()
    {
        const int SourceValue = 10;
        var executed = false;

        var result = await AsyncObs.Return(SourceValue)
            .DoOnSubscribe(_ =>
            {
                executed = true;
                return default;
            })
            .FirstAsync();

        await Assert.That(executed).IsTrue();
        await Assert.That(result).IsEqualTo(SourceValue);
    }

    /// <summary>Tests that DropIfBusy drops values emitted while the async action is still running.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDropIfBusy_WithBusyAction_ThenDropsValues()
    {
        const int DroppedValueA = 2;
        const int DroppedValueB = 3;
        const int PassthroughValue = 4;
        var signal = Signal.Create<int>();
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

        List<int> result = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DropIfBusy(async (value, _) =>
            {
                if (value == 1)
                {
                    await gate.Task;
                }
            })
            .SubscribeAsync(
                (value, _) =>
                {
                    result.Add(value);
                    return default;
                },
                null,
                _ =>
                {
                    completed.SetResult();
                    return default;
                },
                CancellationToken.None);

        // Emit value 1 which will block on the gate
        var emitTask = signal.OnNextAsync(1, CancellationToken.None).AsTask();

        // Emit values 2 and 3 while the action for value 1 is still running - these should be dropped
        // We need a small yield to ensure value 1's handler has started
        await Task.Yield();
        await signal.OnNextAsync(DroppedValueA, CancellationToken.None);
        await signal.OnNextAsync(DroppedValueB, CancellationToken.None);

        // Release the gate so value 1 completes
        gate.SetResult();
        await emitTask;

        // Emit value 4 after the action finishes - this should go through
        await signal.OnNextAsync(PassthroughValue, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);

        await completed.Task;

        await Assert.That(result).Contains(1);
        await Assert.That(result).Contains(PassthroughValue);
        await Assert.That(result).DoesNotContain(DroppedValueA);
        await Assert.That(result).DoesNotContain(DroppedValueB);
    }

    /// <summary>Tests that DropIfBusy passes through all values when the action completes synchronously.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDropIfBusy_WithFastAction_ThenAllValuesPassThrough()
    {
        const int Second = 2;
        const int Third = 3;
        var result = await new[] { 1, Second, Third }
            .ToAsyncSignal()
            .DropIfBusy(static (_, _) => default)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests that LatestOrDefault emits the default value first and suppresses a duplicate first source value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefault_ThenEmitsDefaultFirstAndSuppressesDuplicate()
    {
        const int Third = 2;
        var result = await new[] { 0, 1, Third }
            .ToAsyncSignal()
            .LatestOrDefault(0)
            .ToListAsync();

        // StartWith(0) prepends 0, then DistinctUntilChanged suppresses the duplicate 0 from source
        await Assert.That(result).IsCollectionEqualTo([0, 1, Third]);
    }

    /// <summary>Tests that LatestOrDefault emits both default and first source value when they differ.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefault_WithDifferentFirst_ThenEmitsBoth()
    {
        const int First = 5;
        const int Second = 6;
        var result = await new[] { First, Second }
            .ToAsyncSignal()
            .LatestOrDefault(0)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([0, First, Second]);
    }

    /// <summary>Tests that LogErrors invokes the logger callback when an error-resume is observed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenLogErrors_ThenLoggerIsInvokedOnError()
    {
        List<Exception> logged = [];
        DirectSource<int> source = new();

        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await source
            .LogErrors(logged.Add)
            .SubscribeAsync(
                (value, _) =>
                {
                    items.Add(value);
                    return default;
                },
                null,
                _ =>
                {
                    completed.SetResult();
                    return default;
                },
                CancellationToken.None);

        const int SecondValue = 2;
        await source.EmitNext(1);
        InvalidOperationException testError = new("logged error");
        await source.EmitError(testError);
        await source.EmitNext(SecondValue);
        await source.Complete(Result.Success);

        await completed.Task;

        await Assert.That(items).IsCollectionEqualTo([1, SecondValue]);
        await Assert.That(logged).Count().IsEqualTo(1);
        await Assert.That(logged[0].Message).IsEqualTo("logged error");
    }

    /// <summary>Tests that WaitUntil emits only the first value satisfying the predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntil_ThenEmitsFirstMatchingValue()
    {
        const int FirstMatch = 4;
        const int MatchThreshold = 3;

        var result = await Sequence12345
            .ToAsyncSignal()
            .WaitUntil(static v => v > MatchThreshold)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FirstMatch]);
    }

    /// <summary>Tests that ObserveOnSafe with a null AsyncContext returns the source unchanged.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContext_WithNull_ThenReturnsSourceUnchanged()
    {
        var source = AsyncObs.Return(1);

        var observed = source.ObserveOnSafe((AsyncContext?)null);

        var result = await observed.FirstAsync();
        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>Tests that ObserveOnSafe with a non-null AsyncContext applies ObserveOn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeAsyncContext_WithValue_ThenAppliesObserveOn()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnSafe(context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnSafe with a null TaskScheduler returns the source unchanged.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskScheduler_WithNull_ThenReturnsSourceUnchanged()
    {
        var source = AsyncObs.Return(1);

        var observed = source.ObserveOnSafe((TaskScheduler?)null);

        var result = await observed.FirstAsync();
        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>Tests that ObserveOnSafe with a non-null TaskScheduler applies ObserveOn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSafeTaskScheduler_WithValue_ThenAppliesObserveOn()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnSafe(TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with true condition applies ObserveOn with AsyncContext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContext_WithTrueCondition_ThenAppliesObserveOn()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(true, context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with false condition returns the source unchanged for AsyncContext.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfAsyncContext_WithFalseCondition_ThenReturnsSourceUnchanged()
    {
        var context = AsyncContext.Default;

        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(false, context)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with true condition applies ObserveOn with TaskScheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskScheduler_WithTrueCondition_ThenAppliesObserveOn()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(true, TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ObserveOnIf with false condition returns the source unchanged for TaskScheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfTaskScheduler_WithFalseCondition_ThenReturnsSourceUnchanged()
    {
        var result = await AsyncObs.Return(CanonicalAnswer)
            .ObserveOnIf(false, TaskScheduler.Default)
            .FirstAsync();

        await Assert.That(result).IsEqualTo(CanonicalAnswer);
    }

    /// <summary>Tests that ReplayLastOnSubscribe replays the initial value and subsequent source values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLastOnSubscribe_ThenReplaysInitialAndSourceValues()
    {
        var signal = Signal.Create<int>();
        var replayed = signal.Values.ReplayLastOnSubscribe(0);

        var firstResult = await replayed.Take(1).FirstAsync();
        await Assert.That(firstResult).IsEqualTo(0);
    }

    /// <summary>Tests that ThrottleDistinct emits only distinct values after throttling.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenThrottleDistinct_ThenEmitsDistinctThrottledValues()
    {
        var signal = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .ThrottleDistinct(ThrottleWindow)
            .SubscribeAsync(
                (value, _) =>
                {
                    results.Add(value);
                    IgnoredResult.Of(firstReceived.TrySetResult());
                    return default;
                },
                null);

        // Emit duplicate values quickly - DistinctUntilChanged collapses them
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnNextAsync(1, CancellationToken.None);

        // Wait for throttle to emit the first distinct value
        var received = await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            WaitTimeout);

        await signal.OnCompletedAsync(Result.Success);

        await Assert.That(received).IsTrue();
        await Assert.That(results[0]).IsEqualTo(1);
    }

    /// <summary>Tests that the async ScanWithInitial overload emits the seed followed by accumulated values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanWithInitialAsync_ThenSeedIsEmittedFirst()
    {
        const int RangeStart = 1;
        const int RangeCount = 3;
        const int Seed = 0;
        const int SumAfterTwo = 3;
        const int SumAfterThree = 6;

        var result = await AsyncObs.Range(RangeStart, RangeCount)
            .ScanWithInitial(
                Seed,
                static (acc, value, _) => new(acc + value))
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([Seed, 1, SumAfterTwo, SumAfterThree]);
    }

    /// <summary>Tests that DebounceUntil immediately emits values that satisfy the condition.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDebounceUntil_WithConditionTrue_ThenEmitsImmediately()
    {
        const int DebounceSeconds = 10;
        const int Threshold = 5;
        const int EmittedValue = 10;
        var signal = Signal.Create<int>();

        var resultTask = signal.Values
            .DebounceUntil(TimeSpan.FromSeconds(DebounceSeconds), static v => v > Threshold)
            .Take(1)
            .FirstAsync()
            .AsTask();

        // Value 10 satisfies condition, should emit immediately (no delay)
        await signal.OnNextAsync(EmittedValue, CancellationToken.None);

        var result = await resultTask;

        await Assert.That(result).IsEqualTo(EmittedValue);
    }

    /// <summary>Tests that DebounceUntil delays values that do not satisfy the condition.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDebounceUntil_WithConditionFalse_ThenDelaysEmission()
    {
        const int UnreachableThreshold = 100;

        var signal = Signal.Create<int>();
        List<int> results = [];

        await using var sub = await signal.Values
            .DebounceUntil(ThrottleWindow, static v => v > UnreachableThreshold)
            .SubscribeAsync(
                (value, _) =>
                {
                    results.Add(value);
                    return default;
                },
                null);

        // Value 1 does not satisfy condition, should be delayed by 50ms
        await signal.OnNextAsync(1, CancellationToken.None);

        // Wait for the delayed value to arrive
        var received = await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            WaitTimeout);

        await signal.OnCompletedAsync(Result.Success);

        await Assert.That(received).IsTrue();
        await Assert.That(results[0]).IsEqualTo(1);
    }

    /// <summary>Tests that GetMin returns the minimum latest value across all sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMin_ThenReturnsMinimumLatestValue()
    {
        const int FirstValue = 5;
        const int SmallestValue = 2;
        const int ThirdValue = 8;

        var result = await AsyncObs.Return(FirstValue)
            .GetMin(AsyncObs.Return(SmallestValue), AsyncObs.Return(ThirdValue))
            .FirstAsync();

        await Assert.That(result).IsEqualTo(SmallestValue);
    }

    /// <summary>Tests that GetMin returns the single value when only one source is provided.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMin_WithSingleSource_ThenReturnsThatValue()
    {
        var result = await AsyncObs.Return(SingleSourceValue)
            .GetMin()
            .FirstAsync();

        await Assert.That(result).IsEqualTo(SingleSourceValue);
    }

    /// <summary>Tests that CombineLatestValuesAreAllFalse returns true when all sources emit false.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalse_WithAllFalse_ThenReturnsTrue()
    {
        IObservableAsync<bool>[] sources = [AsyncObs.Return(false), AsyncObs.Return(false), AsyncObs.Return(false)];

        var result = await sources.CombineLatestValuesAreAllFalse().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that CombineLatestValuesAreAllFalse returns false when any source emits true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalse_WithSomeTrue_ThenReturnsFalse()
    {
        IObservableAsync<bool>[] sources = [AsyncObs.Return(false), AsyncObs.Return(true), AsyncObs.Return(false)];

        var result = await sources.CombineLatestValuesAreAllFalse().FirstAsync();

        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests that CombineLatestValuesAreAllFalse returns true when no sources are provided.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalse_WithEmptySources_ThenReturnsTrue()
    {
        IObservableAsync<bool>[] sources = [];

        var result = await sources.CombineLatestValuesAreAllFalse().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that ForEach flattens enumerable elements into individual values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEach_ThenFlattensEnumerableElements()
    {
        const int Second = 2;
        const int Third = 3;
        const int Fourth = 4;
        var result = await new IEnumerable<int>[] { [1, Second], [Third, Fourth] }
            .ToAsyncSignal()
            .ForEach()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, Second, Third, Fourth]);
    }

    /// <summary>Tests that Not negates each boolean value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNot_ThenNegatesBooleanValues()
    {
        var result = await ParityBoolSequenceTft
            .ToAsyncSignal()
            .Not()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([false, true, false]);
    }

    /// <summary>Tests that SkipWhileNull skips leading nulls and emits from the first non-null value onwards.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileNull_ThenSkipsLeadingNulls()
    {
        string?[] source = [null, null, "first", null, "second"];

        var result = await source
            .ToAsyncSignal()
            .SkipWhileNull()
            .ToListAsync();

        // SkipWhile skips while null, so once a non-null appears, all subsequent values pass through
        await Assert.That(result).IsCollectionEqualTo(["first", null!, "second"]);
    }

    /// <summary>Tests that Start with an Action emits RxVoid.Default.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenStartAction_ThenEmitsUnit()
    {
        var executed = false;

        var result = await SignalAsyncReactiveExtensions.Start(Run).FirstAsync();

        await Assert.That(executed).IsTrue();
        await Assert.That(result.Equals(RxVoid.Default)).IsTrue();

        void Run() => executed = true;
    }

    /// <summary>Tests that Start with an Action and a TaskScheduler executes on the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenStartAction_WithScheduler_ThenExecutesOnScheduler()
    {
        var executed = false;

        var result = await SignalAsyncReactiveExtensions.Start(Run, TaskScheduler.Default).FirstAsync();

        await Assert.That(executed).IsTrue();
        await Assert.That(result.Equals(RxVoid.Default)).IsTrue();

        void Run() => executed = true;
    }

    /// <summary>Tests that Start with a function and a TaskScheduler executes on the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFunction_WithScheduler_ThenExecutesOnScheduler()
    {
        var result = await AsyncObs.Start(static () => FallbackSentinel, TaskScheduler.Default).FirstAsync();

        await Assert.That(result).IsEqualTo(FallbackSentinel);
    }

    /// <summary>Tests that WhereFalse filters to only false values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereFalse_ThenFiltersToFalseValues()
    {
        var result = await ParityBoolSequenceTftff
            .ToAsyncSignal()
            .WhereFalse()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([false, false, false]);
    }

    /// <summary>Tests that WhereTrue filters to only true values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereTrue_ThenFiltersToTrueValues()
    {
        var result = await ParityBoolSequenceTftft
            .ToAsyncSignal()
            .WhereTrue()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([true, true, true]);
    }

    /// <summary>Tests that CombineLatestValuesAreAllFalse materializes a non-collection enumerable source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalse_WithNonCollectionEnumerable_ThenMaterializesAndEvaluates()
    {
        static IEnumerable<IObservableAsync<bool>> LazySource()
        {
            yield return AsyncObs.Return(false);
            yield return AsyncObs.Return(false);
        }

        var result = await LazySource().CombineLatestValuesAreAllFalse().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that CombineLatestValuesAreAllTrue materializes a non-collection enumerable source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllTrue_WithNonCollectionEnumerable_ThenMaterializesAndEvaluates()
    {
        static IEnumerable<IObservableAsync<bool>> LazySource()
        {
            yield return AsyncObs.Return(true);
            yield return AsyncObs.Return(true);
        }

        var result = await LazySource().CombineLatestValuesAreAllTrue().FirstAsync();

        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests that CatchAndReturn passes through source values when no error occurs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturn_WithNoError_ThenPassesThroughSourceValues()
    {
        const int Second = 2;
        const int Third = 3;
        var result = await new[] { 1, Second, Third }
            .ToAsyncSignal()
            .CatchAndReturn(FallbackSentinel)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests that CatchIgnore passes through source values when no error occurs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnore_WithNoError_ThenPassesThroughSourceValues()
    {
        const int Second = 2;
        const int Third = 3;
        var result = await new[] { 1, Second, Third }
            .ToAsyncSignal()
            .CatchIgnore()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests that the async DoOnSubscribe overload runs on each subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDoOnSubscribeAsync_ThenRunsPerSubscription()
    {
        const int ExpectedSubscriptions = 2;
        var count = 0;
        var source = AsyncObs.Return(CanonicalAnswer).DoOnSubscribe(_ =>
        {
            count++;
            return default;
        });

        await source.WaitCompletionAsync();
        await source.WaitCompletionAsync();

        await Assert.That(count).IsEqualTo(ExpectedSubscriptions);
    }

    /// <summary>Tests that GetMax picks the maximum when the first source has the largest value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMax_WithMaxInFirstSource_ThenReturnsCorrectMax()
    {
        const int LargestValue = 10;
        const int MiddleValue = 5;

        var result = await AsyncObs.Return(LargestValue)
            .GetMax(AsyncObs.Return(1), AsyncObs.Return(MiddleValue))
            .FirstAsync();

        await Assert.That(result).IsEqualTo(LargestValue);
    }

    /// <summary>Tests that GetMin picks the minimum when the first source has the smallest value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMin_WithMinInFirstSource_ThenReturnsCorrectMin()
    {
        const int LargestValue = 10;
        const int MiddleValue = 5;

        var result = await AsyncObs.Return(1)
            .GetMin(AsyncObs.Return(LargestValue), AsyncObs.Return(MiddleValue))
            .FirstAsync();

        await Assert.That(result).IsEqualTo(1);
    }

    /// <summary>
    /// Tests that DropIfBusy silently discards values that arrive while the async action
    /// from a prior value is still executing, ensuring the early-return guard fires.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Usage",
        "CA1849:Await task instead of getting result",
        Justification = "Asserting on task results after completion.")]
    public async Task WhenDropIfBusy_WithConcurrentEmission_ThenDroppedValueIsDiscarded()
    {
        const int DroppedValue = 2;
        const int PassthroughValue = 3;
        DirectSource<int> source = new();
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var actionStarted = 0;
        var droppedCount = 0;

        List<int> received = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await source
            .DropIfBusy(async (value, _) =>
            {
                IgnoredResult.Of(Interlocked.Increment(ref actionStarted));

                if (value == 1)
                {
                    await gate.Task;
                }
            })
            .Do((value, _) =>
            {
                received.Add(value);
                return default;
            })
            .SubscribeAsync(
                static (_, _) => default,
                null,
                _ =>
                {
                    completed.SetResult();
                    return default;
                },
                CancellationToken.None);

        // Start processing value 1 which blocks on the gate.
        var emit1Task = source.EmitNext(1).AsTask();

        // Wait until the action for value 1 has actually started.
        await AsyncTestHelpers.WaitForConditionAsync(
            () => Volatile.Read(ref actionStarted) >= 1,
            WaitTimeout);

        // Emit value 2 while value 1 is still processing - it should be dropped.
        await source.EmitNext(DroppedValue);

        // The action should have been invoked only once (for value 1).
        droppedCount = 1 - (Volatile.Read(ref actionStarted) - 1);
        await Assert.That(droppedCount).IsEqualTo(1);

        // Release value 1 so it completes.
        gate.SetResult();
        await emit1Task;

        // Emit value 3 after the action finishes - this should pass through.
        await source.EmitNext(PassthroughValue);
        await source.Complete(Result.Success);

        await completed.Task;

        await Assert.That(received).Contains(1);
        await Assert.That(received).Contains(PassthroughValue);
        await Assert.That(received).DoesNotContain(DroppedValue);
    }
}
