// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Ported coverage for the migrated synchronous extension operators using primitives runtime types.
/// </summary>
public sealed class ReactiveExtensionsPortedTests
{
    /// <summary>Candidate keys probed by the first-match test.</summary>
    private static readonly int[] MatchCandidates = [1, 2, 3];

    /// <summary>Verifies signal data wrappers keep their update/signal semantics.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DataWrappersExposeUpdateAndSignalState()
    {
        const int HeartbeatUpdate = 42;
        const int StaleUpdate = 7;

        var heartbeat = new Heartbeat<int>();
        var heartbeatUpdate = new Heartbeat<int>(HeartbeatUpdate);
        var stale = new Stale<int>();
        var staleUpdate = new Stale<int>(StaleUpdate);

        await Assert.That(heartbeat.IsHeartbeat).IsTrue();
        await Assert.That(heartbeatUpdate.Update).IsEqualTo(HeartbeatUpdate);
        await Assert.That(stale.IsStale).IsTrue();
        await Assert.That(staleUpdate.Update).IsEqualTo(StaleUpdate);
        Assert.Throws<InvalidOperationException>(() => _ = stale.Update);
    }

    /// <summary>Verifies null filtering, signal projection, and boolean helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task BasicProjectionAndBooleanOperatorsEmitExpectedValues()
    {
        var nullable = new Subject<string?>();
        var notNull = new List<string>();
        using var nonNullSub = nullable.WhereIsNotNull().Subscribe(value => notNull.Add(value!));
        nullable.OnNext(null);
        nullable.OnNext("value");

        var signalValues = new List<RxVoid>();
        using var signalSub = Observable.Return(1).AsSignal().Subscribe(signalValues.Add);

        var bools = new Subject<bool>();
        var notValues = new List<bool>();
        var trueValues = new List<bool>();
        var falseValues = new List<bool>();
        using var notSub = bools.Not().Subscribe(notValues.Add);
        using var trueSub = bools.WhereTrue().Subscribe(trueValues.Add);
        using var falseSub = bools.WhereFalse().Subscribe(falseValues.Add);
        bools.OnNext(true);
        bools.OnNext(false);

        await Assert.That(notNull).IsCollectionEqualTo(["value"]);
        await Assert.That(signalValues.Count).IsEqualTo(1);
        await Assert.That(signalValues[0]).IsEqualTo(RxVoid.Default);
        await Assert.That(notValues).IsCollectionEqualTo([false, true]);
        await Assert.That(trueValues).IsCollectionEqualTo([true]);
        await Assert.That(falseValues).IsCollectionEqualTo([false]);
    }

    /// <summary>Verifies buffering and combining helpers copied from ReactiveUI.Extensions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task BufferAndCombineHelpersPreserveLegacyBehavior()
    {
        const int MaxAInitial = 1;
        const int MaxBInitial = 5;
        const int MaxAUpdate = 9;

        var chars = new Subject<char>();
        var frames = new List<string>();
        using var frameSub = chars.BufferUntil('[', ']').Subscribe(frames.Add);
        foreach (var value in "x[abc]y")
        {
            chars.OnNext(value);
        }

        var first = new BehaviorSubject<bool>(false);
        var second = new BehaviorSubject<bool>(false);
        var allFalse = new List<bool>();
        using var allFalseSub = new[] { first, second }.CombineLatestValuesAreAllFalse().Subscribe(allFalse.Add);
        first.OnNext(true);

        var maxA = new BehaviorSubject<int>(MaxAInitial);
        var maxB = new BehaviorSubject<int>(MaxBInitial);
        var maxValues = new List<int>();
        using var maxSub = maxA.GetMax(maxB).Subscribe(maxValues.Add);
        maxA.OnNext(MaxAUpdate);

        await Assert.That(frames).IsCollectionEqualTo(["[abc]"]);
        await Assert.That(allFalse).IsCollectionEqualTo([true, false]);
        await Assert.That(maxValues).IsCollectionEqualTo([MaxBInitial, MaxAUpdate]);
    }

    /// <summary>Verifies virtual-time operators use <see cref="ISequencer"/> instead of Rx schedulers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task TimeBasedOperatorsUsePrimitiveSequencer()
    {
        const int FirstValue = 1;
        const int SecondValue = 2;
        const int StaleWindowSeconds = 2;
        const int ExpectedStaleCount = 3;
        const int ThirdEmissionIndex = 2;

        var clock = new VirtualClock();
        var source = new Subject<int>();
        var batches = new List<IList<int>>();
        var stale = new List<Stale<int>>();
        using var bufferSub = source.BufferUntilInactive(TimeSpan.FromSeconds(1), clock).Subscribe(batches.Add);
        using var staleSub = source.DetectStale(TimeSpan.FromSeconds(StaleWindowSeconds), clock).Subscribe(stale.Add);

        source.OnNext(FirstValue);
        source.OnNext(SecondValue);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await Assert.That(batches.Count).IsEqualTo(1);
        await Assert.That(batches[0]).IsCollectionEqualTo([FirstValue, SecondValue]);
        await Assert.That(stale.Count).IsEqualTo(ExpectedStaleCount);
        await Assert.That(stale[0].Update).IsEqualTo(FirstValue);
        await Assert.That(stale[1].Update).IsEqualTo(SecondValue);
        await Assert.That(stale[ThirdEmissionIndex].IsStale).IsTrue();
    }

    /// <summary>Verifies scheduling and throttling helpers use primitive clocks.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SchedulingOperatorsUsePrimitiveSequencer()
    {
        const int ScheduledValue = 5;
        const int FirstThrottled = 1;
        const int SuppressedThrottled = 2;
        const int EmittedThrottled = 3;

        var clock = new VirtualClock();
        var scheduled = new List<int>();
        using var scheduledSub = ScheduledValue.Schedule(TimeSpan.FromSeconds(1), clock).Subscribe(scheduled.Add);

        var throttledSource = new Subject<int>();
        var throttled = new List<int>();
        using var throttleSub = throttledSource.ThrottleFirst(TimeSpan.FromSeconds(1), clock).Subscribe(throttled.Add);
        throttledSource.OnNext(FirstThrottled);
        throttledSource.OnNext(SuppressedThrottled);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        throttledSource.OnNext(EmittedThrottled);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await Assert.That(scheduled).IsCollectionEqualTo([ScheduledValue]);
        await Assert.That(throttled).IsCollectionEqualTo([FirstThrottled, EmittedThrottled]);
    }

    /// <summary>Verifies fused projection/filter helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FusedProjectionOperatorsEmitExpectedValues()
    {
        const int EvenDivisor = 2;
        const int OddInput = 1;
        const int EvenInput = 2;

        var values = new Subject<int>();
        var whereSelect = new List<string>();
        var trySelect = new List<string>();
        var constants = new List<string>();

        using var whereSelectSub = values.WhereSelect(x => x % EvenDivisor == 0, x => $"even-{x}").Subscribe(whereSelect.Add);
        using var trySelectSub = values.TrySelect(x => x > OddInput ? $"value-{x}" : null).Subscribe(trySelect.Add);
        using var constantsSub = values.SelectConstant("tick").Subscribe(constants.Add);
        values.OnNext(OddInput);
        values.OnNext(EvenInput);

        await Assert.That(whereSelect).IsCollectionEqualTo(["even-2"]);
        await Assert.That(trySelect).IsCollectionEqualTo(["value-2"]);
        await Assert.That(constants).IsCollectionEqualTo(["tick", "tick"]);
    }

    /// <summary>Verifies catch/fallback operators preserve their terminal behavior.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ErrorFallbackOperatorsHandleFailures()
    {
        const int CatchReturnValue = 7;

        var catchReturn = new List<int>();
        var catchIgnoreCompleted = false;
        Exception? caught = null;

        using var returnSub = Observable.Throw<int>(new InvalidOperationException())
            .CatchReturn(CatchReturnValue)
            .Subscribe(catchReturn.Add);

        using var ignoreSub = Observable.Throw<int>(new InvalidOperationException("handled"))
            .CatchIgnore<int, InvalidOperationException>(ex => caught = ex)
            .Subscribe(static _ => { }, () => catchIgnoreCompleted = true);

        await Assert.That(catchReturn).IsCollectionEqualTo([CatchReturnValue]);
        await Assert.That(caught).IsNotNull();
        await Assert.That(catchIgnoreCompleted).IsTrue();
    }

    /// <summary>Verifies latest, replay, pairwise, partition, sample, and switch helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task StateAndRoutingOperatorsEmitExpectedValues()
    {
        const int DefaultValue = -1;
        const int PartitionDivisor = 2;
        const int FirstValue = 1;
        const int SecondValue = 2;
        const int FallbackValue = 99;

        var source = new Subject<int>();
        var latest = new List<int>();
        var pairwise = new List<(int Previous, int Current)>();
        var even = new List<int>();
        var odd = new List<int>();
        var sampled = new List<int>();
        var trigger = new Subject<object>();

        using var latestSub = source.LatestOrDefault(DefaultValue).Subscribe(latest.Add);
        using var pairwiseSub = source.Pairwise().Subscribe(pairwise.Add);
        var (truePartition, falsePartition) = source.Partition(x => x % PartitionDivisor == 0);
        using var evenSub = truePartition.Subscribe(even.Add);
        using var oddSub = falsePartition.Subscribe(odd.Add);
        using var sampledSub = source.SampleLatest(trigger).Subscribe(sampled.Add);

        source.OnNext(FirstValue);
        trigger.OnNext(new object());
        source.OnNext(SecondValue);
        trigger.OnNext(new object());

        var switched = new List<int>();
        using var switchSub = Observable.Empty<int>().SwitchIfEmpty(Observable.Return(FallbackValue)).Subscribe(switched.Add);

        await Assert.That(latest).IsCollectionEqualTo([DefaultValue, FirstValue, SecondValue]);
        await Assert.That(pairwise).IsCollectionEqualTo([(FirstValue, SecondValue)]);
        await Assert.That(even).IsCollectionEqualTo([SecondValue]);
        await Assert.That(odd).IsCollectionEqualTo([FirstValue]);
        await Assert.That(sampled).IsCollectionEqualTo([FirstValue, SecondValue]);
        await Assert.That(switched).IsCollectionEqualTo([FallbackValue]);
    }

    /// <summary>Verifies async projection and sequential run helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsyncAndSequentialHelpersEmitExpectedValues()
    {
        const int InputValue = 2;
        const int SequentialMultiplier = 2;
        const int ConcurrentMultiplier = 3;
        const int MaxConcurrency = 2;
        const int DelayMilliseconds = 50;
        const int SequentialResult = 4;
        const int ConcurrentResult = 6;

        var source = new Subject<int>();
        var sequential = new List<int>();
        var concurrent = new List<int>();
        using var seqSub = source.SelectAsyncSequential(x => Task.FromResult(x * SequentialMultiplier)).Subscribe(sequential.Add);
        using var conSub = source.SelectAsyncConcurrent(x => Task.FromResult(x * ConcurrentMultiplier), maxConcurrency: MaxConcurrency).Subscribe(concurrent.Add);

        source.OnNext(InputValue);
        await Task.Delay(DelayMilliseconds);

        var runAll = new List<RxVoid>();
        using var runAllSub = new[]
        {
            Observable.Return(RxVoid.Default),
            Observable.Return(RxVoid.Default)
        }.RunAll().Subscribe(runAll.Add);

        await Assert.That(sequential).IsCollectionEqualTo([SequentialResult]);
        await Assert.That(concurrent).IsCollectionEqualTo([ConcurrentResult]);
        await Assert.That(runAll).IsCollectionEqualTo([RxVoid.Default]);
    }

    /// <summary>Verifies candidate probing emits the first transformed match.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FirstMatchFromCandidatesEmitsFirstMatch()
    {
        var results = new List<string>();
        using var sub = MatchCandidates
            .FirstMatchFromCandidates(
                key => Observable.Return(key),
                value => $"value-{value}",
                value => value.EndsWith("2", StringComparison.Ordinal),
                "fallback")
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["value-2"]);
    }
}
