// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using System.IO;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Threading.Tasks;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Ported coverage for the migrated synchronous extension operators using primitives runtime types.
/// </summary>
public sealed class ReactiveExtensionsPortedTests
{
    /// <summary>Verifies signal data wrappers keep their update/signal semantics.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DataWrappersExposeUpdateAndSignalState()
    {
        var heartbeat = new Heartbeat<int>();
        var heartbeatUpdate = new Heartbeat<int>(42);
        var stale = new Stale<int>();
        var staleUpdate = new Stale<int>(7);

        await Assert.That(heartbeat.IsHeartbeat).IsTrue();
        await Assert.That(heartbeatUpdate.Update).IsEqualTo(42);
        await Assert.That(stale.IsStale).IsTrue();
        await Assert.That(staleUpdate.Update).IsEqualTo(7);
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

        var maxA = new BehaviorSubject<int>(1);
        var maxB = new BehaviorSubject<int>(5);
        var maxValues = new List<int>();
        using var maxSub = maxA.GetMax(maxB).Subscribe(maxValues.Add);
        maxA.OnNext(9);

        await Assert.That(frames).IsCollectionEqualTo(["[abc]"]);
        await Assert.That(allFalse).IsCollectionEqualTo([true, false]);
        await Assert.That(maxValues).IsCollectionEqualTo([5, 9]);
    }

    /// <summary>Verifies virtual-time operators use <see cref="ISequencer"/> instead of Rx schedulers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task TimeBasedOperatorsUsePrimitiveSequencer()
    {
        var clock = new VirtualClock();
        var source = new Subject<int>();
        var batches = new List<IList<int>>();
        var stale = new List<Stale<int>>();
        using var bufferSub = source.BufferUntilInactive(TimeSpan.FromSeconds(1), clock).Subscribe(batches.Add);
        using var staleSub = source.DetectStale(TimeSpan.FromSeconds(2), clock).Subscribe(stale.Add);

        source.OnNext(1);
        source.OnNext(2);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await Assert.That(batches.Count).IsEqualTo(1);
        await Assert.That(batches[0]).IsCollectionEqualTo([1, 2]);
        await Assert.That(stale.Count).IsEqualTo(3);
        await Assert.That(stale[0].Update).IsEqualTo(1);
        await Assert.That(stale[1].Update).IsEqualTo(2);
        await Assert.That(stale[2].IsStale).IsTrue();
    }

    /// <summary>Verifies scheduling and throttling helpers use primitive clocks.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SchedulingOperatorsUsePrimitiveSequencer()
    {
        var clock = new VirtualClock();
        var scheduled = new List<int>();
        using var scheduledSub = 5.Schedule(TimeSpan.FromSeconds(1), clock).Subscribe(scheduled.Add);

        var throttledSource = new Subject<int>();
        var throttled = new List<int>();
        using var throttleSub = throttledSource.ThrottleFirst(TimeSpan.FromSeconds(1), clock).Subscribe(throttled.Add);
        throttledSource.OnNext(1);
        throttledSource.OnNext(2);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));
        throttledSource.OnNext(3);
        clock.AdvanceBy(TimeSpan.FromSeconds(1));

        await Assert.That(scheduled).IsCollectionEqualTo([5]);
        await Assert.That(throttled).IsCollectionEqualTo([1, 3]);
    }

    /// <summary>Verifies fused projection/filter helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FusedProjectionOperatorsEmitExpectedValues()
    {
        var values = new Subject<int>();
        var whereSelect = new List<string>();
        var trySelect = new List<string>();
        var constants = new List<string>();

        using var whereSelectSub = values.WhereSelect(x => x % 2 == 0, x => $"even-{x}").Subscribe(whereSelect.Add);
        using var trySelectSub = values.TrySelect(x => x > 1 ? $"value-{x}" : null).Subscribe(trySelect.Add);
        using var constantsSub = values.SelectConstant("tick").Subscribe(constants.Add);
        values.OnNext(1);
        values.OnNext(2);

        await Assert.That(whereSelect).IsCollectionEqualTo(["even-2"]);
        await Assert.That(trySelect).IsCollectionEqualTo(["value-2"]);
        await Assert.That(constants).IsCollectionEqualTo(["tick", "tick"]);
    }

    /// <summary>Verifies catch/fallback operators preserve their terminal behavior.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ErrorFallbackOperatorsHandleFailures()
    {
        var catchReturn = new List<int>();
        var catchIgnoreCompleted = false;
        Exception? caught = null;

        using var returnSub = Observable.Throw<int>(new InvalidOperationException())
            .CatchReturn(7)
            .Subscribe(catchReturn.Add);

        using var ignoreSub = Observable.Throw<int>(new InvalidOperationException("handled"))
            .CatchIgnore<int, InvalidOperationException>(ex => caught = ex)
            .Subscribe(static _ => { }, () => catchIgnoreCompleted = true);

        await Assert.That(catchReturn).IsCollectionEqualTo([7]);
        await Assert.That(caught).IsNotNull();
        await Assert.That(catchIgnoreCompleted).IsTrue();
    }

    /// <summary>Verifies latest, replay, pairwise, partition, sample, and switch helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task StateAndRoutingOperatorsEmitExpectedValues()
    {
        var source = new Subject<int>();
        var latest = new List<int>();
        var pairwise = new List<(int Previous, int Current)>();
        var even = new List<int>();
        var odd = new List<int>();
        var sampled = new List<int>();
        var trigger = new Subject<object>();

        using var latestSub = source.LatestOrDefault(-1).Subscribe(latest.Add);
        using var pairwiseSub = source.Pairwise().Subscribe(pairwise.Add);
        var (truePartition, falsePartition) = source.Partition(x => x % 2 == 0);
        using var evenSub = truePartition.Subscribe(even.Add);
        using var oddSub = falsePartition.Subscribe(odd.Add);
        using var sampledSub = source.SampleLatest(trigger).Subscribe(sampled.Add);

        source.OnNext(1);
        trigger.OnNext(new object());
        source.OnNext(2);
        trigger.OnNext(new object());

        var switched = new List<int>();
        using var switchSub = Observable.Empty<int>().SwitchIfEmpty(Observable.Return(99)).Subscribe(switched.Add);

        await Assert.That(latest).IsCollectionEqualTo([-1, 1, 2]);
        await Assert.That(pairwise).IsCollectionEqualTo([(1, 2)]);
        await Assert.That(even).IsCollectionEqualTo([2]);
        await Assert.That(odd).IsCollectionEqualTo([1]);
        await Assert.That(sampled).IsCollectionEqualTo([1, 2]);
        await Assert.That(switched).IsCollectionEqualTo([99]);
    }

    /// <summary>Verifies async projection and sequential run helpers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsyncAndSequentialHelpersEmitExpectedValues()
    {
        var source = new Subject<int>();
        var sequential = new List<int>();
        var concurrent = new List<int>();
        using var seqSub = source.SelectAsyncSequential(x => Task.FromResult(x * 2)).Subscribe(sequential.Add);
        using var conSub = source.SelectAsyncConcurrent(x => Task.FromResult(x * 3), maxConcurrency: 2).Subscribe(concurrent.Add);

        source.OnNext(2);
        await Task.Delay(50);

        var runAll = new List<RxVoid>();
        using var runAllSub = new[]
        {
            Observable.Return(RxVoid.Default),
            Observable.Return(RxVoid.Default)
        }.RunAll().Subscribe(runAll.Add);

        await Assert.That(sequential).IsCollectionEqualTo([4]);
        await Assert.That(concurrent).IsCollectionEqualTo([6]);
        await Assert.That(runAll).IsCollectionEqualTo([RxVoid.Default]);
    }

    /// <summary>Verifies candidate probing emits the first transformed match.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FirstMatchFromCandidatesEmitsFirstMatch()
    {
        var results = new List<string>();
        using var sub = new[] { 1, 2, 3 }
            .FirstMatchFromCandidates(
                key => Observable.Return(key),
                value => $"value-{value}",
                value => value.EndsWith("2", StringComparison.Ordinal),
                "fallback")
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["value-2"]);
    }
}
