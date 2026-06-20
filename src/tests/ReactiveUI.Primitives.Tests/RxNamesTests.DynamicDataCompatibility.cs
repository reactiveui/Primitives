// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for Rx compatibility operators required by DynamicData migration.</summary>
public partial class RxNamesTests
{
    /// <summary>Verifies <c>SubscribeSafe</c> converts downstream value-handler exceptions into a terminal error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeSafeStopsAfterDownstreamOnNextException()
    {
        Signal<int> source = new();
        InvalidOperationException expected = new(Boom);
        Exception? observed = null;
        var completed = 0;
        var delivered = 0;

        using var subscription = source.SubscribeSafe(
            _ =>
            {
                delivered++;
                throw expected;
            },
            error => observed = error,
            () => completed++);

        source.OnNext(One);
        source.OnNext(Two);

        await Assert.That(delivered).IsEqualTo(One);
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Verifies enumerable <c>Merge(maxConcurrent)</c> waits to subscribe to later sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EnumerableMergeHonorsMaxConcurrent()
    {
        Signal<int> first = new();
        Signal<int> second = new();
        List<int> values = [];

        using var subscription = new IObservable<int>[] { first, second }.Merge(One).Subscribe(values.Add);

        second.OnNext(Two);
        first.OnNext(One);

        await Assert.That(values.SequenceEqual([One])).IsTrue();

        first.OnCompleted();
        second.OnNext(Two);

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies the Rx migration aliases used by DynamicData produce expected values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DynamicDataCompatibilityAliasesProduceExpectedValues()
    {
        var startWith = Collect(Signal.FromEnumerable([Three]).StartWith(One, Two));
        var enumerableSelectMany = Collect(Signal.FromEnumerable([One, Two]).SelectMany(static value => new[] { value, value + Ten }));
        var repeatedSelectMany = Collect(Signal.FromEnumerable([One, Two]).SelectMany(Signal.Return(Ten)));
        var recovered = Collect(Signal.Throw<int>(new InvalidOperationException(Boom)).Catch((InvalidOperationException _) => Signal.Return(Two)));

        await Assert.That(startWith.SequenceEqual([One, Two, Three])).IsTrue();
        await Assert.That(enumerableSelectMany.SequenceEqual([One, One + Ten, Two, Two + Ten])).IsTrue();
        await Assert.That(repeatedSelectMany.SequenceEqual([Ten, Ten])).IsTrue();
        await Assert.That(recovered.SequenceEqual([Two])).IsTrue();
    }

    /// <summary>Verifies timed <c>Buffer</c> flushes scheduled batches and the final batch on completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BufferFlushesTimedAndFinalBatches()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<IList<int>> batches = [];

        using var subscription = source.Buffer(TimeSpan.FromTicks(Two), clock).Subscribe(batches.Add);
        source.OnNext(One);
        source.OnNext(Two);

        await Assert.That(batches.Count).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(Two));

        await Assert.That(batches.Count).IsEqualTo(One);
        await Assert.That(batches[0].SequenceEqual([One, Two])).IsTrue();

        source.OnNext(Three);
        source.OnCompleted();

        await Assert.That(batches.Count).IsEqualTo(Two);
        await Assert.That(batches[1].SequenceEqual([Three])).IsTrue();
    }

    /// <summary>Verifies <c>Throttle</c> emits only the latest value after the quiet period.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThrottleEmitsLatestAfterQuietPeriod()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];

        using var subscription = source.Throttle(TimeSpan.FromTicks(Two), clock).Subscribe(values.Add);
        source.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(One));
        source.OnNext(Two);
        clock.AdvanceBy(TimeSpan.FromTicks(One));

        await Assert.That(values.Count).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(One));

        await Assert.That(values.SequenceEqual([Two])).IsTrue();
    }

    /// <summary>Verifies <c>Finally</c> runs once when a subscription completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FinallyRunsOnceOnCompletion()
    {
        var cleanupCount = 0;
        List<int> values = [];

        Signal.FromEnumerable([One]).Finally(() => cleanupCount++).Subscribe(values.Add);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(cleanupCount).IsEqualTo(One);
    }
}
