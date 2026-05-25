// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
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

public class FactoryOperatorContractTests
{
    [Test]
    public void FactoriesEmitExpectedFiniteSequencesAndDisposeResources()
    {
        var values = new List<int>();
        var completed = 0;
        var disposed = 0;

        Signal.Range(2, 3)
            .Concat(Signal.Repeat(9, 2))
            .Concat(Signal.Unfold(1, state => state <= 3, state => state + 1, state => state * 10))
            .Concat(Signal.Use(
                () => Disposable.Create(() => disposed++),
                _ => Signal.FromEnumerable(new[] { 7, 8 })))
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        Assert.Equal(new[] { 2, 3, 4, 9, 9, 10, 20, 30, 7, 8 }, values);
        Assert.Equal(1, completed);
        Assert.Equal(1, disposed);
    }

    [Test]
    public void UnaryOperatorsTransformFilterAggregateAndMaterialize()
    {
        var sparks = new List<Spark<int>>();
        var values = new List<int>();
        var terminal = new List<int>();
        var taps = 0;

        Signal.FromEnumerable(new[] { 1, 2, 2, 3, 4 })
            .Map(value => value * 2)
            .Keep(value => value >= 4)
            .DistinctUntilChanged()
            .Tap(_ => taps++)
            .Scan(0, (sum, value) => sum + value)
            .Take(3)
            .Sparkify()
            .Subscribe(sparks.Add);

        Signal.FromEnumerable(sparks).Unspark().Subscribe(values.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3, 4 }).Fold(0, (sum, value) => sum + value).Subscribe(terminal.Add);

        Assert.Equal(new[] { 4, 10, 18 }, values);
        Assert.Equal(new[] { 10 }, terminal);
        Assert.Equal(3, taps);
        Assert.Equal(SparkKind.OnCompleted, sparks[^1].Kind);
    }

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
        source.OnNext(1);
        source.OnNext(2);
        selectedSubscription.Dispose();
        filteredSubscription.Dispose();

        Assert.False(source.HasObservers);
        source.OnNext(3);

        Assert.Equal(new[] { 2, 3 }, selectedValues);
        Assert.Equal(new[] { 2 }, filteredValues);
    }

    [Test]
    public void CombiningOperatorsPreserveCoreOrderingSemantics()
    {
        var merged = new List<int>();
        var concatenated = new List<int>();
        var zipped = new List<int>();
        var latest = new List<string>();

        Signal.Merge(Signal.FromEnumerable(new[] { 1, 2 }), Signal.FromEnumerable(new[] { 3, 4 })).Subscribe(merged.Add);
        Signal.Concat(Signal.FromEnumerable(new[] { 1, 2 }), Signal.FromEnumerable(new[] { 3, 4 })).Subscribe(concatenated.Add);
        Signal.Zip(Signal.FromEnumerable(new[] { 1, 2 }), Signal.FromEnumerable(new[] { 10, 20 }), (left, right) => left + right).Subscribe(zipped.Add);
        Signal.CombineLatest(Signal.FromEnumerable(new[] { 1, 2 }), Signal.FromEnumerable(new[] { "a", "b" }), (left, right) => left + right).Subscribe(latest.Add);

        Assert.Equal(new[] { 1, 2, 3, 4 }, merged);
        Assert.Equal(new[] { 1, 2, 3, 4 }, concatenated);
        Assert.Equal(new[] { 11, 22 }, zipped);
        Assert.Equal(new[] { "2a", "2b" }, latest);
    }

    [Test]
    public void RetryResubscribesUntilSuccess()
    {
        var attempts = 0;
        var values = new List<int>();

        Signal.Defer(() =>
            {
                attempts++;
                return attempts < 3 ? Signal.Throw<int>(new InvalidOperationException("try again")) : Signal.Return(42);
            })
            .Retry(3)
            .Subscribe(values.Add);

        Assert.Equal(3, attempts);
        Assert.Equal(new[] { 42 }, values);
    }

    [Test]
    public async Task AsyncEnumerableFactoryCancelsEnumeratorOnDispose()
    {
        var disposed = false;
        var values = new List<int>();

        async IAsyncEnumerable<int> Values([EnumeratorCancellation] CancellationToken token = default)
        {
            try
            {
                yield return 1;
                await Task.Delay(5000, token);
                yield return 2;
            }
            finally
            {
                disposed = true;
            }
        }

        var subscription = Signal.FromAsyncEnumerable(Values()).Subscribe(values.Add, _ => { }, () => { });
        await Task.Delay(50);
        subscription.Dispose();
        await Task.Delay(50);

        Assert.Equal(new[] { 1 }, values);
        Assert.True(disposed);
    }

    [Test]
    public void TimeFactoriesUseInjectedScheduler()
    {
        var clock = new TestClock();
        var after = new List<long>();
        var every = new List<long>();

        Signal.After(TimeSpan.FromTicks(5), clock).Subscribe(after.Add);
        var subscription = Signal.Every(TimeSpan.FromTicks(3), clock).Subscribe(every.Add);

        clock.AdvanceBy(TimeSpan.FromTicks(4));
        Assert.Equal(0, after.Count);
        Assert.Equal(new[] { 0L }, every);

        clock.AdvanceBy(TimeSpan.FromTicks(1));
        Assert.Equal(new[] { 0L }, after);

        clock.AdvanceBy(TimeSpan.FromTicks(4));
        subscription.Dispose();
        clock.AdvanceBy(TimeSpan.FromTicks(10));
        Assert.Equal(new[] { 0L, 1L, 2L }, every);
    }

    [Test]
    public void AdditionalFactoriesAndUnaryOperatorsCoverCommonParitySurface()
    {
        var leadAppend = new List<int>();
        var ignored = new List<int>();
        var distinctBy = new List<int>();
        var takeWhile = new List<int>();
        var skipWhile = new List<int>();
        var defaulted = new List<int>();
        var count = new List<int>();
        var any = new List<bool>();
        var all = new List<bool>();
        var contains = new List<bool>();
        var isEmpty = new List<bool>();
        var selected = new List<int>();

        Signal.FromEnumerable(new[] { 2, 3 }).Lead(1).Append(4).Prepend(0).Subscribe(leadAppend.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3 }).IgnoreValues().Subscribe(ignored.Add);
        Signal.FromEnumerable(new[] { 11, 12, 21, 22 }).DistinctBy(value => value / 10).Subscribe(distinctBy.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3, 1 }).TakeWhile(value => value < 3).Subscribe(takeWhile.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3, 1 }).SkipWhile(value => value < 3).Subscribe(skipWhile.Add);
        Signal.Empty<int>().DefaultIfEmpty(42).Subscribe(defaulted.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3 }).Count().Subscribe(count.Add);
        Signal.FromEnumerable(new[] { 1, 2, 3 }).Any(value => value == 2).Subscribe(any.Add);
        Signal.FromEnumerable(new[] { 2, 4, 6 }).All(value => value % 2 == 0).Subscribe(all.Add);
        Signal.FromEnumerable(new[] { 2, 4, 6 }).Contains(4).Subscribe(contains.Add);
        Signal.Empty<int>().IsEmpty().Subscribe(isEmpty.Add);
        Signal.FromEnumerable(new[] { 1, 2 }).Bind(value => Signal.Range(value * 10, 2)).Subscribe(selected.Add);

        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, leadAppend);
        Assert.Equal(0, ignored.Count);
        Assert.Equal(new[] { 11, 21 }, distinctBy);
        Assert.Equal(new[] { 1, 2 }, takeWhile);
        Assert.Equal(new[] { 3, 1 }, skipWhile);
        Assert.Equal(new[] { 42 }, defaulted);
        Assert.Equal(new[] { 3 }, count);
        Assert.Equal(new[] { true }, any);
        Assert.Equal(new[] { true }, all);
        Assert.Equal(new[] { true }, contains);
        Assert.Equal(new[] { true }, isEmpty);
        Assert.Equal(new[] { 10, 11, 20, 21 }, selected);
    }

    [Test]
    public async Task SystemReactiveNamedAliasesCoverMigrationConvenienceSurface()
    {
        var values = new List<int>();
        var sideEffects = new List<int>();
        var recovered = new List<int>();
        var observed = new List<int>();
        var clock = new TestClock();
        var source = new Signal<int>();

        Signal.FromEnumerable(new[] { 2, 3 })
            .StartWith(new[] { 0, 1 })
            .Do(sideEffects.Add)
            .AsObservable()
            .Subscribe(values.Add);

        Signal.Throw<int>(new InvalidOperationException("recover"))
            .Catch(_ => Signal.Return(42))
            .Subscribe(recovered.Add);

        source.ObserveOn(clock).Subscribe(observed.Add);
        source.OnNext(7);

        Assert.Equal(new[] { 0, 1, 2, 3 }, values);
        Assert.Equal((IEnumerable<int>)values, sideEffects);
        Assert.Equal(new[] { 42 }, recovered);
        Assert.Equal(0, observed.Count);

        clock.Start();

        Assert.Equal(new[] { 7 }, observed);

        var converted = new[] { 4, 5 }.ToObservable();
        var last = await converted.ToTask();
        var first = await Signal.FromEnumerable(new[] { 9, 10 }).FirstAsync().ToTask();
        var started = await Signal.Start(() => 11, Sequencer.CurrentThread).ToTask();

        Assert.Equal(5, last);
        Assert.Equal(9, first);
        Assert.Equal(11, started);
    }

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

        source.Throttle(TimeSpan.FromTicks(5), clock).Subscribe(throttled.Add);
        source.Sample(TimeSpan.FromTicks(4), clock).Subscribe(sampled.Add);
        source.TimeInterval(clock).Subscribe(intervals.Add);

        source.OnNext(1);
        clock.AdvanceBy(TimeSpan.FromTicks(2));
        source.OnNext(2);
        clock.AdvanceBy(TimeSpan.FromTicks(4));
        source.OnNext(3);
        clock.AdvanceBy(TimeSpan.FromTicks(5));
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(4));

        Signal.FromEnumerable(new[] { 1, 2 }).ZipLatest(Signal.FromEnumerable(new[] { "a", "b" }), (left, right) => left + right).Subscribe(latest.Add);
        Signal.ForkJoin(Signal.FromEnumerable(new[] { 1, 2 }), Signal.FromEnumerable(new[] { 10, 20 }), (left, right) => left + right).Subscribe(forkJoined.Add);

        Assert.Equal(new[] { 3 }, throttled);
        Assert.Equal(new[] { 2, 3 }, sampled);
        Assert.Equal(TimeSpan.Zero, intervals[0].Interval);
        Assert.Equal(TimeSpan.FromTicks(2), intervals[1].Interval);
        Assert.Equal(TimeSpan.FromTicks(4), intervals[2].Interval);
        Assert.Equal(new[] { "2a", "2b" }, latest);
        Assert.Equal(new[] { 22 }, forkJoined);
    }

    [Test]
    public async Task TerminalTaskOperatorsCompleteWithExpectedSemantics()
    {
        var first = await Signal.FromEnumerable(new[] { 3, 4 }).FirstAsync();
        var collected = await Signal.FromEnumerable(new[] { 1, 2, 3 }).CollectArrayAsync();
        var none = await Signal.Empty<int>().FirstOrDefaultAsync(42);

        Assert.Equal(3, first);
        Assert.Equal(new[] { 1, 2, 3 }, (IEnumerable<int>)collected);
        Assert.Equal(42, none);
    }
}
