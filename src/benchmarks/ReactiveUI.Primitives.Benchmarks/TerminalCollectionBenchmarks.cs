// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading;

using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for terminal and collection APIs.
/// </summary>
[MemoryDiagnoser]
public class TerminalCollectionBenchmarks
{
    private const int Count = 32;

    /// <summary>
    /// Benchmarks collecting into a list signal.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCollectList()
    {
        var result = 0;
        using var subscription = Signal.Sequence(1, Count).CollectList().Subscribe(values => result = values.Count);
        return result;
    }

    /// <summary>
    /// Benchmarks collecting into a list using System.Reactive.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int SystemReactiveCollectList()
    {
        var result = 0;
        using var subscription = RxObservable.Range(1, Count).ToList().Subscribe(values => result = values.Count);
        return result;
    }

    /// <summary>
    /// Benchmarks collecting into a list using R3.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> R3CollectList()
    {
        return (await R3.ObservableExtensions.ToListAsync(R3.Observable.Range(1, Count), CancellationToken.None)
            .ConfigureAwait(false)).Count;
    }

    /// <summary>
    /// Benchmarks collecting into an array signal.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int PrimitivesCollectArray()
    {
        var result = 0;
        using var subscription = Signal.Sequence(1, Count).CollectArray().Subscribe(values => result = values.Length);
        return result;
    }

    /// <summary>
    /// Benchmarks collecting into an array using System.Reactive.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public int SystemReactiveCollectArray()
    {
        var result = 0;
        using var subscription = RxObservable.Range(1, Count).ToArray().Subscribe(values => result = values.Length);
        return result;
    }

    /// <summary>
    /// Benchmarks collecting into an array using R3.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> R3CollectArray()
    {
        return (await R3.ObservableExtensions.ToArrayAsync(R3.Observable.Range(1, Count), CancellationToken.None)
            .ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks asynchronous array collection.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCollectArrayAsync()
    {
        return (await Signal.Sequence(1, Count).CollectArrayAsync().ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks asynchronous array collection using System.Reactive.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveCollectArrayAsync()
    {
        return (await RxObservable.Range(1, Count).ToArray().ToTask().ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks asynchronous array collection using R3.
    /// </summary>
    /// <returns>The collected count.</returns>
    [Benchmark]
    public async Task<int> R3CollectArrayAsync()
    {
        return (await R3.ObservableExtensions.ToArrayAsync(R3.Observable.Range(1, Count), CancellationToken.None)
            .ConfigureAwait(false)).Length;
    }

    /// <summary>
    /// Benchmarks first-value task conversion.
    /// </summary>
    /// <returns>The first value.</returns>
    [Benchmark]
    public Task<int> PrimitivesFirstAsync() =>
        Signal.Sequence(1, Count).FirstAsync();

    /// <summary>
    /// Benchmarks first-value task conversion using System.Reactive.
    /// </summary>
    /// <returns>The first value.</returns>
    [Benchmark]
    public Task<int> SystemReactiveFirstAsync() =>
        RxObservable.Range(1, Count).FirstAsync().ToTask();

    /// <summary>
    /// Benchmarks first-value task conversion using R3.
    /// </summary>
    /// <returns>The first value.</returns>
    [Benchmark]
    public Task<int> R3FirstAsync() =>
        R3.ObservableExtensions.FirstAsync(R3.Observable.Range(1, Count), CancellationToken.None);

    /// <summary>
    /// Benchmarks last-value task conversion.
    /// </summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public Task<int> PrimitivesToTask() =>
        Signal.Sequence(1, Count).ToTask();

    /// <summary>
    /// Benchmarks last-value task conversion using System.Reactive.
    /// </summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public Task<int> SystemReactiveToTask() =>
        RxObservable.Range(1, Count).ToTask();

    /// <summary>
    /// Benchmarks last-value task conversion using R3.
    /// </summary>
    /// <returns>The last value.</returns>
    [Benchmark]
    public Task<int> R3ToTask() =>
        R3.ObservableExtensions.LastAsync(R3.Observable.Range(1, Count), CancellationToken.None);

    /// <summary>
    /// Benchmarks predicate count.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public int PrimitivesCountPredicate()
    {
        var observer = new IntSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Count(static value => value % 2 == 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks predicate count using System.Reactive.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public int SystemReactiveCountPredicate()
    {
        var observer = new IntSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Count(static value => value % 2 == 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks predicate count using R3.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public Task<int> R3CountPredicate() =>
        R3.ObservableExtensions.CountAsync(
            R3.Observable.Range(1, Count),
            static (int value) => value % 2 == 0,
            CancellationToken.None);

    /// <summary>
    /// Benchmarks predicate long-count over a range signal.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public long PrimitivesLongCountPredicate()
    {
        var observer = new LongSignalObserver();
        using var subscription = Signal.Sequence(1, Count).LongCount(static value => value % 2 == 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks predicate long-count using System.Reactive.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public long SystemReactiveLongCountPredicate()
    {
        var observer = new LongSignalObserver();
        using var subscription = RxObservable.Range(1, Count).LongCount(static value => value % 2 == 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks predicate long-count using R3.
    /// </summary>
    /// <returns>The matching count.</returns>
    [Benchmark]
    public async Task<long> R3LongCountPredicate() =>
        await R3.ObservableExtensions.CountAsync(
            R3.Observable.Range(1, Count),
            static (int value) => value % 2 == 0,
            CancellationToken.None).ConfigureAwait(false);

    /// <summary>
    /// Benchmarks all over a range signal.
    /// </summary>
    /// <returns>One when all values match.</returns>
    [Benchmark]
    public int PrimitivesAllRange()
    {
        var observer = new BoolSignalObserver();
        using var subscription = Signal.Sequence(1, Count).All(static value => value > 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks all over a range using System.Reactive.
    /// </summary>
    /// <returns>One when all values match.</returns>
    [Benchmark]
    public int SystemReactiveAllRange()
    {
        var observer = new BoolSignalObserver();
        using var subscription = RxObservable.Range(1, Count).All(static value => value > 0).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks all over a range using R3.
    /// </summary>
    /// <returns>One when all values match.</returns>
    [Benchmark]
    public async Task<int> R3AllRange() =>
        await R3.ObservableExtensions.AllAsync(
            R3.Observable.Range(1, Count),
            static (int value) => value > 0,
            CancellationToken.None).ConfigureAwait(false) ? 1 : 0;

    /// <summary>
    /// Benchmarks contains over a range signal.
    /// </summary>
    /// <returns>One when the value is present.</returns>
    [Benchmark]
    public int PrimitivesContainsRange()
    {
        var observer = new BoolSignalObserver();
        using var subscription = Signal.Sequence(1, Count).Contains(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks contains over a range using System.Reactive.
    /// </summary>
    /// <returns>One when the value is present.</returns>
    [Benchmark]
    public int SystemReactiveContainsRange()
    {
        var observer = new BoolSignalObserver();
        using var subscription = RxObservable.Range(1, Count).Contains(Count).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>
    /// Benchmarks contains over a range using R3.
    /// </summary>
    /// <returns>One when the value is present.</returns>
    [Benchmark]
    public async Task<int> R3ContainsRange() =>
        await R3.ObservableExtensions.ContainsAsync(
            R3.Observable.Range(1, Count),
            Count,
            CancellationToken.None).ConfigureAwait(false) ? 1 : 0;

    /// <summary>
    /// Benchmarks all and contains terminal predicates.
    /// </summary>
    /// <returns>The number of true results.</returns>
    [Benchmark]
    public int PrimitivesAllContains()
    {
        var allObserver = new BoolSignalObserver();
        var containsObserver = new BoolSignalObserver();
        using var all = Signal.Sequence(1, Count).All(static value => value > 0).Subscribe(allObserver);
        using var contains = Signal.Sequence(1, Count).Contains(Count).Subscribe(containsObserver);
        return allObserver.Total + containsObserver.Total;
    }

    /// <summary>
    /// Benchmarks all and contains terminal predicates using System.Reactive.
    /// </summary>
    /// <returns>The number of true results.</returns>
    [Benchmark]
    public int SystemReactiveAllContains()
    {
        var allObserver = new BoolSignalObserver();
        var containsObserver = new BoolSignalObserver();
        using var all = RxObservable.Range(1, Count).All(static value => value > 0).Subscribe(allObserver);
        using var contains = RxObservable.Range(1, Count).Contains(Count).Subscribe(containsObserver);
        return allObserver.Total + containsObserver.Total;
    }

    /// <summary>
    /// Benchmarks all and contains terminal predicates using R3.
    /// </summary>
    /// <returns>The number of true results.</returns>
    [Benchmark]
    public async Task<int> R3AllContains()
    {
        var all = await R3.ObservableExtensions.AllAsync(
                R3.Observable.Range(1, Count),
                static (int value) => value > 0,
                CancellationToken.None)
            .ConfigureAwait(false);
        var contains = await R3.ObservableExtensions.ContainsAsync(
                R3.Observable.Range(1, Count),
                Count,
                CancellationToken.None)
            .ConfigureAwait(false);
        return (all ? 1 : 0) + (contains ? 1 : 0);
    }
}
