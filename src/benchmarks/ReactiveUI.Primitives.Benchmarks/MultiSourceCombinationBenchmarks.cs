// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures operators that combine several sources: wide-arity latest-value combination, distinct merge and fork-join.</summary>
[MemoryDiagnoser]
public class MultiSourceCombinationBenchmarks
{
    /// <summary>The number of values pushed through each case.</summary>
    private const int Count = 64;

    /// <summary>The number of sources in the wide-arity cases.</summary>
    private const int WideArity = 10;

    /// <summary>The number of sources in the narrow-arity cases.</summary>
    private const int NarrowArity = 3;

    /// <summary>Sums the ten latest values.</summary>
    private static readonly Func<int, int, int, int, int, int, int, int, int, int, int> SumTen =
        static (a, b, c, d, e, f, g, h, i, j) => a + b + c + d + e + f + g + h + i + j;

    /// <summary>Combines the latest values of ten sources under the Primitives name.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSyncLatestTenSources()
    {
        IntSignalWitness observer = new();
        var s = CreateSignals(WideArity);
        using var subscription = s[0]
            .SyncLatest(s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], SumTen)
            .Subscribe(observer);
        PushRoundRobin(s);
        Dispose(s);
        return observer.LastValue;
    }

    /// <summary>Combines the latest values of ten sources under the System.Reactive name.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark]
    public int PrimitivesCombineLatestTenSources()
    {
        IntSignalWitness observer = new();
        var s = CreateSignals(WideArity);
        using var subscription = s[0]
            .CombineLatest(s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], SumTen)
            .Subscribe(observer);
        PushRoundRobin(s);
        Dispose(s);
        return observer.LastValue;
    }

    /// <summary>Combines the latest values of ten sources using System.Reactive.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestTenSources()
    {
        IntSignalWitness observer = new();
        var s = new RxSubject[WideArity];
        for (var i = 0; i < s.Length; i++)
        {
            s[i] = new();
        }

        using var subscription = RxObservable
            .CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], SumTen)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            s[i % s.Length].OnNext(i);
        }

        foreach (var subject in s)
        {
            subject.Dispose();
        }

        return observer.LastValue;
    }

    /// <summary>Combines the latest values of ten sources using R3.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark]
    public int R3CombineLatestTenSources()
    {
        IntR3Witness observer = new();
        var s = new R3.Subject<int>[WideArity];
        for (var i = 0; i < s.Length; i++)
        {
            s[i] = new();
        }

        using var subscription = R3.Observable
            .CombineLatest(s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], SumTen)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            s[i % s.Length].OnNext(i);
        }

        foreach (var subject in s)
        {
            subject.Dispose();
        }

        return observer.LastValue;
    }

    /// <summary>Combines the latest values of three sources.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark]
    public int PrimitivesSyncLatestThreeSources()
    {
        IntSignalWitness observer = new();
        var s = CreateSignals(NarrowArity);
        using var subscription = s[0]
            .SyncLatest(s[1], s[2], static (a, b, c) => a + b + c)
            .Subscribe(observer);
        PushRoundRobin(s);
        Dispose(s);
        return observer.LastValue;
    }

    /// <summary>Combines the latest values of three sources using System.Reactive.</summary>
    /// <returns>The last combined value.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestThreeSources()
    {
        IntSignalWitness observer = new();
        using RxSubject a = new();
        using RxSubject b = new();
        using RxSubject c = new();
        RxSubject[] s = [a, b, c];
        using var subscription = RxObservable
            .CombineLatest(a, b, c, static (x, y, z) => x + y + z)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            s[i % s.Length].OnNext(i);
        }

        return observer.LastValue;
    }

    /// <summary>Merges two live sources and suppresses adjacent duplicates in one fused sink.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int PrimitivesBlendUnique()
    {
        IntSignalWitness observer = new();
        using Signal<int> left = new();
        using Signal<int> right = new();
        using var subscription = LinqExtensions.BlendUnique(left, right).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i >> 1);
            right.OnNext(i >> 1);
        }

        left.OnCompleted();
        right.OnCompleted();
        return observer.Total;
    }

    /// <summary>Merges two live sources and suppresses adjacent duplicates using System.Reactive.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int SystemReactiveMergeDistinctUntilChanged()
    {
        IntSignalWitness observer = new();
        using RxSubject left = new();
        using RxSubject right = new();
        using var subscription = RxObservable.DistinctUntilChanged(RxObservable.Merge(left, right)).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i >> 1);
            right.OnNext(i >> 1);
        }

        left.OnCompleted();
        right.OnCompleted();
        return observer.Total;
    }

    /// <summary>Merges two live sources and suppresses adjacent duplicates using R3.</summary>
    /// <returns>The total observed.</returns>
    [Benchmark]
    public int R3MergeDistinctUntilChanged()
    {
        IntR3Witness observer = new();
        using R3.Subject<int> left = new();
        using R3.Subject<int> right = new();
        using var subscription = R3.ObservableExtensions
            .DistinctUntilChanged(R3.ObservableExtensions.Merge(left, right))
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i >> 1);
            right.OnNext(i >> 1);
        }

        left.OnCompleted(R3.Result.Success);
        right.OnCompleted(R3.Result.Success);
        return observer.Total;
    }

    /// <summary>Pairs the final values of two live sources once both complete.</summary>
    /// <returns>The combined final value.</returns>
    [Benchmark]
    public int PrimitivesForkJoin()
    {
        IntSignalWitness observer = new();
        using Signal<int> left = new();
        using Signal<int> right = new();
        using var subscription = left.ForkJoin(right, static (l, r) => l + r).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i);
            right.OnNext(-i);
        }

        left.OnCompleted();
        right.OnCompleted();
        using var ranges = Signal.Range(1, Count).ForkJoin(Signal.Range(1, Count), static (l, r) => l + r).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Pairs the final values of two live sources once both complete using System.Reactive.</summary>
    /// <returns>The combined final value.</returns>
    [Benchmark]
    public int SystemReactiveLastZip()
    {
        IntSignalWitness observer = new();
        using RxSubject left = new();
        using RxSubject right = new();
        using var subscription = RxObservable
            .Zip(RxObservable.LastAsync(left), RxObservable.LastAsync(right), static (l, r) => l + r)
            .Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            left.OnNext(i);
            right.OnNext(-i);
        }

        left.OnCompleted();
        right.OnCompleted();
        using var ranges = RxObservable
            .Zip(RxObservable.LastAsync(RxObservable.Range(1, Count)), RxObservable.LastAsync(RxObservable.Range(1, Count)), static (l, r) => l + r)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Creates live signals.</summary>
    /// <param name="count">The number of signals.</param>
    /// <returns>The signals.</returns>
    private static Signal<int>[] CreateSignals(int count)
    {
        var signals = new Signal<int>[count];
        for (var i = 0; i < signals.Length; i++)
        {
            signals[i] = new();
        }

        return signals;
    }

    /// <summary>Pushes the benchmark values across the signals in turn.</summary>
    /// <param name="signals">The signals receiving values.</param>
    private static void PushRoundRobin(Signal<int>[] signals)
    {
        for (var i = 0; i < Count; i++)
        {
            signals[i % signals.Length].OnNext(i);
        }
    }

    /// <summary>Disposes the signals.</summary>
    /// <param name="signals">The signals to dispose.</param>
    private static void Dispose(Signal<int>[] signals)
    {
        foreach (var signal in signals)
        {
            signal.Dispose();
        }
    }
}
