// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks latest-value combination across the shapes that reach different coordinators: the pairwise
/// coordinator holds both values in typed fields, while three or more sources share one coordinator that keeps
/// latest values in an <c>object?[]</c>. The tuple and list overloads sit on top of those, so their cases show
/// what the result shape itself costs on each path.
/// </summary>
[MemoryDiagnoser]
public class OperatorCombineLatestBenchmarks
{
    /// <summary>The number of values pushed through the leading source.</summary>
    private const int Count = 1000;

    /// <summary>Pairwise combination through an explicit selector.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSelectorTwoSources()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        IntSignalWitness observer = new();
        using var subscription = first.CombineLatest(second, static (left, right) => left + right).Subscribe(observer);
        return Drive(observer, first, second);
    }

    /// <summary>Pairwise combination returning a tuple.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int PrimitivesTupleTwoSources()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        IntSignalWitness observer = new();
        using var subscription = first.CombineLatest(second)
            .Select(static values => values.First + values.Second)
            .Subscribe(observer);
        return Drive(observer, first, second);
    }

    /// <summary>Pairwise combination using System.Reactive.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int SystemReactiveSelectorTwoSources()
    {
        using RxSubject first = new();
        using RxSubject second = new();
        IntSignalWitness observer = new();
        using var subscription = RxObservable
            .CombineLatest(first, second, static (left, right) => left + right)
            .Subscribe(observer);
        return DriveRx(observer, first, second);
    }

    /// <summary>Four-source combination through an explicit selector.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int PrimitivesSelectorFourSources()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        using Signal<int> fourth = new();
        IntSignalWitness observer = new();
        using var subscription = first
            .CombineLatest(second, third, fourth, static (a, b, c, d) => a + b + c + d)
            .Subscribe(observer);
        return Drive(observer, first, second, third, fourth);
    }

    /// <summary>Four-source combination returning a tuple.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int PrimitivesTupleFourSources()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        using Signal<int> fourth = new();
        IntSignalWitness observer = new();
        using var subscription = first
            .CombineLatest(second, third, fourth)
            .Select(static values => values.First + values.Second + values.Third + values.Fourth)
            .Subscribe(observer);
        return Drive(observer, first, second, third, fourth);
    }

    /// <summary>Four-source combination returning a list.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int PrimitivesListFourSources()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        using Signal<int> fourth = new();
        IntSignalWitness observer = new();
        using var subscription = LinqExtensions.CombineLatest<int>(first, second, third, fourth)
            .Select(static values => values[0] + values[1] + values[2] + values[3])
            .Subscribe(observer);
        return Drive(observer, first, second, third, fourth);
    }

    /// <summary>Four-source combination using System.Reactive.</summary>
    /// <returns>The sum of combined values.</returns>
    [Benchmark]
    public int SystemReactiveSelectorFourSources()
    {
        using RxSubject first = new();
        using RxSubject second = new();
        using RxSubject third = new();
        using RxSubject fourth = new();
        IntSignalWitness observer = new();
        using var subscription = RxObservable
            .CombineLatest(first, second, third, fourth, static (a, b, c, d) => a + b + c + d)
            .Subscribe(observer);
        return DriveRx(observer, first, second, third, fourth);
    }

    /// <summary>Primes the trailing sources, then pushes a burst through the leading one.</summary>
    /// <param name="observer">The observer collecting the combined values.</param>
    /// <param name="sources">The sources, the first of which carries the burst.</param>
    /// <returns>The sum of combined values.</returns>
    private static int Drive(IntSignalWitness observer, params Signal<int>[] sources)
    {
        for (var i = 1; i < sources.Length; i++)
        {
            sources[i].OnNext(i);
        }

        for (var i = 0; i < Count; i++)
        {
            sources[0].OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Primes the trailing System.Reactive sources, then pushes a burst through the leading one.</summary>
    /// <param name="observer">The observer collecting the combined values.</param>
    /// <param name="sources">The sources, the first of which carries the burst.</param>
    /// <returns>The sum of combined values.</returns>
    private static int DriveRx(IntSignalWitness observer, params RxSubject[] sources)
    {
        for (var i = 1; i < sources.Length; i++)
        {
            sources[i].OnNext(i);
        }

        for (var i = 0; i < Count; i++)
        {
            sources[0].OnNext(i);
        }

        return observer.Total;
    }
}
