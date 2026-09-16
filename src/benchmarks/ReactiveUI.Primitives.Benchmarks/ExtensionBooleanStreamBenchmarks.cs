// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using R3BoolSubject = R3.Subject<bool>;
using RxBoolSubject = System.Reactive.Subjects.Subject<bool>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures negation and true/false filtering over a hot boolean stream.</summary>
[MemoryDiagnoser]
public class ExtensionBooleanStreamBenchmarks
{
    /// <summary>The number of booleans pushed through each pipeline.</summary>
    private const int Count = 256;

    /// <summary>Negates each boolean and keeps the true results.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesNotWhereTrue()
    {
        BoolSignalWitness observer = new();
        using Signal<bool> source = new();
        using var subscription = PrimitivesExtensions.WhereTrue(PrimitivesExtensions.Not(source)).Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Negates each boolean and keeps the true results using System.Reactive.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int SystemReactiveNotWhereTrue()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject source = new();
        using var subscription = RxObservable.Where(RxObservable.Select(source, static value => !value), static value => value)
            .Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Negates each boolean and keeps the true results using ReactiveUI.Extensions.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsNotWhereTrue()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject source = new();
        using var subscription = PackageExtensions.WhereTrue(PackageExtensions.Not(source)).Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Negates each boolean and keeps the true results using R3.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int R3NotWhereTrue()
    {
        CountingR3Witness<bool> observer = new();
        using R3BoolSubject source = new();
        using var subscription = source.Select(static value => !value).Where(static value => value).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext((i & 1) == 0);
        }

        return observer.Count;
    }

    /// <summary>Keeps the false booleans of the stream.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int PrimitivesWhereFalse()
    {
        BoolSignalWitness observer = new();
        using Signal<bool> source = new();
        using var subscription = PrimitivesExtensions.WhereFalse(source).Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Keeps the false booleans of the stream using System.Reactive.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int SystemReactiveWhereFalse()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject source = new();
        using var subscription = RxObservable.Where(source, static value => !value).Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Keeps the false booleans of the stream using ReactiveUI.Extensions.</summary>
    /// <returns>The number of values that reached the observer.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsWhereFalse()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject source = new();
        using var subscription = PackageExtensions.WhereFalse(source).Subscribe(observer);
        PushAlternating(source);
        return observer.NextCount;
    }

    /// <summary>Pushes alternating booleans, starting with true.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushAlternating(IObserver<bool> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext((i & 1) == 0);
        }
    }
}
