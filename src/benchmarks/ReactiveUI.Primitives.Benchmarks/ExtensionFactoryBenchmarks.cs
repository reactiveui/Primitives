// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the factory-style extension operators: array and batch emission, deferred start, resource scoping, loops and sequential runs.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class ExtensionFactoryBenchmarks
{
    /// <summary>The number of subscriptions or iterations each case performs.</summary>
    private const int Count = 256;

    /// <summary>The number of batches flattened by the ForEach cases.</summary>
    private const int BatchCount = 16;

    /// <summary>The result produced by the started functions.</summary>
    private const int StartResult = 1;

    /// <summary>The values emitted by the array-backed cases.</summary>
    private static readonly int[] Values = [.. Enumerable.Range(0, Count)];

    /// <summary>Benchmarks FromArray emitting every element of an array to one subscriber.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("FromArray")]
    public int PrimitivesFromArray()
    {
        IntSignalWitness observer = new();
        using var subscription = PrimitivesExtensions.FromArray(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks FromArray using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted values.</returns>
    [Benchmark]
    [BenchmarkCategory("FromArray")]
    public int PackageFromArray()
    {
        IntSignalWitness observer = new();
        using var subscription = PackageExtensions.FromArray(Values).Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks ForEach flattening a sequence of array batches into individual values.</summary>
    /// <returns>The sum of the flattened values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ForEach")]
    public int PrimitivesForEach()
    {
        var batches = new IEnumerable<int>[BatchCount];
        Array.Fill(batches, Values);
        IntSignalWitness observer = new();
        using var subscription = PrimitivesExtensions.ForEach(PrimitivesExtensions.FromArray(batches), null)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks ForEach batch flattening using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the flattened values.</returns>
    [Benchmark]
    [BenchmarkCategory("ForEach")]
    public int PackageForEach()
    {
        var batches = new IEnumerable<int>[BatchCount];
        Array.Fill(batches, Values);
        IntSignalWitness observer = new();
        using var subscription = PackageExtensions.ForEach(PackageExtensions.FromArray(batches), null)
            .Subscribe(observer);
        return observer.Total;
    }

    /// <summary>Benchmarks Start for an action, subscribing once per iteration on the immediate sequencer.</summary>
    /// <returns>The number of completion signals observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StartAction")]
    public int PrimitivesStartAction()
    {
        CountingSignalWitness<RxVoid> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PrimitivesExtensions.Start(static () => { }, Sequencer.Immediate)
                .Subscribe(observer);
        }

        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks Start for an action using ReactiveUI.Extensions.</summary>
    /// <returns>The number of completion signals observed.</returns>
    [Benchmark]
    [BenchmarkCategory("StartAction")]
    public int PackageStartAction()
    {
        CountingSignalWitness<RxUnit> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PackageExtensions.Start(static () => { }, ImmediateScheduler.Instance)
                .Subscribe(observer);
        }

        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks Start for a function, running it inline once per subscription.</summary>
    /// <returns>The sum of the produced results.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("StartFunc")]
    public int PrimitivesStartFunc()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PrimitivesExtensions.Start(static () => StartResult, null).Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks Start for a function using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the produced results.</returns>
    [Benchmark]
    [BenchmarkCategory("StartFunc")]
    public int PackageStartFunc()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = PackageExtensions.Start(static () => StartResult, ImmediateScheduler.Instance)
                .Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Benchmarks Using with an action, touching and disposing a resource per subscription.</summary>
    /// <returns>The number of touches plus the number of disposals.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("UsingAction")]
    public int PrimitivesUsingAction()
    {
        CountingSignalWitness<RxVoid> observer = new();
        var disposals = 0;
        for (var i = 0; i < Count; i++)
        {
            CountingResource resource = new();
            using var subscription = PrimitivesExtensions.Using(
                    resource,
                    static item => { _ = item.Touch(); },
                    Sequencer.Immediate)
                .Subscribe(observer);
            disposals += resource.Disposals;
        }

        return observer.Count + disposals;
    }

    /// <summary>Benchmarks Using with an action using ReactiveUI.Extensions.</summary>
    /// <returns>The number of touches plus the number of disposals.</returns>
    [Benchmark]
    [BenchmarkCategory("UsingAction")]
    public int PackageUsingAction()
    {
        CountingSignalWitness<RxUnit> observer = new();
        var disposals = 0;
        for (var i = 0; i < Count; i++)
        {
            CountingResource resource = new();
            using var subscription = PackageExtensions.Using(
                    resource,
                    static item => { _ = item.Touch(); },
                    ImmediateScheduler.Instance)
                .Subscribe(observer);
            disposals += resource.Disposals;
        }

        return observer.Count + disposals;
    }

    /// <summary>Benchmarks Using with a function, reading and disposing a resource per subscription.</summary>
    /// <returns>The sum of the read values plus the number of disposals.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("UsingFunc")]
    public int PrimitivesUsingFunc()
    {
        IntSignalWitness observer = new();
        var disposals = 0;
        for (var i = 0; i < Count; i++)
        {
            CountingResource resource = new();
            using var subscription = PrimitivesExtensions.Using(resource, static item => item.Touch()).Subscribe(observer);
            disposals += resource.Disposals;
        }

        return observer.Total + disposals;
    }

    /// <summary>Benchmarks Using with a function using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the read values plus the number of disposals.</returns>
    [Benchmark]
    [BenchmarkCategory("UsingFunc")]
    public int PackageUsingFunc()
    {
        IntSignalWitness observer = new();
        var disposals = 0;
        for (var i = 0; i < Count; i++)
        {
            CountingResource resource = new();
            using var subscription = PackageExtensions.Using(resource, static item => item.Touch()).Subscribe(observer);
            disposals += resource.Disposals;
        }

        return observer.Total + disposals;
    }

    /// <summary>Benchmarks While looping on the immediate sequencer until a counter runs out.</summary>
    /// <returns>The number of iteration signals plus the completion count.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("While")]
    public int PrimitivesWhile()
    {
        LoopCounter loop = new(Count);
        CountingSignalWitness<RxVoid> observer = new();
        using var subscription = PrimitivesExtensions.While(loop.HasRemaining, loop.Step, Sequencer.Immediate)
            .Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks While looping until a counter runs out using ReactiveUI.Extensions.</summary>
    /// <returns>The number of iteration signals plus the completion count.</returns>
    [Benchmark]
    [BenchmarkCategory("While")]
    public int PackageWhile()
    {
        LoopCounter loop = new(Count);
        CountingSignalWitness<RxUnit> observer = new();
        using var subscription = PackageExtensions.While(loop.HasRemaining, loop.Step, ImmediateScheduler.Instance)
            .Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks RunAll walking a list of synchronously completing sources in order.</summary>
    /// <returns>The number of completion signals observed.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("RunAll")]
    public int PrimitivesRunAll()
    {
        var sources = new IObservable<RxVoid>[Count];
        Array.Fill(sources, PrimitivesObservables.Return(RxVoid.Default));
        CountingSignalWitness<RxVoid> observer = new();
        using var subscription = PrimitivesExtensions.RunAll(sources).Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks RunAll over synchronously completing sources using ReactiveUI.Extensions.</summary>
    /// <returns>The number of completion signals observed.</returns>
    [Benchmark]
    [BenchmarkCategory("RunAll")]
    public int PackageRunAll()
    {
        var sources = new IObservable<RxUnit>[Count];
        Array.Fill(sources, PackageObservables.Return(RxUnit.Default));
        CountingSignalWitness<RxUnit> observer = new();
        using var subscription = PackageExtensions.RunAll(sources).Subscribe(observer);
        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Disposable resource that counts how often it is touched and disposed.</summary>
    private sealed class CountingResource : IDisposable
    {
        /// <summary>Gets the number of times the resource was disposed.</summary>
        internal int Disposals { get; private set; }

        /// <summary>Gets the number of times the resource was touched.</summary>
        internal int Touches { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => Disposals++;

        /// <summary>Records one use of the resource.</summary>
        /// <returns>The number of touches so far.</returns>
        internal int Touch() => ++Touches;
    }

    /// <summary>Loop state that allows a fixed number of iterations.</summary>
    /// <param name="iterations">The number of iterations to allow.</param>
    private sealed class LoopCounter(int iterations)
    {
        /// <summary>The iterations still allowed.</summary>
        private int _remaining = iterations;

        /// <summary>Reports whether another iteration is allowed.</summary>
        /// <returns><see langword="true"/> while iterations remain.</returns>
        internal bool HasRemaining() => _remaining > 0;

        /// <summary>Consumes one iteration.</summary>
        internal void Step() => _remaining--;
    }
}
