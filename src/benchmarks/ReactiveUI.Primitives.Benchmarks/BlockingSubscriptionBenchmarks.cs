// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using ReactiveUI.Primitives.Concurrency;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageSubscriptionExtensions = ReactiveUI.Extensions.ObservableSubscriptionExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesSubscriptionExtensions = ReactiveUI.Primitives.Extensions.ObservableSubscriptionExtensions;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures the subscribe-and-read and blocking wait helpers over sources that terminate during subscribe.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class BlockingSubscriptionBenchmarks
{
    /// <summary>The number of subscribe or wait calls each case makes.</summary>
    private const int Count = 256;

    /// <summary>The number of values emitted by the array source.</summary>
    private const int ValueCount = 16;

    /// <summary>The number of waits each completion iteration makes.</summary>
    private const int WaitsPerIteration = 2;

    /// <summary>The wait timeout; every source terminates before it is reached.</summary>
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>The values emitted by the array source.</summary>
    private static readonly int[] Values = [.. Enumerable.Range(1, ValueCount)];

    /// <summary>Benchmarks SubscribeGetValue reading the last value of a synchronous array source.</summary>
    /// <returns>The sum of the captured values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SubscribeGetValue")]
    public int PrimitivesSubscribeGetValue()
    {
        var source = PrimitivesExtensions.FromArray(Values);
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += PrimitivesSubscriptionExtensions.SubscribeGetValue(source);
        }

        return total;
    }

    /// <summary>Benchmarks SubscribeGetValue using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the captured values.</returns>
    [Benchmark]
    [BenchmarkCategory("SubscribeGetValue")]
    public int PackageSubscribeGetValue()
    {
        var source = PackageExtensions.FromArray(Values);
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += PackageSubscriptionExtensions.SubscribeGetValue(source);
        }

        return total;
    }

    /// <summary>Benchmarks SubscribeGetError capturing the failure of a typed source and of a unit source.</summary>
    /// <returns>The number of captured errors.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SubscribeGetError")]
    public int PrimitivesSubscribeGetError()
    {
        FailingSource<int> source = new();
        FailingSource<RxVoid> unitSource = new();
        var errors = 0;
        for (var i = 0; i < Count; i++)
        {
            errors += PrimitivesSubscriptionExtensions.SubscribeGetError(source) is null ? 0 : 1;
            errors += PrimitivesSubscriptionExtensions.SubscribeGetError(unitSource) is null ? 0 : 1;
        }

        return errors;
    }

    /// <summary>Benchmarks SubscribeGetError using ReactiveUI.Extensions.</summary>
    /// <returns>The number of captured errors.</returns>
    [Benchmark]
    [BenchmarkCategory("SubscribeGetError")]
    public int PackageSubscribeGetError()
    {
        FailingSource<int> source = new();
        FailingSource<RxUnit> unitSource = new();
        var errors = 0;
        for (var i = 0; i < Count; i++)
        {
            errors += PackageSubscriptionExtensions.SubscribeGetError(source) is null ? 0 : 1;
            errors += PackageSubscriptionExtensions.SubscribeGetError(unitSource) is null ? 0 : 1;
        }

        return errors;
    }

    /// <summary>Benchmarks SubscribeAndComplete on a single-value unit source.</summary>
    /// <returns>The number of completed calls.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("SubscribeAndComplete")]
    public int PrimitivesSubscribeAndComplete()
    {
        var source = PrimitivesObservables.Return(RxVoid.Default);
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            PrimitivesSubscriptionExtensions.SubscribeAndComplete(source);
            completed++;
        }

        return completed;
    }

    /// <summary>Benchmarks SubscribeAndComplete using ReactiveUI.Extensions.</summary>
    /// <returns>The number of completed calls.</returns>
    [Benchmark]
    [BenchmarkCategory("SubscribeAndComplete")]
    public int PackageSubscribeAndComplete()
    {
        var source = PackageObservables.Return(RxUnit.Default);
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            PackageSubscriptionExtensions.SubscribeAndComplete(source);
            completed++;
        }

        return completed;
    }

    /// <summary>Benchmarks WaitForValue inline and through the immediate sequencer.</summary>
    /// <returns>The sum of the returned values.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WaitForValue")]
    public int PrimitivesWaitForValue()
    {
        var source = PrimitivesExtensions.FromArray(Values);
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += PrimitivesSubscriptionExtensions.WaitForValue(source, Timeout);
            total += PrimitivesSubscriptionExtensions.WaitForValue(source, Sequencer.Immediate, Timeout);
        }

        return total;
    }

    /// <summary>Benchmarks WaitForValue inline and through the immediate scheduler using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the returned values.</returns>
    [Benchmark]
    [BenchmarkCategory("WaitForValue")]
    public int PackageWaitForValue()
    {
        var source = PackageExtensions.FromArray(Values);
        var total = 0;
        for (var i = 0; i < Count; i++)
        {
            total += PackageSubscriptionExtensions.WaitForValue(source, Timeout);
            total += PackageSubscriptionExtensions.WaitForValue(source, ImmediateScheduler.Instance, Timeout);
        }

        return total;
    }

    /// <summary>Benchmarks WaitForCompletion inline and through the immediate sequencer.</summary>
    /// <returns>The number of completed waits.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WaitForCompletion")]
    public int PrimitivesWaitForCompletion()
    {
        var source = PrimitivesObservables.Return(RxVoid.Default);
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            PrimitivesSubscriptionExtensions.WaitForCompletion(source, Timeout);
            PrimitivesSubscriptionExtensions.WaitForCompletion(source, Sequencer.Immediate, Timeout);
            completed += WaitsPerIteration;
        }

        return completed;
    }

    /// <summary>Benchmarks WaitForCompletion inline and through the immediate scheduler using ReactiveUI.Extensions.</summary>
    /// <returns>The number of completed waits.</returns>
    [Benchmark]
    [BenchmarkCategory("WaitForCompletion")]
    public int PackageWaitForCompletion()
    {
        var source = PackageObservables.Return(RxUnit.Default);
        var completed = 0;
        for (var i = 0; i < Count; i++)
        {
            PackageSubscriptionExtensions.WaitForCompletion(source, Timeout);
            PackageSubscriptionExtensions.WaitForCompletion(source, ImmediateScheduler.Instance, Timeout);
            completed += WaitsPerIteration;
        }

        return completed;
    }

    /// <summary>Benchmarks WaitForError inline and through the immediate sequencer on a failing source.</summary>
    /// <returns>The number of captured errors.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WaitForError")]
    public int PrimitivesWaitForError()
    {
        FailingSource<int> source = new();
        var errors = 0;
        for (var i = 0; i < Count; i++)
        {
            errors += PrimitivesSubscriptionExtensions.WaitForError(source, Timeout) is null ? 0 : 1;
            errors += PrimitivesSubscriptionExtensions.WaitForError(source, Sequencer.Immediate, Timeout) is null ? 0 : 1;
        }

        return errors;
    }

    /// <summary>Benchmarks WaitForError inline and through the immediate scheduler using ReactiveUI.Extensions.</summary>
    /// <returns>The number of captured errors.</returns>
    [Benchmark]
    [BenchmarkCategory("WaitForError")]
    public int PackageWaitForError()
    {
        FailingSource<int> source = new();
        var errors = 0;
        for (var i = 0; i < Count; i++)
        {
            errors += PackageSubscriptionExtensions.WaitForError(source, Timeout) is null ? 0 : 1;
            errors += PackageSubscriptionExtensions.WaitForError(source, ImmediateScheduler.Instance, Timeout) is null
                ? 0
                : 1;
        }

        return errors;
    }

    /// <summary>Cold source that fails every subscription during subscribe.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class FailingSource<T> : IObservable<T>
    {
        /// <summary>The failure emitted to every subscriber.</summary>
        private static readonly InvalidOperationException Failure = new("Source failed.");

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnError(Failure);
            return FinishedSubscription.Instance;
        }
    }

    /// <summary>Subscription handle for a source that finishes during subscribe.</summary>
    private sealed class FinishedSubscription : IDisposable
    {
        /// <summary>The shared instance.</summary>
        internal static readonly FinishedSubscription Instance = new();

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
