// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures subscribing to single-value signals backed by shared cached instances.</summary>
[MemoryDiagnoser]
public class CachedReturnSignalBenchmarks
{
    /// <summary>The number of subscriptions per case.</summary>
    private const int Count = 1000;

    /// <summary>The span of integers requested, which runs past the cached range.</summary>
    private const int ValueSpan = 12;

    /// <summary>The lowest integer requested.</summary>
    private const int MinValue = -1;

    /// <summary>Benchmarks subscribing to the shared Boolean return signals.</summary>
    /// <returns>The number of true values plus completions observed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesEmitBoolean()
    {
        BoolSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = Signal.Emit((i & 1) == 0).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks the inline delegate subscription on the shared Boolean return signals.</summary>
    /// <returns>The number of true values plus completions observed.</returns>
    [Benchmark]
    public int PrimitivesEmitBooleanInline()
    {
        var trueCount = 0;
        var completed = 0;
        Action<bool> onNext = value => trueCount += value ? 1 : 0;
        Action<Exception> onError = static _ => { };
        Action onCompleted = () => completed++;
        for (var i = 0; i < Count; i++)
        {
            var signal = (i & 1) == 0 ? (IInlineSignal<bool>)ImmutableReturnTrueSignal.Instance : ImmutableReturnFalseSignal.Instance;
            using var subscription = signal.Subscribe(onNext, onError, onCompleted);
        }

        return trueCount + completed
            + (ImmutableReturnTrueSignal.Instance.IsRequiredSubscribeOnCurrentThread() ? 1 : 0)
            + (ImmutableReturnFalseSignal.Instance.IsRequiredSubscribeOnCurrentThread() ? 1 : 0);
    }

    /// <summary>Benchmarks subscribing to Boolean return sequences using System.Reactive.</summary>
    /// <returns>The number of true values plus completions observed.</returns>
    [Benchmark]
    public int SystemReactiveReturnBoolean()
    {
        BoolSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = System.Reactive.Linq.Observable.Return((i & 1) == 0).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks subscribing to Boolean return sequences using R3.</summary>
    /// <returns>The number of values plus completions observed.</returns>
    [Benchmark]
    public int R3ReturnBoolean()
    {
        CountingR3Witness<bool> observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = R3.Observable.Return((i & 1) == 0).Subscribe(observer);
        }

        return observer.Count + observer.CompletionCount;
    }

    /// <summary>Benchmarks subscribing to integer return signals, cached for small values.</summary>
    /// <returns>The sum of observed values plus completions.</returns>
    [Benchmark]
    public int PrimitivesCachedInt32Return()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = ImmutableReturnInt32Signal.GetInt32Signals((i % ValueSpan) + MinValue).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks the inline delegate subscription on integer return signals.</summary>
    /// <returns>The sum of observed values plus completions.</returns>
    [Benchmark]
    public int PrimitivesInt32ReturnInline()
    {
        var total = 0;
        var completed = 0;
        Action<int> onNext = value => total += value;
        Action<Exception> onError = static _ => { };
        Action onCompleted = () => completed++;
        for (var i = 0; i < Count; i++)
        {
            ImmutableReturnInt32Signal signal = new((i % ValueSpan) + MinValue);
            using var subscription = signal.Subscribe(onNext, onError, onCompleted);
            completed += signal.IsRequiredSubscribeOnCurrentThread() ? 1 : 0;
        }

        return total + completed;
    }

    /// <summary>Benchmarks subscribing to integer return sequences using System.Reactive.</summary>
    /// <returns>The sum of observed values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveReturnInt32()
    {
        IntSignalWitness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = System.Reactive.Linq.Observable.Return((i % ValueSpan) + MinValue).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Benchmarks subscribing to integer return sequences using R3.</summary>
    /// <returns>The sum of observed values plus completions.</returns>
    [Benchmark]
    public int R3ReturnInt32()
    {
        IntR3Witness observer = new();
        for (var i = 0; i < Count; i++)
        {
            using var subscription = R3.Observable.Return((i % ValueSpan) + MinValue).Subscribe(observer);
        }

        return observer.Total + observer.CompletionCount;
    }
}
