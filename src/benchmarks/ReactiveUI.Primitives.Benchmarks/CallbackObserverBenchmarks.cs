// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using RxObserver = System.Reactive.Observer;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures delegate-backed observers receiving a value stream and its terminal notification.</summary>
[MemoryDiagnoser]
public class CallbackObserverBenchmarks
{
    /// <summary>The number of values pushed to each observer.</summary>
    private const int Count = 1000;

    /// <summary>The value appended after the source completes.</summary>
    private const int AppendedValue = -1;

    /// <summary>Benchmarks an observer forwarding to next, error and result-completion delegates.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCallbackWitness()
    {
        var total = 0;
        var completed = 0;
        using Signal<int> source = new();
        using var subscription = source.Subscribe(
            new CallbackWitness<int>(value => total += value, _ => completed--, _ => completed++));
        Push(source);
        return total + completed;
    }

    /// <summary>Benchmarks an observer forwarding to static delegates that receive explicit state.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesStatefulWitness()
    {
        IntSignalWitness state = new();
        using Signal<int> source = new();
        using var subscription = source.Subscribe(
            new StatefulWitness<int, IntSignalWitness>(
                state,
                static (value, sink) => sink.OnNext(value),
                static (error, sink) => sink.OnError(error),
                static (_, sink) => sink.OnCompleted()));
        Push(source);
        return state.Total + state.CompletionCount;
    }

    /// <summary>Benchmarks an observer forwarding to next and completion delegates with optional error handling.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesDelegateWitness()
    {
        var total = 0;
        var completed = 0;
        using Signal<int> source = new();
        using var subscription = source.Subscribe(
            new DelegateWitness<int>(value => total += value, onCompleted: () => completed++));
        Push(source);
        return total + completed;
    }

    /// <summary>Benchmarks a delegate-backed observer using System.Reactive.</summary>
    /// <returns>The sum of observed values plus the completion count.</returns>
    [Benchmark]
    public int SystemReactiveObserverCreate()
    {
        var total = 0;
        var completed = 0;
        using RxSubject source = new();
        using var subscription = source.Subscribe(
            RxObserver.Create<int>(value => total += value, _ => completed--, () => completed++));
        Push(source);
        return total + completed;
    }

    /// <summary>Benchmarks a delegate sink that appends a value when the source completes.</summary>
    /// <returns>The sum of observed values, including the appended one, plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesAppendDelegateWitness()
    {
        var total = 0;
        var completed = 0;
        using Signal<int> source = new();
        using AppendDelegateWitness<int> sink = new(value => total += value, _ => completed--, () => completed++, AppendedValue);
        sink.SetSubscription(source.Subscribe(sink));
        Push(source);
        return total + completed;
    }

    /// <summary>Benchmarks appending a value on completion using System.Reactive.</summary>
    /// <returns>The sum of observed values, including the appended one, plus the completion count.</returns>
    [Benchmark]
    public int SystemReactiveAppend()
    {
        var total = 0;
        var completed = 0;
        using RxSubject source = new();
        using var subscription = System.Reactive.Linq.Observable.Append(source, AppendedValue).Subscribe(
            RxObserver.Create<int>(value => total += value, _ => completed--, () => completed++));
        Push(source);
        return total + completed;
    }

    /// <summary>Pushes the value range and completes a Primitives source.</summary>
    /// <param name="source">The source to drive.</param>
    private static void Push(Signal<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }

    /// <summary>Pushes the value range and completes a System.Reactive source.</summary>
    /// <param name="source">The source to drive.</param>
    private static void Push(RxSubject source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }
}
