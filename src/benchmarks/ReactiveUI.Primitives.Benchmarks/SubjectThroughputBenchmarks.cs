// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for hot path subject-like emission throughput.</summary>
[MemoryDiagnoser]
public class SubjectThroughputBenchmarks
{
    /// <summary>The small emission count used by the throughput benchmarks.</summary>
    private const int EmitCount32 = 32;

    /// <summary>The large emission count used by the throughput benchmarks.</summary>
    private const int EmitCount1024 = 1024;

    /// <summary>Emits 32 values into primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectEmit32() => EmitThroughSignal(EmitCount32);

    /// <summary>Emits 32 values into System.Reactive Subject.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int SystemReactiveSubjectEmit32() => EmitThroughSystemSubject(EmitCount32);

    /// <summary>Emits 32 values into <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int R3SubjectEmit32() => EmitThroughR3Subject(EmitCount32);

    /// <summary>Emits 1024 values into primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int PrimitivesSubjectEmit1024() => EmitThroughSignal(EmitCount1024);

    /// <summary>Emits 1024 values into System.Reactive Subject.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int SystemReactiveSubjectEmit1024() => EmitThroughSystemSubject(EmitCount1024);

    /// <summary>Emits 1024 values into <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int R3SubjectEmit1024() => EmitThroughR3Subject(EmitCount1024);

    /// <summary>Emits the requested number of values through a primitives signal and sums the observed values.</summary>
    /// <param name="count">The number of values to emit.</param>
    /// <returns>The sum of observed values.</returns>
    private static int EmitThroughSignal(int count)
    {
        IntSignalWitness observer = new();
        using Signal<int> subject = new();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Emits the requested number of values through a System.Reactive subject and sums the observed values.</summary>
    /// <param name="count">The number of values to emit.</param>
    /// <returns>The sum of observed values.</returns>
    private static int EmitThroughSystemSubject(int count)
    {
        IntSignalWitness observer = new();
        using RxSubject subject = new();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Emits the requested number of values through an R3 subject and sums the observed values.</summary>
    /// <param name="count">The number of values to emit.</param>
    /// <returns>The sum of observed values.</returns>
    private static int EmitThroughR3Subject(int count)
    {
        IntR3Witness observer = new();
        using Subject<int> subject = new();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }
}
