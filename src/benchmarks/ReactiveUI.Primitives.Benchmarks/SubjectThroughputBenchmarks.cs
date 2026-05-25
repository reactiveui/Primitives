// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using R3;

using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for hot path subject-like emission throughput.
/// </summary>
[MemoryDiagnoser]
public class SubjectThroughputBenchmarks
{
    private const int EmitCount32 = 32;
    private const int EmitCount1024 = 1024;

    /// <summary>
    /// Emits 32 values into primitives <see cref="Signal{T}"/>.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectEmit32()
    {
        return EmitThroughSignal(EmitCount32);
    }

    /// <summary>
    /// Emits 32 values into System.Reactive Subject.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int SystemReactiveSubjectEmit32()
    {
        return EmitThroughSystemSubject(EmitCount32);
    }

    /// <summary>
    /// Emits 32 values into <see cref="R3.Subject{T}"/>.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int R3SubjectEmit32()
    {
        return EmitThroughR3Subject(EmitCount32);
    }

    /// <summary>
    /// Emits 1024 values into primitives <see cref="Signal{T}"/>.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int PrimitivesSubjectEmit1024()
    {
        return EmitThroughSignal(EmitCount1024);
    }

    /// <summary>
    /// Emits 1024 values into System.Reactive Subject.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int SystemReactiveSubjectEmit1024()
    {
        return EmitThroughSystemSubject(EmitCount1024);
    }

    /// <summary>
    /// Emits 1024 values into <see cref="R3.Subject{T}"/>.
    /// </summary>
    /// <returns>The sum of observed values.</returns>
    [Benchmark]
    public int R3SubjectEmit1024()
    {
        return EmitThroughR3Subject(EmitCount1024);
    }

    private static int EmitThroughSignal(int count)
    {
        var observer = new IntSignalObserver();
        using var subject = new Signal<int>();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    private static int EmitThroughSystemSubject(int count)
    {
        var observer = new IntSignalObserver();
        using var subject = new RxSubject();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    private static int EmitThroughR3Subject(int count)
    {
        var observer = new IntR3Observer();
        using var subject = new R3.Subject<int>();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }
}
