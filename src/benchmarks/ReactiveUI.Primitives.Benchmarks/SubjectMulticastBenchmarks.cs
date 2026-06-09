// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks fan-out emission throughput: emitting a stream of values into a subject that already has several
/// observers attached, so the cost is dominated by the per-observer dispatch loop rather than subscription setup.
/// </summary>
[MemoryDiagnoser]
public class SubjectMulticastBenchmarks
{
    /// <summary>The number of values emitted per benchmark, large enough that dispatch dominates one-time subscription cost.</summary>
    private const int EmitCount = 1024;

    /// <summary>The small fan-out observer count.</summary>
    private const int ObserverCount4 = 4;

    /// <summary>The large fan-out observer count.</summary>
    private const int ObserverCount8 = 8;

    /// <summary>Emits to four observers through primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSignalMulticast4() => EmitThroughSignal(ObserverCount4);

    /// <summary>Emits to four observers through System.Reactive Subject.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark]
    public int SystemReactiveSubjectMulticast4() => EmitThroughSystemSubject(ObserverCount4);

    /// <summary>Emits to four observers through <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark]
    public int R3SubjectMulticast4() => EmitThroughR3Subject(ObserverCount4);

    /// <summary>Emits to eight observers through primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark]
    public int PrimitivesSignalMulticast8() => EmitThroughSignal(ObserverCount8);

    /// <summary>Emits to eight observers through System.Reactive Subject.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark]
    public int SystemReactiveSubjectMulticast8() => EmitThroughSystemSubject(ObserverCount8);

    /// <summary>Emits to eight observers through <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>The sum observed by the first observer.</returns>
    [Benchmark]
    public int R3SubjectMulticast8() => EmitThroughR3Subject(ObserverCount8);

    /// <summary>Emits <see cref="EmitCount"/> values through a primitives signal fanned out to the requested observers.</summary>
    /// <param name="observerCount">The number of observers to attach.</param>
    /// <returns>The sum observed by the first observer.</returns>
    private static int EmitThroughSignal(int observerCount)
    {
        using var subject = new Signal<int>();
        var observers = new IntSignalWitness[observerCount];
        var subscriptions = new IDisposable[observerCount];
        for (var i = 0; i < observerCount; i++)
        {
            observers[i] = new();
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        for (var i = 0; i < observerCount; i++)
        {
            subscriptions[i].Dispose();
        }

        return observers[0].Total;
    }

    /// <summary>Emits <see cref="EmitCount"/> values through a System.Reactive subject fanned out to the requested observers.</summary>
    /// <param name="observerCount">The number of observers to attach.</param>
    /// <returns>The sum observed by the first observer.</returns>
    private static int EmitThroughSystemSubject(int observerCount)
    {
        using var subject = new RxSubject();
        var observers = new IntSignalWitness[observerCount];
        var subscriptions = new IDisposable[observerCount];
        for (var i = 0; i < observerCount; i++)
        {
            observers[i] = new();
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        for (var i = 0; i < observerCount; i++)
        {
            subscriptions[i].Dispose();
        }

        return observers[0].Total;
    }

    /// <summary>Emits <see cref="EmitCount"/> values through an R3 subject fanned out to the requested observers.</summary>
    /// <param name="observerCount">The number of observers to attach.</param>
    /// <returns>The sum observed by the first observer.</returns>
    private static int EmitThroughR3Subject(int observerCount)
    {
        using var subject = new R3.Subject<int>();
        var observers = new IntR3Witness[observerCount];
        var subscriptions = new IDisposable[observerCount];
        for (var i = 0; i < observerCount; i++)
        {
            observers[i] = new();
            subscriptions[i] = subject.Subscribe(observers[i]);
        }

        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        for (var i = 0; i < observerCount; i++)
        {
            subscriptions[i].Dispose();
        }

        return observers[0].Total;
    }
}
