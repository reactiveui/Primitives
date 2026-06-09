// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxBehaviorSubject = System.Reactive.Subjects.BehaviorSubject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for stateful latest-value and replay-like signal subscriptions.</summary>
[MemoryDiagnoser]
public class StatefulSignalBenchmarks
{
    /// <summary>The small notification count used by the stateful signal benchmarks.</summary>
    private const int Count32 = 32;

    /// <summary>The large notification count used by the stateful signal benchmarks.</summary>
    private const int Count1024 = 1024;

    /// <summary>Baseline state signal updates with 32 notifications.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesStateSignal32() => EmitAndReadStateSignal(Count32);

    /// <summary>Behavior subject updates with 32 notifications using System.Reactive.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int SystemReactiveBehaviorSubject32() => EmitAndReadSystemBehaviorSubject(Count32);

    /// <summary>Behavior subject updates with 32 notifications using R3.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int R3BehaviorSubject32() => EmitAndReadR3BehaviorSubject(Count32);

    /// <summary>Baseline state signal updates with 1024 notifications.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int PrimitivesStateSignal1024() => EmitAndReadStateSignal(Count1024);

    /// <summary>Behavior subject updates with 1024 notifications using System.Reactive.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int SystemReactiveBehaviorSubject1024() => EmitAndReadSystemBehaviorSubject(Count1024);

    /// <summary>Behavior subject updates with 1024 notifications using R3.</summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int R3BehaviorSubject1024() => EmitAndReadR3BehaviorSubject(Count1024);

    /// <summary>Emits the requested number of notifications through a primitives state signal and reads the result.</summary>
    /// <param name="count">The number of notifications to emit.</param>
    /// <returns>The final observed sum plus the latest value.</returns>
    private static int EmitAndReadStateSignal(int count)
    {
        var observer = new IntSignalWitness();
        using var subject = new StateSignal<int>(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }

    /// <summary>Emits the requested number of notifications through a System.Reactive behavior subject and reads the result.</summary>
    /// <param name="count">The number of notifications to emit.</param>
    /// <returns>The final observed sum plus the latest value.</returns>
    private static int EmitAndReadSystemBehaviorSubject(int count)
    {
        var observer = new IntSignalWitness();
        using var subject = new RxBehaviorSubject(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }

    /// <summary>Emits the requested number of notifications through an R3 behavior subject and reads the result.</summary>
    /// <param name="count">The number of notifications to emit.</param>
    /// <returns>The final observed sum plus the latest value.</returns>
    private static int EmitAndReadR3BehaviorSubject(int count)
    {
        var observer = new IntR3Witness();
        using var subject = new R3.BehaviorSubject<int>(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }
}
