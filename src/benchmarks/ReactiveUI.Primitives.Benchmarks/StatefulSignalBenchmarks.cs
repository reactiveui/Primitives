// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using R3;

using RxBehaviorSubject = System.Reactive.Subjects.BehaviorSubject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks for stateful behaviour/replay-like signal subscriptions.
/// </summary>
[MemoryDiagnoser]
public class StatefulSignalBenchmarks
{
    private const int Count32 = 32;
    private const int Count1024 = 1024;

    /// <summary>
    /// Baseline behavior-like stream updates with 32 notifications.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesBehaviourSignal32()
    {
        return EmitAndReadBehaviourSignal(Count32);
    }

    /// <summary>
    /// Behavior subject updates with 32 notifications using System.Reactive.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int SystemReactiveBehaviorSubject32()
    {
        return EmitAndReadSystemBehaviorSubject(Count32);
    }

    /// <summary>
    /// Behavior subject updates with 32 notifications using R3.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int R3BehaviorSubject32()
    {
        return EmitAndReadR3BehaviorSubject(Count32);
    }

    /// <summary>
    /// Baseline behavior-like stream updates with 1024 notifications.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int PrimitivesBehaviourSignal1024()
    {
        return EmitAndReadBehaviourSignal(Count1024);
    }

    /// <summary>
    /// Behavior subject updates with 1024 notifications using System.Reactive.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int SystemReactiveBehaviorSubject1024()
    {
        return EmitAndReadSystemBehaviorSubject(Count1024);
    }

    /// <summary>
    /// Behavior subject updates with 1024 notifications using R3.
    /// </summary>
    /// <returns>The final sum plus latest value.</returns>
    [Benchmark]
    public int R3BehaviorSubject1024()
    {
        return EmitAndReadR3BehaviorSubject(Count1024);
    }

    private static int EmitAndReadBehaviourSignal(int count)
    {
        var observer = new IntSignalObserver();
        using var subject = new BehaviorSignal<int>(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }

    private static int EmitAndReadSystemBehaviorSubject(int count)
    {
        var observer = new IntSignalObserver();
        using var subject = new RxBehaviorSubject(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }

    private static int EmitAndReadR3BehaviorSubject(int count)
    {
        var observer = new IntR3Observer();
        using var subject = new R3.BehaviorSubject<int>(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 1; i <= count; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total + subject.Value;
    }
}

