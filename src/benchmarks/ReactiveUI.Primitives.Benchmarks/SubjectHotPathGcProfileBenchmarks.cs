// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnosers;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// GC-verbose profile for the subject hot paths touched by the lock-free copy-on-write rewrite of
/// <see cref="Signal{T}"/>, the System.Threading.Lock migration of the value-carrying subjects, and
/// the readonly-Observer change. Covers steady-state emission (per-OnNext should be allocation-free)
/// and subscribe/unsubscribe churn (exercises the copy-on-write add/remove path).
/// </summary>
[ShortRunJob]
[MemoryDiagnoser]
[EventPipeProfiler(EventPipeProfile.GcVerbose)]
public class SubjectHotPathGcProfileBenchmarks
{
    /// <summary>The number of values emitted in each steady-state emission benchmark.</summary>
    private const int EmitCount = 1024;

    /// <summary>The number of subscribe/dispose cycles performed in each churn benchmark.</summary>
    private const int ChurnCount = 1024;

    /// <summary>The number of concurrent subscribers used in the fan-out churn benchmark.</summary>
    private const int FanOut = 8;

    /// <summary>Steady-state emission through <see cref="Signal{T}"/> (single subscriber fast path).</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SignalEmit()
    {
        IntSignalWitness observer = new();
        using Signal<int> subject = new();
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Steady-state emission through <see cref="BehaviorSignal{T}"/>.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int BehaviorEmit()
    {
        IntSignalWitness observer = new();
        using BehaviorSignal<int> subject = new(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Steady-state emission through <see cref="StateSignal{T}"/>.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int StateEmit()
    {
        IntSignalWitness observer = new();
        using StateSignal<int> subject = new(0);
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Steady-state emission through a bounded <see cref="HistorySignal{T}"/> replay buffer.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int ReplayEmit()
    {
        IntSignalWitness observer = new();
        using HistorySignal<int> subject = new(16);
        using var subscription = subject.Subscribe(observer);
        for (var i = 0; i < EmitCount; i++)
        {
            subject.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Single-subscriber subscribe/dispose churn on <see cref="Signal{T}"/> (copy-on-write slot).</summary>
    /// <returns>The churn count.</returns>
    [Benchmark]
    public int SignalSubscribeDisposeChurn()
    {
        IntSignalWitness observer = new();
        using Signal<int> subject = new();
        for (var i = 0; i < ChurnCount; i++)
        {
            var subscription = subject.Subscribe(observer);
            subscription.Dispose();
        }

        return ChurnCount;
    }

    /// <summary>Multi-subscriber subscribe/dispose churn (exercises the copy-on-write array path).</summary>
    /// <returns>The churn count.</returns>
    [Benchmark]
    public int SignalFanOutChurn()
    {
        IntSignalWitness observer = new();
        using Signal<int> subject = new();
        var handles = new IDisposable[FanOut];
        for (var round = 0; round < ChurnCount / FanOut; round++)
        {
            for (var i = 0; i < FanOut; i++)
            {
                handles[i] = subject.Subscribe(observer);
            }

            for (var i = 0; i < FanOut; i++)
            {
                handles[i].Dispose();
            }
        }

        return ChurnCount;
    }
}
