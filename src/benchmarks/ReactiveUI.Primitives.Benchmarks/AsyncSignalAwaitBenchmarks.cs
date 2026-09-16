// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxAsyncSubject = System.Reactive.Subjects.AsyncSubject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures last-value signals that replay their final value on completion and can be awaited.</summary>
[MemoryDiagnoser]
public class AsyncSignalAwaitBenchmarks
{
    /// <summary>The number of values recorded before completion.</summary>
    private const int Count = 1000;

    /// <summary>The number of observers subscribed before completion.</summary>
    private const int Subscribers = 4;

    /// <summary>Benchmarks fanning out the completed value to several observers, one of which unsubscribes early, plus a late subscriber.</summary>
    /// <returns>The sum of values delivered to every observer.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesAsyncSignalFanOut()
    {
        var observers = CreateObservers();
        var handles = new IDisposable[Subscribers];
        var total = 0;
        using (AsyncSignal<int> signal = new())
        {
            for (var i = 0; i < Subscribers; i++)
            {
                handles[i] = signal.Subscribe(observers[i]);
            }

            handles[0].Dispose();
            handles[0].Dispose();
            for (var i = 0; i < Count; i++)
            {
                signal.OnNext(i);
            }

            total += signal.HasObservers ? 1 : 0;
            signal.OnCompleted();
            IntSignalWitness late = new();
            using var lateSubscription = signal.Subscribe(late);
            total += late.Total + signal.Value + (signal.IsCompleted ? 1 : 0);
        }

        foreach (var observer in observers)
        {
            total += observer.Total;
        }

        return total;
    }

    /// <summary>Benchmarks fanning out the completed value to several observers using System.Reactive.</summary>
    /// <returns>The sum of values delivered to every observer.</returns>
    [Benchmark]
    public int SystemReactiveAsyncSubjectFanOut()
    {
        var observers = CreateObservers();
        var handles = new IDisposable[Subscribers];
        var total = 0;
        using (RxAsyncSubject subject = new())
        {
            for (var i = 0; i < Subscribers; i++)
            {
                handles[i] = subject.Subscribe(observers[i]);
            }

            handles[0].Dispose();
            handles[0].Dispose();
            for (var i = 0; i < Count; i++)
            {
                subject.OnNext(i);
            }

            total += subject.HasObservers ? 1 : 0;
            subject.OnCompleted();
            IntSignalWitness late = new();
            using var lateSubscription = subject.Subscribe(late);
            total += late.Total + subject.GetResult() + (subject.IsDisposed ? 0 : 1);
        }

        foreach (var observer in observers)
        {
            total += observer.Total;
        }

        return total;
    }

    /// <summary>Benchmarks awaiting a pending signal that completes after the continuation is registered.</summary>
    /// <returns>The awaited value.</returns>
    [Benchmark]
    public async Task<int> PrimitivesAsyncSignalAwait()
    {
        AsyncSignal<int> signal = new();
        var pending = AwaitSignalAsync(signal);
        for (var i = 0; i < Count; i++)
        {
            signal.OnNext(i);
        }

        signal.OnCompleted();
        var value = await pending.ConfigureAwait(false);
        signal.Dispose();
        return value;
    }

    /// <summary>Benchmarks awaiting a pending subject that completes after the continuation is registered using System.Reactive.</summary>
    /// <returns>The awaited value.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveAsyncSubjectAwait()
    {
        RxAsyncSubject subject = new();
        var pending = AwaitSubjectAsync(subject);
        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        subject.OnCompleted();
        var value = await pending.ConfigureAwait(false);
        subject.Dispose();
        return value;
    }

    /// <summary>Awaits a Primitives last-value signal.</summary>
    /// <param name="signal">The signal to await.</param>
    /// <returns>The completed value.</returns>
    private static async Task<int> AwaitSignalAsync(AsyncSignal<int> signal) => await signal;

    /// <summary>Awaits a System.Reactive last-value subject.</summary>
    /// <param name="subject">The subject to await.</param>
    /// <returns>The completed value.</returns>
    private static async Task<int> AwaitSubjectAsync(RxAsyncSubject subject) => await subject;

    /// <summary>Creates the observers subscribed before completion.</summary>
    /// <returns>The observers.</returns>
    private static IntSignalWitness[] CreateObservers()
    {
        var observers = new IntSignalWitness[Subscribers];
        for (var i = 0; i < Subscribers; i++)
        {
            observers[i] = new();
        }

        return observers;
    }
}
