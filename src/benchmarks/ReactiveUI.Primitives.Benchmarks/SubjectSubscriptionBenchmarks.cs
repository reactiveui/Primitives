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
/// Benchmarks for subscription fan-in and disposal operations.
/// </summary>
[MemoryDiagnoser]
public class SubjectSubscriptionBenchmarks
{
    private const int SubscriberCount8 = 8;
    private const int SubscriberCount64 = 64;

    /// <summary>
    /// Subscribes and disposes 8 observers from primitives <see cref="Signal{T}"/>.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectSubscribeDispose8()
    {
        return SubscribeDisposeCountSignal(SubscriberCount8);
    }

    /// <summary>
    /// Subscribes and disposes 8 observers from System.Reactive Subject.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark]
    public int SystemReactiveSubjectSubscribeDispose8()
    {
        return SubscribeDisposeCountSystemSubject(SubscriberCount8);
    }

    /// <summary>
    /// Subscribes and disposes 8 observers from <see cref="R3.Subject{T}"/>.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark]
    public int R3SubjectSubscribeDispose8()
    {
        return SubscribeDisposeCountR3Subject(SubscriberCount8);
    }

    /// <summary>
    /// Subscribes and disposes 64 observers from primitives <see cref="Signal{T}"/>.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark]
    public int PrimitivesSubjectSubscribeDispose64()
    {
        return SubscribeDisposeCountSignal(SubscriberCount64);
    }

    /// <summary>
    /// Subscribes and disposes 64 observers from System.Reactive Subject.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark]
    public int SystemReactiveSubjectSubscribeDispose64()
    {
        return SubscribeDisposeCountSystemSubject(SubscriberCount64);
    }

    /// <summary>
    /// Subscribes and disposes 64 observers from <see cref="R3.Subject{T}"/>.
    /// </summary>
    /// <returns>The final observer count after disposal.</returns>
    [Benchmark]
    public int R3SubjectSubscribeDispose64()
    {
        return SubscribeDisposeCountR3Subject(SubscriberCount64);
    }

    private static int SubscribeDisposeCountSignal(int subscribers)
    {
        var observer = new IntSignalObserver();
        using var subject = new Signal<int>();
        var disposables = new IDisposable[subscribers];
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i] = subject.Subscribe(observer);
        }

        var before = subject.HasObservers ? 1 : 0;
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i].Dispose();
        }

        return before + (subject.HasObservers ? 1 : 0);
    }

    private static int SubscribeDisposeCountSystemSubject(int subscribers)
    {
        var observer = new IntSignalObserver();
        using var subject = new RxSubject();
        var disposables = new System.IDisposable[subscribers];
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i] = subject.Subscribe(observer);
        }

        for (var i = 0; i < subscribers; i++)
        {
            disposables[i].Dispose();
        }

        return disposables.Length;
    }

    private static int SubscribeDisposeCountR3Subject(int subscribers)
    {
        using var subject = new R3.Subject<int>();
        var disposables = new IDisposable[subscribers];
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i] = subject.Subscribe(new IntR3ActionObserver());
        }

        for (var i = 0; i < subscribers; i++)
        {
            disposables[i].Dispose();
        }

        return disposables.Length;
    }
}
