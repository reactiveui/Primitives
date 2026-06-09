// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks for subscription fan-in and disposal operations.</summary>
[MemoryDiagnoser]
public class SubjectSubscriptionBenchmarks
{
    /// <summary>The small subscriber count used by the subscribe/dispose benchmarks.</summary>
    private const int SubscriberCount8 = 8;

    /// <summary>The large subscriber count used by the subscribe/dispose benchmarks.</summary>
    private const int SubscriberCount64 = 64;

    /// <summary>Subscribes and disposes 8 observers from primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectSubscribeDispose8() => SubscribeDisposeCountSignal(SubscriberCount8);

    /// <summary>Subscribes and disposes 8 observers from System.Reactive Subject.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark]
    public int SystemReactiveSubjectSubscribeDispose8() => SubscribeDisposeCountSystemSubject(SubscriberCount8);

    /// <summary>Subscribes and disposes 8 observers from <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark]
    public int R3SubjectSubscribeDispose8() => SubscribeDisposeCountR3Subject(SubscriberCount8);

    /// <summary>Subscribes and disposes 64 observers from primitives <see cref="Signal{T}"/>.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark]
    public int PrimitivesSubjectSubscribeDispose64() => SubscribeDisposeCountSignal(SubscriberCount64);

    /// <summary>Subscribes and disposes 64 observers from System.Reactive Subject.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark]
    public int SystemReactiveSubjectSubscribeDispose64() => SubscribeDisposeCountSystemSubject(SubscriberCount64);

    /// <summary>Subscribes and disposes 64 observers from <see cref="R3.Subject{T}"/>.</summary>
    /// <returns>A lifecycle marker confirming the benchmark created subscriptions and ran the disposal path.</returns>
    [Benchmark]
    public int R3SubjectSubscribeDispose64() => SubscribeDisposeCountR3Subject(SubscriberCount64);

    /// <summary>Subscribes and disposes the requested number of observers against a primitives signal.</summary>
    /// <param name="subscribers">The number of observers to subscribe and dispose.</param>
    /// <returns>A lifecycle marker confirming the subscriptions were created and disposed.</returns>
    private static int SubscribeDisposeCountSignal(int subscribers)
    {
        var observer = new IntSignalWitness();
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

    /// <summary>Subscribes and disposes the requested number of observers against a System.Reactive subject.</summary>
    /// <param name="subscribers">The number of observers to subscribe and dispose.</param>
    /// <returns>A lifecycle marker confirming the subscriptions were created and disposed.</returns>
    private static int SubscribeDisposeCountSystemSubject(int subscribers)
    {
        var observer = new IntSignalWitness();
        using var subject = new RxSubject();
        var disposables = new IDisposable[subscribers];
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i] = subject.Subscribe(observer);
        }

        var before = disposables.Length;
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i].Dispose();
        }

        return before;
    }

    /// <summary>Subscribes and disposes the requested number of observers against an R3 subject.</summary>
    /// <param name="subscribers">The number of observers to subscribe and dispose.</param>
    /// <returns>A lifecycle marker confirming the subscriptions were created and disposed.</returns>
    private static int SubscribeDisposeCountR3Subject(int subscribers)
    {
        using var subject = new R3.Subject<int>();
        var disposables = new IDisposable[subscribers];
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i] = subject.Subscribe(new IntR3ActionWitness());
        }

        var before = disposables.Length;
        for (var i = 0; i < subscribers; i++)
        {
            disposables[i].Dispose();
        }

        return before;
    }
}
