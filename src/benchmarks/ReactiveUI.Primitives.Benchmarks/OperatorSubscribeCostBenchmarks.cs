// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Measures subscribing to one operator over a live source, giving it a few values and disposing the subscription: the cost of
/// building one link of a chain, which a view model pays for every property it follows.
/// </summary>
[MemoryDiagnoser]
public class OperatorSubscribeCostBenchmarks
{
    /// <summary>The values given to each pipeline.</summary>
    private const int Values = 4;

    /// <summary>The values <c>Take</c> lets through.</summary>
    private const int TakeCount = 2;

    /// <summary>The value <c>StartWith</c> gives first.</summary>
    private const int StartValue = 9;

    /// <summary>Subscribes through <c>Skip</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSkip()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Skip(1).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Skip</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveSkip()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Skip(source, 1).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Where</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesWhere()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Where(IsPositive).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Where</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveWhere()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Where(source, IsPositive).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Select</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesSelect()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Select(AddOne).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Select</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveSelect()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Select(source, AddOne).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Take</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesTake()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Take(TakeCount).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Take</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveTake()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Take(source, TakeCount).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>StartWith</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesStartWith()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.StartWith(StartValue).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>StartWith</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveStartWith()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.StartWith(source, StartValue).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>DistinctUntilChanged</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesDistinctUntilChanged()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.DistinctUntilChanged().Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>DistinctUntilChanged</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveDistinctUntilChanged()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.DistinctUntilChanged(source).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Do</c>.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesDo()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = source.Do(Ignore).Subscribe(observer);
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Do</c> using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveDo()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Do(source, Ignore).Subscribe(observer);
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Publish</c> with a connection and one subscriber.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesPublish()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        var published = source.Publish();
        using var subscription = published.Subscribe(observer);
        using var connection = published.Connect();
        Feed(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>Publish</c> with a connection and one subscriber using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactivePublish()
    {
        IntSignalWitness observer = new();
        using RxSubject source = new();
        var published = RxObservable.Publish(source);
        using var subscription = published.Subscribe(observer);
        using var connection = published.Connect();
        FeedRx(source);
        return observer.Total;
    }

    /// <summary>Subscribes through <c>CombineLatest</c> of three sources.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int PrimitivesCombineLatestThree()
    {
        IntSignalWitness observer = new();
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        using var subscription = first.CombineLatest(second, third, Add).Subscribe(observer);
        for (var i = 0; i < Values; i++)
        {
            first.OnNext(i);
            second.OnNext(i);
            third.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Subscribes through <c>CombineLatest</c> of three sources using System.Reactive.</summary>
    /// <returns>The sum of the values.</returns>
    [Benchmark]
    public int SystemReactiveCombineLatestThree()
    {
        IntSignalWitness observer = new();
        using RxSubject first = new();
        using RxSubject second = new();
        using RxSubject third = new();
        using var subscription = RxObservable.CombineLatest(first, second, third, Add).Subscribe(observer);
        for (var i = 0; i < Values; i++)
        {
            first.OnNext(i);
            second.OnNext(i);
            third.OnNext(i);
        }

        return observer.Total;
    }

    /// <summary>Tells whether a value is above zero.</summary>
    /// <param name="value">The value.</param>
    /// <returns><see langword="true"/> when it is.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsPositive(int value) => value > 0;

    /// <summary>Adds one to a value.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The value plus one.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AddOne(int value) => value + 1;

    /// <summary>Adds three values.</summary>
    /// <param name="first">The first value.</param>
    /// <param name="second">The second value.</param>
    /// <param name="third">The third value.</param>
    /// <returns>The sum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Add(int first, int second, int third) => first + second + third;

    /// <summary>Does nothing with a value.</summary>
    /// <param name="value">The value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Ignore(int value)
    {
        // The value is the point of the call, not what is done with it.
    }

    /// <summary>Gives a source four values.</summary>
    /// <param name="source">The source.</param>
    private static void Feed(Signal<int> source)
    {
        for (var i = 0; i < Values; i++)
        {
            source.OnNext(i);
        }
    }

    /// <summary>Gives a System.Reactive source four values.</summary>
    /// <param name="source">The source.</param>
    private static void FeedRx(RxSubject source)
    {
        for (var i = 0; i < Values; i++)
        {
            source.OnNext(i);
        }
    }
}
