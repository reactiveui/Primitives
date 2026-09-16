// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;
using RxTaskObservable = System.Reactive.Threading.Tasks.TaskObservableExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures queries that reduce a sequence to one answer: emptiness, any-match and count.</summary>
[MemoryDiagnoser]
public class SequenceQueryTerminalBenchmarks
{
    /// <summary>The number of values each source emits.</summary>
    private const int Count = 256;

    /// <summary>The number of subscriptions the emptiness case runs.</summary>
    private const int SubscriptionCount = 16;

    /// <summary>The source values.</summary>
    private static readonly int[] Values = CreateValues();

    /// <summary>Benchmarks awaiting the number of values a hot source emits.</summary>
    /// <returns>The value count.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesCountAsyncSubject()
    {
        using Signal<int> source = new();
        var count = source.CountAsync();
        EmitAll(source);
        return await count.ConfigureAwait(false);
    }

    /// <summary>Benchmarks awaiting the number of values a hot source emits using System.Reactive.</summary>
    /// <returns>The value count.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveCountSubject()
    {
        using RxSubject source = new();
        var count = RxTaskObservable.ToTask(RxObservable.Count(source));
        EmitAll(source);
        return await count.ConfigureAwait(false);
    }

    /// <summary>Benchmarks awaiting whether any value of a hot source matches the final value.</summary>
    /// <returns>One when a value matched; otherwise zero.</returns>
    [Benchmark]
    public async Task<int> PrimitivesAnyAsyncSubject()
    {
        using Signal<int> source = new();
        var any = source.AnyAsync(static value => value == Count - 1);
        EmitAll(source);
        return await any.ConfigureAwait(false) ? 1 : 0;
    }

    /// <summary>Benchmarks awaiting whether any value matches using System.Reactive.</summary>
    /// <returns>One when a value matched; otherwise zero.</returns>
    [Benchmark]
    public async Task<int> SystemReactiveAnySubject()
    {
        using RxSubject source = new();
        var any = RxTaskObservable.ToTask(RxObservable.Any(source, static value => value == Count - 1));
        EmitAll(source);
        return await any.ConfigureAwait(false) ? 1 : 0;
    }

    /// <summary>Benchmarks asking whether a finite source is empty.</summary>
    /// <returns>The number of answers observed.</returns>
    [Benchmark]
    public int PrimitivesIsEmpty()
    {
        CountingSignalWitness<bool> observer = new();
        for (var i = 0; i < SubscriptionCount; i++)
        {
            using var subscription = Signal.FromEnumerable(Values).IsEmpty().Subscribe(observer);
        }

        return observer.Count;
    }

    /// <summary>Benchmarks asking whether a finite source is empty using System.Reactive.</summary>
    /// <returns>The number of answers observed.</returns>
    [Benchmark]
    public int SystemReactiveIsEmpty()
    {
        CountingSignalWitness<bool> observer = new();
        for (var i = 0; i < SubscriptionCount; i++)
        {
            using var subscription = RxObservable.IsEmpty(RxObservable.ToObservable(Values)).Subscribe(observer);
        }

        return observer.Count;
    }

    /// <summary>Creates the source values.</summary>
    /// <returns>The values.</returns>
    private static int[] CreateValues()
    {
        var values = new int[Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = i;
        }

        return values;
    }

    /// <summary>Emits every value on the source and completes it.</summary>
    /// <param name="source">The source to drive.</param>
    private static void EmitAll(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        source.OnCompleted();
    }
}
